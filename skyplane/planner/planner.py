from importlib.resources import path
from typing import Dict, List, Optional, Tuple, Tuple
import re

from skyplane import compute
from skyplane.api.config import TransferConfig
from skyplane.utils import logger

from skyplane.planner.topology import TopologyPlan
from skyplane.gateway.gateway_program import (
    GatewayProgram,
    GatewayMuxOr,
    GatewayMuxAnd,
    GatewayReadObjectStore,
    GatewayWriteObjectStore,
    GatewayReceive,
    GatewaySend,
)

from skyplane.api.transfer_job import TransferJob
from skyplane.config_paths import aws_quota_path, gcp_quota_path, azure_standardDv5_quota_path
import json

from skyplane.utils.fn import do_parallel


class Planner:
    # Only supporting "aws:m5.", "azure:StandardD_v5", and "gcp:n2-standard" instances for now
    _VCPUS = (96, 64, 48, 32, 16, 8, 4, 2)

    def plan(self) -> TopologyPlan:
        raise NotImplementedError

    @staticmethod
    def _vm_to_vcpus(cloud_provider: str, vm: str) -> int:
        if cloud_provider == "aws":
            n_vcpus = re.findall(r"\d*x", vm)[0]
            return 4 * int(n_vcpus.replace("x", "")) if "x" in n_vcpus else int(n_vcpus)
        elif cloud_provider == "azure":
            n_vcpus = re.findall(r"\d+", vm)
            return int(n_vcpus[0])
        else:
            return int(vm.split("-")[-1])

    @staticmethod
    def _vcpus_to_vm(cloud_provider: str, vcpus: int) -> str:
        if cloud_provider == "aws":
            return "m5.large" if vcpus == 2 else "m5.xlarge" if vcpus == 4 else f"m5.{vcpus // 4}xlarge"
        vm_family = "Standard_D{}_v5" if cloud_provider == "azure" else "n2-standard-{}"
        return vm_family.format(vcpus)

    @staticmethod
    def _split_vcpus(num_vcpus: int, quota_limit: int) -> Optional[Tuple]:
        """Splits the total/target number of vcpus used into smaller partitions according to the quota limit with the
        given vcpu options. These options are the same for all providers for now. Enforces that the vcpu_count is
        all the same for all partitions, meaning that for a num_vcpus=32, you cannot partition into 1 vm of 16 vcpus and
        2 vms of 8 vcpus but can partition into 4 vms of 8 vcpus. Returns the number of partitions and their vcpus usage
        """
        # If the desired vCPU count is within the quota limit, don't fall back
        if num_vcpus <= quota_limit:
            return (1, num_vcpus)

        # Otherwise, try to split the desired vCPU count into smaller portions that are within the quota limit and use the largest option
        else:
            for vcpu_count in Planner._VCPUS:
                if vcpu_count <= quota_limit and vcpu_count <= num_vcpus:
                    portions = num_vcpus // vcpu_count
                    remaining_vcpus = num_vcpus - (vcpu_count * portions)
                    # If the remaining vCPUs are 0, use the current option to launch all portions
                    if remaining_vcpus == 0:
                        return (portions, vcpu_count)  # [vcpu_count] * portions
        # Return None if no valid vCPU portions were found
        return None


class UnicastDirectPlanner(Planner):
    # DO NOT USE THIS - broken for single-region transfers
    def __init__(self, n_instances: int, n_connections: int):
        self.n_instances = n_instances
        self.n_connections = n_connections
        super().__init__()

    def plan(self, jobs: List[TransferJob]) -> TopologyPlan:
        # make sure only single destination
        for job in jobs:
            assert len(job.dst_ifaces) == 1, f"DirectPlanner only support single destination jobs, got {len(job.dst_ifaces)}"

        src_region_tag = jobs[0].src_iface.region_tag()
        dst_region_tag = jobs[0].dst_ifaces[0].region_tag()
        # jobs must have same sources and destinations
        for job in jobs[1:]:
            assert job.src_iface.region_tag() == src_region_tag, "All jobs must have same source region"
            assert job.dst_ifaces[0].region_tag() == dst_region_tag, "All jobs must have same destination region"

        plan = TopologyPlan(src_region_tag=src_region_tag, dest_region_tags=[dst_region_tag])
        # TODO: use VM limits to determine how many instances to create in each region
        # TODO: support on-sided transfers but not requiring VMs to be created in source/destination regions
        for i in range(self.n_instances):
            plan.add_gateway(src_region_tag)
            plan.add_gateway(dst_region_tag)

        # ids of gateways in dst region
        dst_gateways = plan.get_region_gateways(dst_region_tag)

        src_program = GatewayProgram()
        dst_program = GatewayProgram()

        for job in jobs:
            src_bucket = job.src_iface.bucket()
            dst_bucket = job.dst_ifaces[0].bucket()

            # give each job a different partition id, so we can read/write to different buckets
            partition_id = jobs.index(job)

            # source region gateway program
            obj_store_read = src_program.add_operator(
                GatewayReadObjectStore(src_bucket, src_region_tag, self.n_connections), partition_id=partition_id
            )
            mux_or = src_program.add_operator(GatewayMuxOr(), parent_handle=obj_store_read, partition_id=partition_id)
            for i in range(self.n_instances):
                src_program.add_operator(
                    GatewaySend(target_gateway_id=dst_gateways[i].gateway_id, region=src_region_tag, num_connections=self.n_connections),
                    parent_handle=mux_or,
                    partition_id=partition_id,
                )

            # dst region gateway program
            recv_op = dst_program.add_operator(GatewayReceive(), partition_id=partition_id)
            dst_program.add_operator(
                GatewayWriteObjectStore(dst_bucket, dst_region_tag, self.n_connections), parent_handle=recv_op, partition_id=partition_id
            )

            # update cost per GB
            plan.cost_per_gb += compute.CloudProvider.get_transfer_cost(src_region_tag, dst_region_tag)

        # set gateway programs
        plan.set_gateway_program(src_region_tag, src_program)
        plan.set_gateway_program(dst_region_tag, dst_program)

        return plan


class MulticastDirectPlanner(Planner):
    n_instances: int
    n_connections: int
    transfer_config: TransferConfig

    def __init__(self, n_instances: int, n_connections: int, transfer_config: TransferConfig):
        self.n_instances = n_instances
        self.n_connections = n_connections
        self.transfer_config = transfer_config

        # Loading the quota information, add ibm cloud when it is supported
        self.quota_limits = {}
        with aws_quota_path.open("r") as f:
            self.quota_limits["aws"] = json.load(f)
        with gcp_quota_path.open("r") as f:
            self.quota_limits["gcp"] = json.load(f)
        with open(azure_standardDv5_quota_path, "r") as f:
            self.quota_limits["azure"] = json.load(f)

        super().__init__()

    def plan(self, jobs: List[TransferJob]) -> TopologyPlan:
        src_region_tag = jobs[0].src_iface.region_tag()
        dst_region_tags = [iface.region_tag() for iface in jobs[0].dst_ifaces]
        # jobs must have same sources and destinations
        for job in jobs[1:]:
            assert job.src_iface.region_tag() == src_region_tag, "All jobs must have same source region"
            assert [iface.region_tag() for iface in job.dst_ifaces] == dst_region_tags, "Add jobs must have same destination set"

        plan = TopologyPlan(src_region_tag=src_region_tag, dest_region_tags=dst_region_tags)

        # Dynammically calculate n_instances based on quota limits - do_parallel returns tuples of (target_vcpus, quota_limit)
        targets = do_parallel(self._calculate_vm_portions, [(src_region_tag, dst_region_tag) for dst_region_tag in dst_region_tags])
        targets = [t[1] for t in targets]
        min_target_vcpus = min(t[0] for t in targets)  # type: ignore
        min_quota_limit = min(t[1] for t in targets if t[1] is not None)  # type: ignore
        n_portions, vcpus = (
            Planner._split_vcpus(num_vcpus=min_target_vcpus, quota_limit=min_quota_limit)
            if min_quota_limit is not None
            else (1, min_target_vcpus)
        )  # type: ignore

        # Get the source vm_type
        src_provider = src_region_tag.split(":")[0]
        src_vm_type = Planner._vcpus_to_vm(cloud_provider=src_provider, vcpus=vcpus)

        # TODO: support on-sided transfers but not requiring VMs to be created in source/destination regions
        for i in range(n_portions * self.n_instances):
            plan.add_gateway(src_region_tag, src_vm_type)
            for dst_region_tag in dst_region_tags:
                dst_provider = dst_region_tag.split(":")[0]
                plan.add_gateway(dst_region_tag, Planner._vcpus_to_vm(cloud_provider=dst_provider, vcpus=vcpus))

        # initialize gateway programs per region
        dst_program = {dst_region: GatewayProgram() for dst_region in dst_region_tags}
        src_program = GatewayProgram()

        # iterate through all jobs
        for job in jobs:
            src_bucket = job.src_iface.bucket()
            src_region_tag = job.src_iface.region_tag()
            src_provider = src_region_tag.split(":")[0]

            # give each job a different partition id, so we can read/write to different buckets
            partition_id = jobs.index(job)

            # source region gateway program
            obj_store_read = src_program.add_operator(
                GatewayReadObjectStore(src_bucket, src_region_tag, self.n_connections), partition_id=partition_id
            )
            # send to all destination
            mux_and = src_program.add_operator(GatewayMuxAnd(), parent_handle=obj_store_read, partition_id=partition_id)
            dst_prefixes = job.dst_prefixes
            for i in range(len(job.dst_ifaces)):
                dst_iface = job.dst_ifaces[i]
                dst_prefix = dst_prefixes[i]
                dst_region_tag = dst_iface.region_tag()
                dst_bucket = dst_iface.bucket()
                dst_gateways = plan.get_region_gateways(dst_region_tag)

                # special case where destination is same region as source
                if dst_region_tag == src_region_tag:
                    src_program.add_operator(
                        GatewayWriteObjectStore(dst_bucket, dst_region_tag, self.n_connections, key_prefix=dst_prefix),
                        parent_handle=mux_and,
                        partition_id=partition_id,
                    )
                    continue

                # can send to any gateway in region
                mux_or = src_program.add_operator(GatewayMuxOr(), parent_handle=mux_and, partition_id=partition_id)
                for i in range(n_portions * self.n_instances):
                    private_ip = False
                    if dst_gateways[i].provider == "gcp" and src_provider == "gcp":
                        # print("Using private IP for GCP to GCP transfer", src_region_tag, dst_region_tag)
                        private_ip = True
                    src_program.add_operator(
                        GatewaySend(
                            target_gateway_id=dst_gateways[i].gateway_id,
                            region=dst_region_tag,
                            num_connections=self.n_connections,
                            private_ip=private_ip,
                        ),
                        parent_handle=mux_or,
                        partition_id=partition_id,
                    )

                # each gateway also recieves data from source
                recv_op = dst_program[dst_region_tag].add_operator(GatewayReceive(), partition_id=partition_id)
                dst_program[dst_region_tag].add_operator(
                    GatewayWriteObjectStore(dst_bucket, dst_region_tag, self.n_connections, key_prefix=dst_prefix),
                    parent_handle=recv_op,
                    partition_id=partition_id,
                )

                # update cost per GB
                plan.cost_per_gb += compute.CloudProvider.get_transfer_cost(src_region_tag, dst_region_tag)

        # set gateway programs
        plan.set_gateway_program(src_region_tag, src_program)
        for dst_region_tag, program in dst_program.items():
            if dst_region_tag != src_region_tag:  # don't overwrite
                plan.set_gateway_program(dst_region_tag, program)
        return plan

    def _get_quota_limits_for(self, cloud_provider: str, region: str, spot: bool = False) -> Optional[int]:
        """Gets the quota info from the saved files. Returns None if quota_info isn't loaded during `skyplane init`
        or if the quota info doesn't include the region
        """
        quota_limits = self.quota_limits[cloud_provider]
        if not quota_limits:
            # User needs to reinitialize to save the quota information
            logger.warning(f"Please run `skyplane init --reinit-{cloud_provider}` to load the quota information")
            return None
        if cloud_provider == "gcp":
            region_family = "-".join(region.split("-")[:2])
            if region_family in quota_limits:
                return quota_limits[region_family]
        elif cloud_provider == "azure":
            if region in quota_limits:
                return quota_limits[region]
        elif cloud_provider == "aws":
            for quota in quota_limits:
                if quota["region_name"] == region:
                    return quota["spot_standard_vcpus"] if spot else quota["on_demand_standard_vcpus"]
        return None

    def _calculate_vm_portions(self, tags: Tuple[str, str]) -> Tuple[int, Optional[int]]:
        """Calculates how to split the source and destination vms according to the quota limits on number of vcpus.
        Constraints:
            - Take the minimum of the quota limits of source and destination
            - If the configured vm types for source and destination don't launch the same number of vcpus, take
            the min of them
        As a result, both the source and the destination use the same number of partitions with the same number of vcpus
        on each partition. Returns the number of partitions and vcpus.
        """
        src_tag, destination_tag = tags

        # Get source vcpu info
        src_provider, src_region = src_tag.split(":")
        src_vm_type = getattr(self.transfer_config, f"{src_provider}_instance_class")
        src_target_vcpus = Planner._vm_to_vcpus(src_provider, src_vm_type)

        # Get destination vcpu info
        dst_provider, dst_region = destination_tag.split(":")
        dst_vm_type = getattr(self.transfer_config, f"{dst_provider}_instance_class")
        dst_target_vcpus = Planner._vm_to_vcpus(dst_provider, dst_vm_type)

        # Get the quota info for both
        src_quota_limit = self._get_quota_limits_for(
            cloud_provider=src_provider, region=src_region, spot=getattr(self.transfer_config, f"{src_provider}_use_spot_instances")
        )
        dst_quota_limit = self._get_quota_limits_for(
            cloud_provider=dst_provider, region=dst_region, spot=getattr(self.transfer_config, f"{dst_provider}_use_spot_instances")
        )
        quota_limit = min(src_quota_limit, dst_quota_limit) if src_quota_limit is not None and dst_quota_limit is not None else None

        # If the target vcpus are not the same, also take the min of target vcpus
        target_vcpus = src_target_vcpus if src_target_vcpus == dst_target_vcpus else min(src_target_vcpus, dst_target_vcpus)

        return target_vcpus, quota_limit


class UnicastILPPlanner(Planner):
    def __init__(self, n_instances: int, n_connections: int, required_throughput_gbits: float):
        self.n_instances = n_instances
        self.n_connections = n_connections
        self.solver_required_throughput_gbits = required_throughput_gbits
        super().__init__()

    def plan(self, jobs: List[TransferJob]) -> TopologyPlan:
        raise NotImplementedError("ILP solver not implemented yet")


class MulticastILPPlanner(Planner):
    def __init__(self, n_instances: int, n_connections: int, required_throughput_gbits: float):
        self.n_instances = n_instances
        self.n_connections = n_connections
        self.solver_required_throughput_gbits = required_throughput_gbits
        super().__init__()

    def plan(self, jobs: List[TransferJob]) -> TopologyPlan:
        raise NotImplementedError("ILP solver not implemented yet")


class MulticastMDSTPlanner(Planner):
    def __init__(self, n_instances: int, n_connections: int):
        self.n_instances = n_instances
        self.n_connections = n_connections
        super().__init__()

    def plan(self, jobs: List[TransferJob]) -> TopologyPlan:
        raise NotImplementedError("MDST solver not implemented yet")


class MulticastSteinerTreePlanner(Planner):
    def __init__(self, n_instances: int, n_connections: int):
        self.n_instances = n_instances
        self.n_connections = n_connections
        super().__init__()

    def plan(self, jobs: List[TransferJob]) -> TopologyPlan:
        raise NotImplementedError("Steiner tree solver not implemented yet")
