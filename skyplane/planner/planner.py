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
            return 4 * int(n_vcpus.replace("x", "") or 1) if "x" in n_vcpus else int(n_vcpus)
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

    # @staticmethod
    # def _split_vcpus(num_vcpus: int, quota_limit: int) -> Optional[Tuple]:
    #     """Splits the total/target number of vcpus used into smaller partitions according to the quota limit with the
    #     given vcpu options. These options are the same for all providers for now. Enforces that the vcpu_count is
    #     all the same for all partitions, meaning that for a num_vcpus=32, you cannot partition into 1 vm of 16 vcpus and
    #     2 vms of 8 vcpus but can partition into 4 vms of 8 vcpus. Returns the number of partitions and their vcpus usage
    #     """
    #     # If the desired vCPU count is within the quota limit, don't fall back
    #     if num_vcpus <= quota_limit:
    #         return (1, num_vcpus)

    #     # Otherwise, try to split the desired vCPU count into smaller portions that are within the quota limit and use the largest option
    #     else:
    #         for vcpu_count in Planner._VCPUS:
    #             if vcpu_count <= quota_limit and vcpu_count <= num_vcpus:
    #                 portions = num_vcpus // vcpu_count
    #                 remaining_vcpus = num_vcpus - (vcpu_count * portions)
    #                 # If the remaining vCPUs are 0, use the current option to launch all portions
    #                 if remaining_vcpus == 0:
    #                     return (portions, vcpu_count)  # [vcpu_count] * portions
    #     # Return None if no valid vCPU portions were found
    #     return None


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
        with self.transfer_config.aws_vcpu_file.open("r") as f:
            self.quota_limits["aws"] = json.load(f)
        with self.transfer_config.gcp_vcpu_file.open("r") as f:
            self.quota_limits["gcp"] = json.load(f)
        with self.transfer_config.azure_vcpu_file.open("r") as f:
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

        # Dynammically calculate n_instances based on quota limits - do_parallel returns
        # tuples of (vcpus, n_instances)
        vm_info = do_parallel(self._calculate_vm_types, [src_region_tag] + dst_region_tags)  # type: ignore
        vm_types = {v[0]: Planner._vcpus_to_vm(cloud_provider=v[0].split(":")[0], vcpus=v[1][0]) for v in vm_info}  # type: ignore

        # Taking the minimum so that we can use the same number of instances for both source and destination
        n_instances = min(v[1][1] for v in vm_info)  # type: ignore

        # TODO: support on-sided transfers but not requiring VMs to be created in source/destination regions
        for i in range(n_instances):
            plan.add_gateway(src_region_tag, vm_types[src_region_tag])
            for dst_region_tag in dst_region_tags:
                plan.add_gateway(dst_region_tag, vm_types[dst_region_tag])

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
                for i in range(n_instances):
                    private_ip = False
                    if dst_gateways[i].provider == "gcp" and src_provider == "gcp":
                        # print("Using private IP for GCP to GCP transfer", src_region_tag, dst_region_tag)
                        private_ip = True
                    src_program.add_operator(
                        GatewaySend(
                            target_gateway_id=dst_gateways[i].gateway_id,
                            region=dst_region_tag,
                            num_connections=int(self.n_connections / len(dst_gateways)),
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
        or if the quota info doesn't include the region.

        :param cloud_provider: name of the cloud provider of the region
        :type cloud_provider: str
        :param region: name of the region for which to get the quota for
        :type region: int
        :param spot: whether to use spot specified by the user config (default: False)
        :type spot: bool
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

    def _calculate_vm_types(self, region_tag: str) -> Optional[Tuple[int, int]]:
        """Calculates the largest allowed vm type according to the regional quota limit as well as
        how many of these vm types can we launch to avoid QUOTA_EXCEEDED errors. Returns None if quota
        information wasn't properly loaded or allowed vcpu list is wrong.

        :param region_tag: tag of the node we are calculating the above for, example -> "aws:us-east-1"
        :type region_tag: str
        """
        cloud_provider, region = region_tag.split(":")

        # Get the quota limit
        quota_limit = self._get_quota_limits_for(
            cloud_provider=cloud_provider, region=region, spot=getattr(self.transfer_config, f"{cloud_provider}_use_spot_instances")
        )

        # No quota limits (quota limits weren't initialized properly during skyplane init)
        if quota_limit is None:
            return None

        config_vm_type = getattr(self.transfer_config, f"{cloud_provider}_instance_class")
        config_vcpus = Planner._vm_to_vcpus(cloud_provider, config_vm_type)
        if config_vcpus <= quota_limit:
            return config_vcpus, quota_limit // config_vcpus

        vcpus = None
        for value in Planner._VCPUS:
            if value <= quota_limit:
                vcpus = value
                break

        # shouldn't happen, but just in case we use more complicated vm types in the future
        if vcpus is None:
            return None

        # number of instances allowed by the quota with the selected vm type
        n_instances = quota_limit // vcpus
        logger.warning(
            f"Falling back to instance class `{Planner._vcpus_to_vm(cloud_provider, vcpus)}` at {region_tag} "
            f"due to cloud vCPU limit of {quota_limit}. You can visit https://skyplane.org/en/latest/increase_vcpus.html "
            "to learn more about how to increase your cloud vCPU limits for any cloud provider."
        )
        return (vcpus, n_instances)


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
