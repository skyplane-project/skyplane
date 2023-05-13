from importlib.resources import path
from typing import List, Optional, Tuple, Tuple
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


class Planner:
    # Only supporting "aws:m5.", "azure:StandardD_v5", and "gcp:n2-standard" instances for now
    _AWS_VCPUS = (96, 64, 48, 32, 16, 8, 4, 2)
    _AZURE_VCPUS = (96, 64, 48, 32, 16, 8, 4, 2)
    _GCP_VCPUS = (128, 96, 80, 64, 48, 32, 16, 8, 4, 2)

    def plan(self) -> TopologyPlan:
        raise NotImplementedError

    @staticmethod
    def _fall_back_to_smaller_vm_if_neccessary(cloud_provider: str, instance_type: str, quota_limit: int) -> Optional[str]:
        if cloud_provider not in ("aws", "azure", "gcp"):
            raise ValueError(f"Invalid cloud provider '{cloud_provider}'")

        num_vcpus, all_vcpus, vm_family = (
            (re.search(r"\d+x", instance_type), Planner._AWS_VCPUS, "m5.{}large")
            if cloud_provider == "aws"
            else (re.search(r"\d+", instance_type), Planner._AZURE_VCPUS, "Standard_D{}_v5")
            if cloud_provider == "azure"
            else (int(instance_type.split("-")[-1]), Planner._GCP_VCPUS, "n2-standard-{}")
        )

        vm_portions = Planner._calculate_vcpu_portions(num_vcpus, quota_limit, all_vcpus)
        if vm_portions is ():
            return None

        num_portions, vm_size = vm_portions
        return (num_portions, vm_family.format(vm_size if cloud_provider != "aws" else f"{vm_size // 4}x" if vm_size // 4 else "2"))

    @staticmethod
    def _calculate_vcpu_portions(num_vcpus: int, quota_limit: int, vcpu_options: List[int]) -> Tuple:
        # If the desired vCPU count is within the quota limit, don't fall back
        if num_vcpus <= quota_limit:
            return (1, num_vcpus)

        # Otherwise, try to split the desired vCPU count into smaller portions that are within the quota limit and use the largest option
        else:
            for vcpu_count in vcpu_options:
                if vcpu_count <= quota_limit and vcpu_count <= num_vcpus:
                    portions = num_vcpus // vcpu_count
                    remaining_vcpus = num_vcpus - (vcpu_count * portions)
                    # If the remaining vCPUs are 0, use the current option to launch all portions
                    if remaining_vcpus == 0:
                        return (portions, vcpu_count)  # [vcpu_count] * portions
        # Return an empty list if no valid vCPU portions were found
        return ()

    def _get_quota_limits_for(self, cloud_provider: str, region: str, spot: bool = False) -> Optional[int]:
        quota_limits = self.quota_limits.get(cloud_provider)
        if not quota_limits:
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
    n_connection: int
    quota_limit: Dict[str, any]
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

        # Calculate the number of vms to launch based on the vcpu quota
        cloud_provider, region = src_region_tag.split(":")
        num_portions, vm_type = 1, getattr(self.transfer_config, f"{cloud_provider}_use_spot_instances")  # default vm configuration
        spot = getattr(self.transfer_config, f"{cloud_provider}_instance_class")
        quota_limit = self._get_quota_limits_for(cloud_provider=cloud_provider, region=region, spot=spot)
        if quota_limit is not None:
            vm_portions = Planner._fall_back_to_smaller_vm_if_neccessary(
                cloud_provider=cloud_provider, instance_type=vm_type, quota_limit=quota_limit
            )
            if vm_portions is not None:
                num_portions, vm_type = vm_portions
                if num_portions > 1:
                    logger.warning(f"Falling back to {vm_type} at {region} due to the vCPU quota limit {quota_limit}")

        # TODO: Calculate this separetly for the destination, too. Since they might be of different cloud providers.

        # TODO: support on-sided transfers but not requiring VMs to be created in source/destination regions
        for i in range(num_portions * self.n_instances):
            plan.add_gateway(src_region_tag, vm_type)
            for dst_region_tag in dst_region_tags:
                plan.add_gateway(dst_region_tag, vm_type)  # FIXME: Change the vm_type here

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
                for i in range(self.n_instances):
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
