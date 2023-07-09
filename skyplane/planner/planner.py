from collections import defaultdict
from importlib.resources import path
from typing import Dict, List, Optional, Tuple, Tuple
import os
import csv

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
from skyplane.config_paths import config_path, azure_standardDv5_quota_path, aws_quota_path, gcp_quota_path
from skyplane.config import SkyplaneConfig


class Planner:
    def __init__(self, transfer_config: TransferConfig, quota_limits_file: Optional[str] = None):
        self.transfer_config = transfer_config
        self.config = SkyplaneConfig.load_config(config_path)
        self.n_instances = self.config.get_flag("max_instances")

        # Loading the quota information, add ibm cloud when it is supported
        quota_limits = {}
        if quota_limits_file is not None:
            with open(quota_limits_file, "r") as f:
                quota_limits = json.load(f)
        else:
            if os.path.exists(aws_quota_path):
                with aws_quota_path.open("r") as f:
                    quota_limits["aws"] = json.load(f)
            if os.path.exists(azure_standardDv5_quota_path):
                with azure_standardDv5_quota_path.open("r") as f:
                    quota_limits["azure"] = json.load(f)
            if os.path.exists(gcp_quota_path):
                with gcp_quota_path.open("r") as f:
                    quota_limits["gcp"] = json.load(f)
        self.quota_limits = quota_limits

        # Loading the vcpu information - a dictionary of dictionaries
        # {"cloud_provider": {"instance_name": vcpu_cost}}
        self.vcpu_info = defaultdict(dict)
        with path("skyplane.data", "vcpu_info.csv") as file_path:
            with open(file_path, "r") as csvfile:
                reader = csv.reader(csvfile)
                next(reader)  # Skip the header row

                for row in reader:
                    instance_name, cloud_provider, vcpu_cost = row
                    vcpu_cost = int(vcpu_cost)
                    self.vcpu_info[cloud_provider][instance_name] = vcpu_cost

    def plan(self) -> TopologyPlan:
        raise NotImplementedError

    def _vm_to_vcpus(self, cloud_provider: str, vm: str) -> int:
        """Gets the vcpu_cost of the given vm instance (instance_name)

        :param cloud_provider: name of the cloud_provider
        :type cloud_provider: str
        :param instance_name: name of the vm instance
        :type instance_name: str
        """
        return self.vcpu_info[cloud_provider][vm]

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
        quota_limits = self.quota_limits.get(cloud_provider, None)
        if not quota_limits:
            # User needs to reinitialize to save the quota information
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

    def _calculate_vm_types(self, region_tag: str) -> Optional[Tuple[str, int]]:
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

        config_vm_type = getattr(self.transfer_config, f"{cloud_provider}_instance_class")

        # No quota limits (quota limits weren't initialized properly during skyplane init)
        if quota_limit is None:
            logger.warning(
                f"Quota limit file not found for {region_tag}. Try running `skyplane init --reinit-{cloud_provider}` to load the quota information"
            )
            # return default instance type and number of instances
            return config_vm_type, self.n_instances

        config_vcpus = self._vm_to_vcpus(cloud_provider, config_vm_type)
        if config_vcpus <= quota_limit:
            return config_vm_type, quota_limit // config_vcpus

        vm_type, vcpus = None, None
        for instance_name, vcpu_cost in sorted(self.vcpu_info[cloud_provider].items(), key=lambda x: x[1], reverse=True):
            if vcpu_cost <= quota_limit:
                vm_type, vcpus = instance_name, vcpu_cost
                break

        # shouldn't happen, but just in case we use more complicated vm types in the future
        assert vm_type is not None and vcpus is not None

        # number of instances allowed by the quota with the selected vm type
        n_instances = quota_limit // vcpus
        logger.warning(
            f"Falling back to instance class `{vm_type}` at {region_tag} "
            f"due to cloud vCPU limit of {quota_limit}. You can visit https://skyplane.org/en/latest/increase_vcpus.html "
            "to learn more about how to increase your cloud vCPU limits for any cloud provider."
        )
        return (vm_type, n_instances)

    def _get_vm_type_and_instances(
        self, src_region_tag: Optional[str] = None, dst_region_tags: Optional[List[str]] = None
    ) -> Tuple[Dict[str, str], int]:
        """Dynamically calculates the vm type each region can use (both the source region and all destination regions)
        based on their quota limits and calculates the number of vms to launch in all regions by conservatively
        taking the minimum of all regions to stay consistent.

        :param src_region_tag: the source region tag (default: None)
        :type src_region_tag: Optional[str]
        :param dst_region_tags: a list of the destination region tags (defualt: None)
        :type dst_region_tags: Optional[List[str]]
        """

        # One of them has to provided
        # assert src_region_tag is not None or dst_region_tags is not None, "There needs to be at least one source or destination"
        src_tags = [src_region_tag] if src_region_tag is not None else []
        dst_tags = dst_region_tags if dst_region_tags is not None else []

        if src_region_tag:
            assert len(src_region_tag.split(":")) == 2, f"Source region tag {src_region_tag} must be in the form of `cloud_provider:region`"
        if dst_region_tags:
            assert (
                len(dst_region_tags[0].split(":")) == 2
            ), f"Destination region tag {dst_region_tags} must be in the form of `cloud_provider:region`"

        # do_parallel returns tuples of (region_tag, (vm_type, n_instances))
        vm_info = do_parallel(self._calculate_vm_types, src_tags + dst_tags)
        # Specifies the vm_type for each region
        vm_types = {v[0]: v[1][0] for v in vm_info}  # type: ignore
        # Taking the minimum so that we can use the same number of instances for both source and destination
        n_instances = min([self.n_instances] + [v[1][1] for v in vm_info])  # type: ignore
        return vm_types, n_instances


class UnicastDirectPlanner(Planner):
    # DO NOT USE THIS - broken for single-region transfers
    def __init__(self, n_instances: int, n_connections: int, transfer_config: TransferConfig, quota_limits_file: Optional[str] = None):
        super().__init__(transfer_config, quota_limits_file)
        self.n_instances = n_instances
        self.n_connections = n_connections

    def plan(self, jobs: List[TransferJob]) -> TopologyPlan:
        # make sure only single destination
        for job in jobs:
            assert len(job.dst_ifaces) == 1, f"DirectPlanner only support single destination jobs, got {len(job.dst_ifaces)}"

        src_region_tag = jobs[0].src_iface.region_tag()
        dst_region_tag = jobs[0].dst_ifaces[0].region_tag()

        assert len(src_region_tag.split(":")) == 2, f"Source region tag {src_region_tag} must be in the form of `cloud_provider:region`"
        assert (
            len(dst_region_tag.split(":")) == 2
        ), f"Destination region tag {dst_region_tag} must be in the form of `cloud_provider:region`"

        # jobs must have same sources and destinations
        for job in jobs[1:]:
            assert job.src_iface.region_tag() == src_region_tag, "All jobs must have same source region"
            assert job.dst_ifaces[0].region_tag() == dst_region_tag, "All jobs must have same destination region"

        plan = TopologyPlan(src_region_tag=src_region_tag, dest_region_tags=[dst_region_tag])

        # Dynammically calculate n_instances based on quota limits
        vm_types, n_instances = self._get_vm_type_and_instances(src_region_tag=src_region_tag, dst_region_tags=[dst_region_tag])

        # TODO: support on-sided transfers but not requiring VMs to be created in source/destination regions
        for i in range(n_instances):
            plan.add_gateway(src_region_tag, vm_types[src_region_tag])
            plan.add_gateway(dst_region_tag, vm_types[dst_region_tag])

        # ids of gateways in dst region
        dst_gateways = plan.get_region_gateways(dst_region_tag)

        src_program = GatewayProgram()
        dst_program = GatewayProgram()

        for job in jobs:
            src_bucket = job.src_iface.bucket()
            dst_bucket = job.dst_ifaces[0].bucket()

            # give each job a different partition id, so we can read/write to different buckets
            partition_id = job.uuid

            # source region gateway program
            obj_store_read = src_program.add_operator(
                GatewayReadObjectStore(src_bucket, src_region_tag, self.n_connections), partition_id=partition_id
            )
            mux_or = src_program.add_operator(GatewayMuxOr(), parent_handle=obj_store_read, partition_id=partition_id)
            for i in range(n_instances):
                src_program.add_operator(
                    GatewaySend(
                        target_gateway_id=dst_gateways[i].gateway_id,
                        region=src_region_tag,
                        num_connections=self.n_connections,
                        compress=True,
                        encrypt=True,
                    ),
                    parent_handle=mux_or,
                    partition_id=partition_id,
                )

            # dst region gateway program
            recv_op = dst_program.add_operator(GatewayReceive(decompress=True, decrypt=True), partition_id=partition_id)
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
    def __init__(self, n_instances: int, n_connections: int, transfer_config: TransferConfig, quota_limits_file: Optional[str] = None):
        super().__init__(transfer_config, quota_limits_file)
        self.n_instances = n_instances
        self.n_connections = n_connections

    def plan(self, jobs: List[TransferJob]) -> TopologyPlan:
        src_region_tag = jobs[0].src_iface.region_tag()
        dst_region_tags = [iface.region_tag() for iface in jobs[0].dst_ifaces]

        # jobs must have same sources and destinations
        for job in jobs[1:]:
            assert job.src_iface.region_tag() == src_region_tag, "All jobs must have same source region"
            assert [iface.region_tag() for iface in job.dst_ifaces] == dst_region_tags, "Add jobs must have same destination set"

        plan = TopologyPlan(src_region_tag=src_region_tag, dest_region_tags=dst_region_tags)

        # Dynammically calculate n_instances based on quota limits
        if src_region_tag.split(":")[0] == "test":
            vm_types = None
            n_instances = self.n_instances
        else:
            vm_types, n_instances = self._get_vm_type_and_instances(src_region_tag=src_region_tag, dst_region_tags=dst_region_tags)

        # TODO: support on-sided transfers but not requiring VMs to be created in source/destination regions
        for i in range(n_instances):
            plan.add_gateway(src_region_tag, vm_types[src_region_tag] if vm_types else None)
            for dst_region_tag in dst_region_tags:
                plan.add_gateway(dst_region_tag, vm_types[dst_region_tag] if vm_types else None)

        # initialize gateway programs per region
        dst_program = {dst_region: GatewayProgram() for dst_region in dst_region_tags}
        src_program = GatewayProgram()

        # iterate through all jobs
        for job in jobs:
            src_bucket = job.src_iface.bucket()
            src_region_tag = job.src_iface.region_tag()
            src_provider = src_region_tag.split(":")[0]

            # give each job a different partition id, so we can read/write to different buckets
            partition_id = job.uuid

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
                            compress=self.transfer_config.use_compression,
                            encrypt=self.transfer_config.use_e2ee,
                        ),
                        parent_handle=mux_or,
                        partition_id=partition_id,
                    )

                # each gateway also recieves data from source
                recv_op = dst_program[dst_region_tag].add_operator(
                    GatewayReceive(decompress=self.transfer_config.use_compression, decrypt=self.transfer_config.use_e2ee),
                    partition_id=partition_id,
                )
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


class DirectPlannerSourceOneSided(MulticastDirectPlanner):
    """Planner that only creates VMs in the source region"""

    def plan(self, jobs: List[TransferJob]) -> TopologyPlan:
        src_region_tag = jobs[0].src_iface.region_tag()
        dst_region_tags = [iface.region_tag() for iface in jobs[0].dst_ifaces]
        # jobs must have same sources and destinations
        for job in jobs[1:]:
            assert job.src_iface.region_tag() == src_region_tag, "All jobs must have same source region"
            assert [iface.region_tag() for iface in job.dst_ifaces] == dst_region_tags, "Add jobs must have same destination set"

        plan = TopologyPlan(src_region_tag=src_region_tag, dest_region_tags=dst_region_tags)

        # Dynammically calculate n_instances based on quota limits
        vm_types, n_instances = self._get_vm_type_and_instances(src_region_tag=src_region_tag)

        # TODO: support on-sided transfers but not requiring VMs to be created in source/destination regions
        for i in range(n_instances):
            plan.add_gateway(src_region_tag, vm_types[src_region_tag])

        # initialize gateway programs per region
        src_program = GatewayProgram()

        # iterate through all jobs
        for job in jobs:
            src_bucket = job.src_iface.bucket()
            src_region_tag = job.src_iface.region_tag()
            src_provider = src_region_tag.split(":")[0]

            # give each job a different partition id, so we can read/write to different buckets
            partition_id = job.uuid

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
                src_program.add_operator(
                    GatewayWriteObjectStore(dst_bucket, dst_region_tag, self.n_connections, key_prefix=dst_prefix),
                    parent_handle=mux_and,
                    partition_id=partition_id,
                )
                # update cost per GB
                plan.cost_per_gb += compute.CloudProvider.get_transfer_cost(src_region_tag, dst_region_tag)

        # set gateway programs
        plan.set_gateway_program(src_region_tag, src_program)
        return plan


class DirectPlannerDestOneSided(MulticastDirectPlanner):
    """Planner that only creates instances in the destination region"""

    def plan(self, jobs: List[TransferJob]) -> TopologyPlan:
        # only create in destination region
        src_region_tag = jobs[0].src_iface.region_tag()
        dst_region_tags = [iface.region_tag() for iface in jobs[0].dst_ifaces]
        # jobs must have same sources and destinations
        for job in jobs[1:]:
            assert job.src_iface.region_tag() == src_region_tag, "All jobs must have same source region"
            assert [iface.region_tag() for iface in job.dst_ifaces] == dst_region_tags, "Add jobs must have same destination set"

        plan = TopologyPlan(src_region_tag=src_region_tag, dest_region_tags=dst_region_tags)

        # Dynammically calculate n_instances based on quota limits
        vm_types, n_instances = self._get_vm_type_and_instances(dst_region_tags=dst_region_tags)

        # TODO: support on-sided transfers but not requiring VMs to be created in source/destination regions
        for i in range(n_instances):
            for dst_region_tag in dst_region_tags:
                plan.add_gateway(dst_region_tag, vm_types[dst_region_tag])

        # initialize gateway programs per region
        dst_program = {dst_region: GatewayProgram() for dst_region in dst_region_tags}

        # iterate through all jobs
        for job in jobs:
            src_bucket = job.src_iface.bucket()
            src_region_tag = job.src_iface.region_tag()
            src_provider = src_region_tag.split(":")[0]

            partition_id = job.uuid

            # send to all destination
            dst_prefixes = job.dst_prefixes
            for i in range(len(job.dst_ifaces)):
                dst_iface = job.dst_ifaces[i]
                dst_prefix = dst_prefixes[i]
                dst_region_tag = dst_iface.region_tag()
                dst_bucket = dst_iface.bucket()
                dst_gateways = plan.get_region_gateways(dst_region_tag)

                # source region gateway program
                obj_store_read = dst_program[dst_region_tag].add_operator(
                    GatewayReadObjectStore(src_bucket, src_region_tag, self.n_connections), partition_id=partition_id
                )

                dst_program[dst_region_tag].add_operator(
                    GatewayWriteObjectStore(dst_bucket, dst_region_tag, self.n_connections, key_prefix=dst_prefix),
                    parent_handle=obj_store_read,
                    partition_id=partition_id,
                )

                # update cost per GB
                plan.cost_per_gb += compute.CloudProvider.get_transfer_cost(src_region_tag, dst_region_tag)

        # set gateway programs
        for dst_region_tag, program in dst_program.items():
            plan.set_gateway_program(dst_region_tag, program)
        return plan
