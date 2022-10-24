from dataclasses import dataclass
from threading import Thread
from typing import Optional, List, Tuple

from skyplane.api.auth_config import AWSConfig, AzureConfig, GCPConfig
from skyplane.api.dataplane import Dataplane
from skyplane.api.provisioner import Provisioner
from skyplane.compute.cloud_providers import CloudProvider
from skyplane.obj_store.object_store_interface import ObjectStoreObject
from skyplane.replicate.replication_plan import ReplicationTopology

# types
from skyplane.utils import logger

TransferList = List[Tuple[ObjectStoreObject, ObjectStoreObject]]


@dataclass
class TransferJob:
    src_path: str
    dst_path: str
    recursive: bool = False

    def estimate_cost(self):
        # TODO
        raise NotImplementedError


@dataclass
class CopyJob(TransferJob):
    def filter_transfer_list(self, transfer_list: TransferList) -> TransferList:
        return transfer_list


@dataclass
class SyncJob(CopyJob):
    def filter_transfer_list(self, transfer_list: TransferList) -> TransferList:
        transfer_pairs = []
        for src_obj, dst_obj in transfer_list:
            if not dst_obj.exists or (src_obj.last_modified > dst_obj.last_modified or src_obj.size != dst_obj.size):
                transfer_pairs.append((src_obj, dst_obj))
        return transfer_pairs


@dataclass
class TransferProgressTracker(Thread):
    dataplane: Dataplane
    jobs: List[TransferJob]

    def run(self):
        # todo: implement following cp_replicate.py and cli.py
        raise NotImplementedError()


class SkyplaneClient:
    def __init__(
        self,
        aws_config: Optional[AWSConfig] = None,
        azure_config: Optional[AzureConfig] = None,
        gcp_config: Optional[GCPConfig] = None,
        host_uuid: Optional[str] = None,
    ):
        self.aws_auth = aws_config.make_auth_provider() if aws_config else None
        self.azure_auth = azure_config.make_auth_provider() if azure_config else None
        self.gcp_auth = gcp_config.make_auth_provider() if gcp_config else None
        self.host_uuid = host_uuid
        self.provisioner = Provisioner(
            aws_auth=self.aws_auth,
            azure_auth=self.azure_auth,
            gcp_auth=self.gcp_auth,
        )
        self.jobs_to_dispatch: List[TransferJob] = []

    def copy(self, src: str, dst: str, recursive: bool = False, num_vms: int = 1):
        raise NotImplementedError("Simple copy not yet implemented")

    # methods to create dataplane

    def _make_direct_topology(
        self,
        src_region_tag: str,
        dst_region_tag: str,
        max_instances: int,
        num_connections: int,
    ):
        if src_region_tag == dst_region_tag:  # intra-region transfer w/o solver
            topo = ReplicationTopology()
            for i in range(max_instances):
                topo.add_objstore_instance_edge(src_region_tag, src_region_tag, i)
                topo.add_instance_objstore_edge(src_region_tag, i, src_region_tag)
            topo.cost_per_gb = 0
            return topo
        else:  # inter-region transfer w/ solver
            topo = ReplicationTopology()
            for i in range(max_instances):
                topo.add_objstore_instance_edge(src_region_tag, src_region_tag, i)
                topo.add_instance_instance_edge(src_region_tag, i, dst_region_tag, i, num_connections)
                topo.add_instance_objstore_edge(dst_region_tag, i, dst_region_tag)
            topo.cost_per_gb = CloudProvider.get_transfer_cost(src_region_tag, dst_region_tag)
            return topo

    def direct_dataplane(
        self,
        src_cloud_provider: str,
        src_region: str,
        dst_cloud_provider: str,
        dst_region: str,
        n_vms: int = 1,
        num_connections: int = 32,
        **kwargs,
    ):
        topo = self._make_direct_topology(
            f"{src_cloud_provider}:{src_region}", f"{dst_cloud_provider}:{dst_region}", n_vms, num_connections
        )
        return Dataplane(topology=topo, provisioner=self.provisioner, **kwargs)

    # main API methods to dispatch transfers to dataplane

    def queue_copy(
        self,
        src: str,
        dst: str,
        recursive: bool = False,
    ):
        job = CopyJob(src, dst, recursive)
        self.jobs_to_dispatch.append(job)

    def queue_sync(
        self,
        src: str,
        dst: str,
        recursive: bool = False,
    ):
        job = SyncJob(src, dst, recursive)
        self.jobs_to_dispatch.append(job)

    def run_async(self, dataplane: Dataplane):
        if not dataplane.provisioned:
            logger.error("Dataplane must be pre-provisioned. Call dataplane.provision() before starting a transfer")
        tracker = TransferProgressTracker(dataplane, self.jobs_to_dispatch)
        tracker.start()
