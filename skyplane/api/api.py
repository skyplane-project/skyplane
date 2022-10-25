from dataclasses import dataclass
from threading import Thread
from typing import List, Optional, Tuple

from skyplane.api.auth_config import AWSConfig, AzureConfig, GCPConfig
from skyplane.api.dataplane import Dataplane
from skyplane.api.impl.planner import DirectPlanner
from skyplane.api.impl.transfer_job import TransferJob, CopyJob, SyncJob
from skyplane.api.impl.provisioner import Provisioner
from skyplane.obj_store.object_store_interface import ObjectStoreObject
from skyplane.utils import logger

# types
TransferList = List[Tuple[ObjectStoreObject, ObjectStoreObject]]


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
        planner = DirectPlanner(
            src_cloud_provider,
            src_region,
            dst_cloud_provider,
            dst_region,
            n_vms,
            num_connections,
        )
        topo = planner.plan()
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
