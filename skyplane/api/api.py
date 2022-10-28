from dataclasses import dataclass
from datetime import datetime
from threading import Thread
from typing import List, Optional, Tuple
import uuid

from skyplane.api.auth_config import AWSConfig, AzureConfig, GCPConfig
from skyplane.api.dataplane import Dataplane
from skyplane.api.impl.planner import DirectPlanner
from skyplane.api.impl.transfer_job import TransferJob, CopyJob, SyncJob
from skyplane.api.impl.provisioner import Provisioner
from skyplane.obj_store.object_store_interface import ObjectStoreObject
from skyplane.utils import logger
from skyplane import tmp_log_dir

# types
TransferList = List[Tuple[ObjectStoreObject, ObjectStoreObject]]


@dataclass(frozen=True)
class TransferConfig:
    multipart_enabled: bool = False
    multipart_threshold_mb: int = 128
    multipart_chunk_size_mb: int = 64
    multipart_max_chunks: int = 10000


class TransferProgressTracker(Thread):
    def __init__(self, dataplane: Dataplane, jobs: List[TransferJob], transfer_config: TransferConfig):
        super().__init__()
        self.dataplane = dataplane
        self.jobs = jobs
        self.transfer_config = transfer_config
        logger.fs.debug(f"[TransferProgressTracker] Using dataplane {dataplane}")
        logger.fs.debug(f"[TransferProgressTracker] Initialized with {len(jobs)} jobs:")
        for job in jobs:
            logger.fs.debug(f"[TransferProgressTracker]   * {job}")
        logger.fs.debug(f"[TransferProgressTracker] Transfer config: {transfer_config}")

        # transfer state
        self.job_chunk_requests = {}
        self.job_pending_chunk_ids = {}
        self.job_complete_chunk_ids = {}

    def run(self):
        job_chunk_request_gen = {}
        for job in self.jobs:
            logger.fs.debug(f"[TransferProgressTracker] Dispatching job {job}")
            job_chunk_request_gen[job] = list(
                job.dispatch(
                    self.dataplane.source_gateways(),
                    transfer_config=self.transfer_config,
                )
            )
            self.job_pending_chunk_ids[job] = set([cr.chunk.chunk_id for cr in job_chunk_request_gen[job]])
            self.job_complete_chunk_ids[job] = set()
            logger.fs.debug(f"[TransferProgressTracker] Job {job} dispatched with {len(job_chunk_request_gen[job])} chunks")
        # self.monitor_transfer()
        # for job in self.jobs:
        #     logger.fs.debug(f"[TransferProgressTracker] Verifying job {job}")
        #     job.verify()

    def monitor_transfer(self):
        # todo implement transfer monitoring to update job_complete_chunk_ids and job_pending_chunk_ids while the transfer is in progress
        pass

    def query_bytes_remaining(self):
        bytes_remaining_per_job = {
            job: sum(
                [cr.chunk.chunk_length_bytes for cr in self.job_chunk_requests[job] if cr.chunk.chunk_id in self.job_pending_chunk_ids[job]]
            )
            for job in self.jobs
        }
        logger.fs.debug(f"[TransferProgressTracker] Bytes remaining per job: {bytes_remaining_per_job}")
        bytes_remaining = sum(bytes_remaining_per_job.values())
        return bytes_remaining


class SkyplaneClient:
    def __init__(
        self,
        aws_config: Optional[AWSConfig] = None,
        azure_config: Optional[AzureConfig] = None,
        gcp_config: Optional[GCPConfig] = None,
        log_dir: Optional[str] = None,
    ):
        self.aws_auth = aws_config.make_auth_provider() if aws_config else None
        self.azure_auth = azure_config.make_auth_provider() if azure_config else None
        self.gcp_auth = gcp_config.make_auth_provider() if gcp_config else None
        self.log_dir = (
            tmp_log_dir / "transfer_logs" / f"{datetime.now().strftime('%Y%m%d_%H%M%S')}-{uuid.uuid4()}" if log_dir is None else log_dir
        )

        # set up logging
        self.log_dir.mkdir(parents=True, exist_ok=True)
        logger.open_log_file(self.log_dir / "client.log")

        self.provisioner = Provisioner(
            host_uuid=uuid.UUID(int=uuid.getnode()).hex,
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
        logger.fs.info(f"[SkyplaneClient.direct_dataplane] Topology: {topo.to_json()}")
        return Dataplane(topology=topo, provisioner=self.provisioner, **kwargs)

    # main API methods to dispatch transfers to dataplane
    def queue_copy(
        self,
        src: str,
        dst: str,
        recursive: bool = False,
    ):
        job = CopyJob(src, dst, recursive)
        logger.fs.debug(f"[SkyplaneClient] Queued copy job {job}")
        self.jobs_to_dispatch.append(job)

    def queue_sync(
        self,
        src: str,
        dst: str,
        recursive: bool = False,
    ):
        job = SyncJob(src, dst, recursive)
        logger.fs.debug(f"[SkyplaneClient] Queued sync job {job}")
        self.jobs_to_dispatch.append(job)

    def run_async(self, dataplane: Dataplane, **kwargs) -> TransferProgressTracker:
        if not dataplane.provisioned:
            logger.error("Dataplane must be pre-provisioned. Call dataplane.provision() before starting a transfer")
        config = TransferConfig(**kwargs)
        tracker = TransferProgressTracker(dataplane, self.jobs_to_dispatch, config)
        tracker.start()
        logger.fs.info(f"[SkyplaneClient] Started async transfer with {len(self.jobs_to_dispatch)} jobs")
        self.jobs_to_dispatch = []
        return tracker

    def run(self, dataplane: Dataplane):
        tracker = self.run_async(dataplane)
        logger.fs.debug(f"[SkyplaneClient] Waiting for transfer to complete")
        tracker.join()
