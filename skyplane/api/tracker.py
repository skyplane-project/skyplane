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
