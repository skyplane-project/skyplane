from threading import Thread
from typing import Dict, List, Set

from skyplane.api.config import TransferConfig
from skyplane.api.impl.transfer_job import TransferJob
from skyplane.chunk import ChunkRequest
from skyplane.utils import logger


class TransferProgressTracker(Thread):
    def __init__(self, dataplane, jobs: List[TransferJob], transfer_config: TransferConfig):
        super().__init__()
        self.dataplane = dataplane
        self.jobs = {job.uuid: job for job in jobs}
        self.transfer_config = transfer_config

        # log job details
        logger.fs.debug(f"[TransferProgressTracker] Using dataplane {dataplane}")
        logger.fs.debug(f"[TransferProgressTracker] Initialized with {len(jobs)} jobs:")
        for job_uuid, job in self.jobs.items():
            logger.fs.debug(f"[TransferProgressTracker]   * {job_uuid}: {job}")
        logger.fs.debug(f"[TransferProgressTracker] Transfer config: {transfer_config}")

        # transfer state
        self.job_chunk_requests: Dict[str, List[ChunkRequest]] = {}
        self.job_pending_chunk_ids: Dict[str, Set[int]] = {}
        self.job_complete_chunk_ids: Dict[str, Set[int]] = {}

    def run(self):
        for job_uuid, job in self.jobs.items():
            logger.fs.debug(f"[TransferProgressTracker] Dispatching job {job}")
            self.job_chunk_requests[job_uuid] = list(
                job.dispatch(
                    self.dataplane.source_gateways(),
                    transfer_config=self.transfer_config,
                )
            )
            self.job_pending_chunk_ids[job_uuid] = set([cr.chunk.chunk_id for cr in self.job_chunk_requests[job_uuid]])
            self.job_complete_chunk_ids[job_uuid] = set()
            logger.fs.debug(f"[TransferProgressTracker] Job {job} dispatched with {len(self.job_chunk_requests[job_uuid])} chunk requests")
        # self.monitor_transfer()
        # for job in self.jobs:
        #     logger.fs.debug(f"[TransferProgressTracker] Verifying job {job}")
        #     job.verify()

    def monitor_transfer(self):
        # todo implement transfer monitoring to update job_complete_chunk_ids and job_pending_chunk_ids while the transfer is in progress
        pass

    def query_bytes_remaining(self):
        bytes_remaining_per_job = {}
        for job_uuid in self.jobs.keys():
            bytes_remaining_per_job[job_uuid] = sum(
                [
                    cr.chunk.chunk_length_bytes
                    for cr in self.job_chunk_requests[job_uuid]
                    if cr.chunk.chunk_id in self.job_pending_chunk_ids[job_uuid]
                ]
            )
        logger.fs.debug(f"[TransferProgressTracker] Bytes remaining per job: {bytes_remaining_per_job}")
        return sum(bytes_remaining_per_job.values())
