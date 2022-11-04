import functools
import json
import time
from datetime import datetime
from threading import Thread

import urllib3
from typing import TYPE_CHECKING, Dict, List, Optional, Set

from skyplane import exceptions
from skyplane.api.transfer_config import TransferConfig
from skyplane.chunk import ChunkRequest, ChunkState
from skyplane.utils import logger, imports
from skyplane.utils.fn import do_parallel

if TYPE_CHECKING:
    from skyplane.api.impl.transfer_job import TransferJob


class TransferProgressTracker(Thread):
    def __init__(self, dataplane, jobs: List["TransferJob"], transfer_config: TransferConfig):
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
        self.job_pending_chunk_ids: Dict[str, Set[str]] = {}
        self.job_complete_chunk_ids: Dict[str, Set[str]] = {}
        self.errors: Optional[Dict[str, List[str]]] = None

        # http_pool
        self.http_pool = urllib3.PoolManager(retries=urllib3.Retry(total=3))

    def __str__(self):
        return f"TransferProgressTracker({self.dataplane}, {self.jobs})"

    def run(self):
        for job_uuid, job in self.jobs.items():
            logger.fs.debug(f"[TransferProgressTracker] Dispatching job {job.uuid}")
            self.job_chunk_requests[job_uuid] = list(job.dispatch(self.dataplane, transfer_config=self.transfer_config))
            self.job_pending_chunk_ids[job_uuid] = set([cr.chunk.chunk_id for cr in self.job_chunk_requests[job_uuid]])
            self.job_complete_chunk_ids[job_uuid] = set()
            logger.fs.debug(
                f"[TransferProgressTracker] Job {job.uuid} dispatched with {len(self.job_chunk_requests[job_uuid])} chunk requests"
            )
        self.monitor_transfer()
        for job in self.jobs.values():
            logger.fs.debug(f"[TransferProgressTracker] Finalizing job {job.uuid}")
            job.finalize()
        for job in self.jobs.values():
            logger.fs.debug(f"[TransferProgressTracker] Verifying job {job.uuid}")
            job.verify()

    @imports.inject("pandas")
    def monitor_transfer(pd, self):
        # todo implement transfer monitoring to update job_complete_chunk_ids and job_pending_chunk_ids while the transfer is in progress
        sinks = self.dataplane.topology.sink_instances()
        sink_regions = set([sink.region for sink in sinks])
        while any([len(self.job_pending_chunk_ids[job_uuid]) > 0 for job_uuid in self.job_pending_chunk_ids]):
            # refresh shutdown status by running noop
            do_parallel(lambda i: i.run_command("echo 1"), self.dataplane.bound_nodes.values(), n=-1)

            # check for errors and exit if there are any (while setting debug flags)
            errors = self.dataplane.check_error_logs()
            if any(errors.values()):
                self.errors = errors
                raise exceptions.SkyplaneGatewayException("Transfer failed with errors", errors)

            log_df = pd.DataFrame(self._query_chunk_status())
            if log_df.empty:
                logger.warning("No chunk status log entries yet")
                time.sleep(0.05)
                continue

            is_complete_rec = (
                lambda row: row["state"] == ChunkState.upload_complete
                and row["instance"] in [s.instance for s in sinks]
                and row["region"] in [s.region for s in sinks]
            )
            sink_status_df = log_df[log_df.apply(is_complete_rec, axis=1)]
            completed_status = sink_status_df.groupby("chunk_id").apply(lambda x: set(x["region"].unique()) == set(sink_regions))
            completed_chunk_ids = completed_status[completed_status].index

            # update job_complete_chunk_ids and job_pending_chunk_ids
            for job_uuid, job in self.jobs.items():
                job_complete_chunk_ids = set(chunk_id for chunk_id in completed_chunk_ids if self._chunk_to_job_map[chunk_id] == job_uuid)
                self.job_complete_chunk_ids[job_uuid] = self.job_complete_chunk_ids[job_uuid].union(job_complete_chunk_ids)
                self.job_pending_chunk_ids[job_uuid] = self.job_pending_chunk_ids[job_uuid].difference(job_complete_chunk_ids)

            # sleep
            time.sleep(0.05)

    @property
    @functools.lru_cache(maxsize=1)
    def _chunk_to_job_map(self):
        return {cr.chunk.chunk_id: job_uuid for job_uuid, crs in self.job_chunk_requests.items() for cr in crs}

    def _query_chunk_status(self):
        def get_chunk_status(args):
            node, instance = args
            reply = self.http_pool.request("GET", f"{instance.gateway_api_url}/api/v1/chunk_status_log")
            if reply.status != 200:
                raise Exception(
                    f"Failed to get chunk status from gateway instance {instance.instance_name()}: {reply.data.decode('utf-8')}"
                )
            logs = []
            for log_entry in json.loads(reply.data.decode("utf-8"))["chunk_status_log"]:
                log_entry["region"] = node.region
                log_entry["instance"] = node.instance
                log_entry["time"] = datetime.fromisoformat(log_entry["time"])
                log_entry["state"] = ChunkState.from_str(log_entry["state"])
                logs.append(log_entry)
            return logs

        rows = []
        for result in do_parallel(get_chunk_status, self.dataplane.bound_nodes.items(), n=-1, return_args=False):
            rows.extend(result)
        return rows

    @property
    def is_complete(self):
        return all([len(self.job_pending_chunk_ids[job_uuid]) == 0 for job_uuid in self.jobs.keys()])

    def query_bytes_remaining(self):
        if len(self.job_chunk_requests) == 0:
            return None
        bytes_remaining_per_job = {}
        for job_uuid in self.job_pending_chunk_ids.keys():
            bytes_remaining_per_job[job_uuid] = sum(
                [
                    cr.chunk.chunk_length_bytes
                    for cr in self.job_chunk_requests[job_uuid]
                    if cr.chunk.chunk_id in self.job_pending_chunk_ids[job_uuid]
                ]
            )
        logger.fs.debug(f"[TransferProgressTracker] Bytes remaining per job: {bytes_remaining_per_job}")
        return sum(bytes_remaining_per_job.values())
