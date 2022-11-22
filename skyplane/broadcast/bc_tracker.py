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
from skyplane.api.usage.client import UsageClient
from skyplane.utils.definitions import GB
from skyplane.api.impl.tracker import TransferProgressTracker

if TYPE_CHECKING:
    from skyplane.broadcast.bc_transfer_job import BCTransferJob


class BCTransferProgressTracker(TransferProgressTracker):
    def __init__(self, dataplane, jobs: List["BCTransferJob"], transfer_config: TransferConfig):
        self.dataplane = dataplane
        self.type_list = set([job.type for job in jobs])
        self.recursive_list = set([str(job.recursive) for job in jobs])

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
        src_cloud_provider = self.dataplane.src_region_tag.split(":")[0]
        dst_cloud_providers = [dst_region_tag.split(":")[0] for dst_region_tag in self.dataplane.dst_region_tags]

        args = {
            "cmd": ",".join(self.type_list),
            "recursive": ",".join(self.recursive_list),
            "multipart": self.transfer_config.multipart_enabled,
            "instances_per_region": self.dataplane.max_instances,
            "src_instance_type": getattr(self.transfer_config, f"{src_cloud_provider}_instance_class"),
            "dst_instance_type": getattr(self.transfer_config, f"{dst_cloud_providers}_instance_class"),
            "src_spot_instance": getattr(self.transfer_config, f"{src_cloud_provider}_use_spot_instances"),
            "dst_spot_instance": getattr(self.transfer_config, f"{dst_cloud_providers}_use_spot_instances"),
        }
        session_start_timestamp_ms = int(time.time() * 1000)
        try:
            for job_uuid, job in self.jobs.items():
                logger.fs.debug(f"[TransferProgressTracker] Dispatching job {job.uuid}")
                self.job_chunk_requests[job_uuid] = list(job.dispatch(self.dataplane, transfer_config=self.transfer_config))
                self.job_pending_chunk_ids[job_uuid] = set([cr.chunk.chunk_id for cr in self.job_chunk_requests[job_uuid]])
                self.job_complete_chunk_ids[job_uuid] = set()
                logger.fs.debug(
                    f"[TransferProgressTracker] Job {job.uuid} dispatched with {len(self.job_chunk_requests[job_uuid])} chunk requests"
                )
        except Exception as e:
            UsageClient.log_exception(
                "dispatch job", e, args, self.dataplane.src_region_tag, self.dataplane.dst_region_tag, session_start_timestamp_ms
            )
            raise e

        # Record only the transfer time
        start_time = int(time.time())
        try:
            self.monitor_transfer()
        except exceptions.SkyplaneGatewayException as err:
            reformat_err = Exception(err.pretty_print_str()[37:])
            UsageClient.log_exception(
                "monitor transfer",
                reformat_err,
                args,
                self.dataplane.src_region_tag,
                self.dataplane.dst_region_tag,
                session_start_timestamp_ms,
            )
            raise err
        except Exception as e:
            UsageClient.log_exception(
                "monitor transfer", e, args, self.dataplane.src_region_tag, self.dataplane.dst_region_tag, session_start_timestamp_ms
            )
            raise e
        end_time = int(time.time())

        try:
            for job in self.jobs.values():
                logger.fs.debug(f"[TransferProgressTracker] Finalizing job {job.uuid}")
                job.finalize()
        except Exception as e:
            UsageClient.log_exception(
                "finalize job", e, args, self.dataplane.src_region_tag, self.dataplane.dst_region_tag, session_start_timestamp_ms
            )
            raise e

        try:
            for job in self.jobs.values():
                logger.fs.debug(f"[TransferProgressTracker] Verifying job {job.uuid}")
                job.verify()
        except Exception as e:
            UsageClient.log_exception(
                "verify job", e, args, self.dataplane.src_region_tag, self.dataplane.dst_region_tag, session_start_timestamp_ms
            )
            raise e

        # transfer successfully completed
        transfer_stats = {
            "total_runtime_s": end_time - start_time,
            "throughput_gbits": self.calculate_size() / (end_time - start_time),
        }
        UsageClient.log_transfer(
            transfer_stats, args, self.dataplane.src_region_tag, self.dataplane.dst_region_tag, session_start_timestamp_ms
        )

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
