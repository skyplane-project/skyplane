import functools
import json
import time
from abc import ABC
from datetime import datetime
from threading import Thread

import urllib3
from typing import TYPE_CHECKING, Dict, List, Optional, Set

from skyplane import exceptions
from skyplane.api.config import TransferConfig
from skyplane.chunk import ChunkRequest, ChunkState, Chunk
from skyplane.utils import logger, imports
from skyplane.utils.definitions import tmp_log_dir
from skyplane.utils.fn import do_parallel
from skyplane.api.usage import UsageClient
from skyplane.utils.definitions import GB

if TYPE_CHECKING:
    from skyplane.api.transfer_job import TransferJob


class TransferHook(ABC):
    """Hook that shows transfer related stats"""

    def on_dispatch_start(self):
        """Starting the dispatch job"""
        raise NotImplementedError()

    def on_chunk_dispatched(self, chunks: List[Chunk]):
        """Dispatching data chunks to transfer"""
        raise NotImplementedError()

    def on_dispatch_end(self):
        """Ending the dispatch job"""
        raise NotImplementedError()

    def on_chunk_completed(self, chunks: List[Chunk]):
        """Chunks are all transferred"""
        raise NotImplementedError()

    def on_transfer_end(self, transfer_stats):
        """Ending the transfer job"""
        raise NotImplementedError()

    def on_transfer_error(self, error):
        """Showing the tranfer error if it fails"""
        raise NotImplementedError()


class EmptyTransferHook(TransferHook):
    """Empty transfer hook that does nothing"""

    def __init__(self):
        return

    def on_dispatch_start(self):
        return

    def on_chunk_dispatched(self, chunks: List[Chunk]):
        return

    def on_dispatch_end(self):
        return

    def on_chunk_completed(self, chunks: List[Chunk]):
        return

    def on_transfer_end(self, transfer_stats):
        return

    def on_transfer_error(self, error):
        return


class TransferProgressTracker(Thread):
    """Tracks transfer progress in one tranfer session"""

    def __init__(self, dataplane, jobs: List["TransferJob"], transfer_config: TransferConfig, hooks: TransferHook):
        """
        :param dataplane: dataplane that starts the transfer
        :type dataplane: Dataplane
        :param jobs: list of transfer jobs launched in parallel
        :type jobs: List
        :param transfer_config: the configuration during the transfer
        :type transfer_config: TransferConfig
        :param hooks: the hook that shows transfer related stats
        :type hooks: TransferHook
        """
        super().__init__()
        self.dataplane = dataplane
        self.jobs = {job.uuid: job for job in jobs}
        self.transfer_config = transfer_config
        self.transfer_dir = tmp_log_dir / "transfer_logs" / datetime.now().strftime("%Y%m%d_%H%M%S")
        self.transfer_dir.mkdir(exist_ok=True, parents=True)
        if hooks is None:
            self.hooks = EmptyTransferHook()
        else:
            self.hooks = hooks

        # log job details
        logger.fs.debug(f"[TransferProgressTracker] Using dataplane {dataplane}")
        logger.fs.debug(f"[TransferProgressTracker] Initialized with {len(jobs)} jobs:")
        for job_uuid, job in self.jobs.items():
            logger.fs.debug(f"[TransferProgressTracker]   * {job_uuid}: {job}")
        logger.fs.debug(f"[TransferProgressTracker] Transfer config: {transfer_config}")

        # transfer state
        self.job_chunk_requests: Dict[str, Dict[str, ChunkRequest]] = {}
        self.job_pending_chunk_ids: Dict[str, Set[str]] = {}
        self.job_complete_chunk_ids: Dict[str, Set[str]] = {}
        self.errors: Optional[Dict[str, List[str]]] = None

        # http_pool
        self.http_pool = urllib3.PoolManager(retries=urllib3.Retry(total=3))

    def __str__(self):
        return f"TransferProgressTracker({self.dataplane}, {self.jobs})"

    def run(self):
        """Dispatch and start the transfer jobs"""
        src_cloud_provider = self.dataplane.src_region_tag.split(":")[0]
        dst_cloud_provider = self.dataplane.dst_region_tag.split(":")[0]
        args = {
            "cmd": ",".join([job.__class__.__name__ for job in self.jobs.values()]),
            "recursive": ",".join([str(job.recursive) for job in self.jobs.values()]),
            "multipart": self.transfer_config.multipart_enabled,
            "instances_per_region": self.dataplane.max_instances,
            "src_instance_type": getattr(self.transfer_config, f"{src_cloud_provider}_instance_class"),
            "dst_instance_type": getattr(self.transfer_config, f"{dst_cloud_provider}_instance_class"),
            "src_spot_instance": getattr(self.transfer_config, f"{src_cloud_provider}_use_spot_instances"),
            "dst_spot_instance": getattr(self.transfer_config, f"{dst_cloud_provider}_use_spot_instances"),
        }
        session_start_timestamp_ms = int(time.time() * 1000)
        try:
            # pre-dispatch chunks to begin pre-buffering chunks
            cr_streams = {
                job_uuid: job.dispatch(self.dataplane, transfer_config=self.transfer_config) for job_uuid, job in self.jobs.items()
            }
            for job_uuid, job in self.jobs.items():
                logger.fs.debug(f"[TransferProgressTracker] Dispatching job {job.uuid}")
                self.job_chunk_requests[job_uuid] = {}
                self.job_pending_chunk_ids[job_uuid] = set()
                self.job_complete_chunk_ids[job_uuid] = set()
                for cr in cr_streams[job_uuid]:
                    chunks_dispatched = [cr.chunk]
                    self.job_chunk_requests[job_uuid][cr.chunk.chunk_id] = cr
                    self.job_pending_chunk_ids[job_uuid].add(cr.chunk.chunk_id)
                    self.hooks.on_chunk_dispatched(chunks_dispatched)
                logger.fs.debug(
                    f"[TransferProgressTracker] Job {job.uuid} dispatched with {len(self.job_chunk_requests[job_uuid])} chunk requests"
                )
        except Exception as e:
            UsageClient.log_exception(
                "dispatch job", e, args, self.dataplane.src_region_tag, self.dataplane.dst_region_tag, session_start_timestamp_ms
            )
            raise e

        self.hooks.on_dispatch_end()

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
            "throughput_gbits": self.query_bytes_dispatched() / (end_time - start_time) / GB * 8,
        }
        self.hooks.on_transfer_end(transfer_stats)
        UsageClient.log_transfer(
            transfer_stats, args, self.dataplane.src_region_tag, self.dataplane.dst_region_tag, session_start_timestamp_ms
        )

    @imports.inject("pandas")
    def monitor_transfer(pd, self):
        """Monitor the tranfer by copying remote gateway logs and show transfer stats by hooks"""
        # todo implement transfer monitoring to update job_complete_chunk_ids and job_pending_chunk_ids while the transfer is in progress
        sinks = self.dataplane.topology.sink_instances()
        sink_regions = set([sink.region for sink in sinks])
        while any([len(self.job_pending_chunk_ids[job_uuid]) > 0 for job_uuid in self.job_pending_chunk_ids]):
            # refresh shutdown status by running noop
            do_parallel(lambda i: i.run_command("echo 1"), self.dataplane.bound_nodes.values(), n=-1)

            # check for errors and exit if there are any (while setting debug flags)
            errors = self.dataplane.check_error_logs()
            if any(errors.values()):
                logger.warning("Copying gateway logs...")
                do_parallel(self.copy_log, self.dataplane.bound_nodes.values(), n=-1)
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
                new_chunk_ids = (
                    self.job_complete_chunk_ids[job_uuid].union(job_complete_chunk_ids).difference(self.job_complete_chunk_ids[job_uuid])
                )
                completed_chunks = []
                for id in new_chunk_ids:
                    completed_chunks.append(self.job_chunk_requests[job_uuid][id].chunk)
                self.hooks.on_chunk_completed(completed_chunks)
                self.job_complete_chunk_ids[job_uuid] = self.job_complete_chunk_ids[job_uuid].union(job_complete_chunk_ids)
                self.job_pending_chunk_ids[job_uuid] = self.job_pending_chunk_ids[job_uuid].difference(job_complete_chunk_ids)

            # sleep
            time.sleep(0.05)

    @property
    @functools.lru_cache(maxsize=1)
    def _chunk_to_job_map(self):
        return {chunk_id: job_uuid for job_uuid, cr_dict in self.job_chunk_requests.items() for chunk_id in cr_dict.keys()}

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
        """Return if the transfer is complete"""
        return all([len(self.job_pending_chunk_ids[job_uuid]) == 0 for job_uuid in self.jobs.keys()])

    def query_bytes_remaining(self):
        """Query the total number of bytes remaining in all the transfer jobs"""
        if len(self.job_chunk_requests) == 0:
            return None
        bytes_remaining_per_job = {}
        for job_uuid in self.job_pending_chunk_ids.keys():
            bytes_remaining_per_job[job_uuid] = sum(
                [
                    cr.chunk.chunk_length_bytes
                    for cr in self.job_chunk_requests[job_uuid].values()
                    if cr.chunk.chunk_id in self.job_pending_chunk_ids[job_uuid]
                ]
            )
        logger.fs.debug(f"[TransferProgressTracker] Bytes remaining per job: {bytes_remaining_per_job}")
        return sum(bytes_remaining_per_job.values())

    def query_bytes_dispatched(self):
        """Query the total number of bytes dispatched to chunks ready for transfer"""
        if len(self.job_chunk_requests) == 0:
            return 0
        bytes_total_per_job = {}
        for job_uuid in self.job_complete_chunk_ids.keys():
            bytes_total_per_job[job_uuid] = sum(
                [
                    cr.chunk.chunk_length_bytes
                    for cr in self.job_chunk_requests[job_uuid].values()
                    if cr.chunk.chunk_id in self.job_complete_chunk_ids[job_uuid]
                ]
            )
        return sum(bytes_total_per_job.values())
