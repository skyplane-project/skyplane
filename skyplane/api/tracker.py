import functools
from pprint import pprint
import json
import time
from abc import ABC
from datetime import datetime
from threading import Thread

import urllib3
from typing import TYPE_CHECKING, Dict, List, Optional, Set

from concurrent.futures import ThreadPoolExecutor, as_completed

from skyplane import exceptions
from skyplane.api.config import TransferConfig
from skyplane.chunk import ChunkState, Chunk
from skyplane.utils import logger, imports
from skyplane.utils.fn import do_parallel
from skyplane.api.usage import UsageClient
from skyplane.utils.definitions import GB

from skyplane.cli.impl.common import print_stats_completed

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

    def on_chunk_completed(self, chunks: List[Chunk], region_tag: Optional[str] = None):
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

    def on_chunk_completed(self, chunks: List[Chunk], region_tag: Optional[str] = None):
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
        self.job_chunk_requests: Dict[str, Dict[str, Chunk]] = {}
        self.job_pending_chunk_ids: Dict[str, Dict[str, Set[str]]] = {}
        self.job_complete_chunk_ids: Dict[str, Dict[str, Set[str]]] = {}
        self.errors: Optional[Dict[str, List[str]]] = None

        # http_pool
        self.http_pool = urllib3.PoolManager(retries=urllib3.Retry(total=9))

    def __str__(self):
        return f"TransferProgressTracker({self.dataplane}, {self.jobs})"

    def run(self):
        """Dispatch and start the transfer jobs"""
        src_cloud_provider = self.dataplane.topology.src_region_tag.split(":")[0]
        dst_cloud_provider = self.dataplane.topology.dest_region_tags[0].split(":")[0]
        args = {
            "cmd": ",".join([job.__class__.__name__ for job in self.jobs.values()]),
            "recursive": ",".join([str(job.recursive) for job in self.jobs.values()]),
            "multipart": self.transfer_config.multipart_enabled,
            # "instances_per_region": 1,  # TODO: read this from config file
            # "src_instance_type": getattr(self.transfer_config, f"{src_cloud_provider}_instance_class"),
            # "dst_instance_type": #getattr(self.transfer_config, f"{dst_cloud_provider}_instance_class"),
            # "src_spot_instance": getattr(self.transfer_config, f"{src_cloud_provider}_use_spot_instances"),
            # "dst_spot_instance": getattr(self.transfer_config, f"{dst_cloud_provider}_use_spot_instances"),
        }
        # TODO: eventually jobs should be able to be concurrently dispatched and executed
        # however this will require being able to handle conflicting multipart uploads ids

        # initialize everything first
        for job_uuid, job in self.jobs.items():
            self.job_chunk_requests[job_uuid] = {}
            self.job_pending_chunk_ids[job_uuid] = {region: set() for region in self.dataplane.topology.dest_region_tags}
            self.job_complete_chunk_ids[job_uuid] = {region: set() for region in self.dataplane.topology.dest_region_tags}

        session_start_timestamp_ms = int(time.time() * 1000)
        for job_uuid, job in self.jobs.items():
            # pre-dispatch chunks to begin pre-buffering chunks
            try:
                chunk_stream = job.dispatch(self.dataplane, transfer_config=self.transfer_config)
                logger.fs.debug(f"[TransferProgressTracker] Dispatching job {job.uuid}")
                for chunk in chunk_stream:
                    chunks_dispatched = [chunk]
                    # TODO: check chunk ID
                    self.job_chunk_requests[job_uuid][chunk.chunk_id] = chunk
                    assert job_uuid in self.job_chunk_requests and chunk.chunk_id in self.job_chunk_requests[job_uuid]
                    self.hooks.on_chunk_dispatched(chunks_dispatched)
                    for region in self.dataplane.topology.dest_region_tags:
                        self.job_pending_chunk_ids[job_uuid][region].add(chunk.chunk_id)

                logger.fs.debug(
                    f"[TransferProgressTracker] Job {job.uuid} dispatched with {len(self.job_chunk_requests[job_uuid])} chunk requests"
                )
            except Exception as e:
                UsageClient.log_exception(
                    "dispatch job",
                    e,
                    args,
                    self.dataplane.topology.src_region_tag,
                    self.dataplane.topology.dest_region_tags[0],  # TODO: support multiple destinations
                    session_start_timestamp_ms,
                )
                raise e

            self.hooks.on_dispatch_end()

            def monitor_single_dst_helper(dst_region):
                start_time = time.time()
                try:
                    self.monitor_transfer(dst_region)
                except exceptions.SkyplaneGatewayException as err:
                    reformat_err = Exception(err.pretty_print_str()[37:])
                    UsageClient.log_exception(
                        "monitor transfer",
                        reformat_err,
                        args,
                        self.dataplane.topology.src_region_tag,
                        dst_region,
                        session_start_timestamp_ms,
                    )
                    raise err
                except Exception as e:
                    UsageClient.log_exception(
                        "monitor transfer", e, args, self.dataplane.topology.src_region_tag, dst_region, session_start_timestamp_ms
                    )
                    raise e
                end_time = time.time()

                runtime_s = end_time - start_time
                # transfer successfully completed
                transfer_stats = {
                    "dst_region": dst_region,
                    "total_runtime_s": round(runtime_s, 4),
                }

            results = []
            dest_regions = self.dataplane.topology.dest_region_tags
            with ThreadPoolExecutor(max_workers=len(dest_regions)) as executor:
                e2e_start_time = time.time()
                try:
                    future_list = [executor.submit(monitor_single_dst_helper, dest) for dest in dest_regions]
                    for future in as_completed(future_list):
                        results.append(future.result())
                except Exception as e:
                    raise e
            e2e_end_time = time.time()
            transfer_stats = {
                "total_runtime_s": e2e_end_time - e2e_start_time,
                "throughput_gbits": self.query_bytes_dispatched() / (e2e_end_time - e2e_start_time) / GB * 8,
            }
            self.hooks.on_transfer_end()

            start_time = int(time.time())
            try:
                for job in self.jobs.values():
                    logger.fs.debug(f"[TransferProgressTracker] Finalizing job {job.uuid}")
                    job.finalize()
            except Exception as e:
                UsageClient.log_exception(
                    "finalize job",
                    e,
                    args,
                    self.dataplane.topology.src_region_tag,
                    self.dataplane.topology.dest_region_tags[0],
                    session_start_timestamp_ms,
                )
                raise e
            end_time = int(time.time())

            # verify transfer
            try:
                for job in self.jobs.values():
                    logger.fs.debug(f"[TransferProgressTracker] Verifying job {job.uuid}")
                    job.verify()
            except Exception as e:
                UsageClient.log_exception(
                    "verify job",
                    e,
                    args,
                    self.dataplane.topology.src_region_tag,
                    self.dataplane.topology.dest_region_tags[0],
                    session_start_timestamp_ms,
                )
                raise e

            # transfer successfully completed
            UsageClient.log_transfer(
                transfer_stats,
                args,
                self.dataplane.topology.src_region_tag,
                self.dataplane.topology.dest_region_tags,
                session_start_timestamp_ms,
            )
            print_stats_completed(total_runtime_s=transfer_stats["total_runtime_s"], throughput_gbits=transfer_stats["throughput_gbits"])

    @imports.inject("pandas")
    def monitor_transfer(pd, self, region_tag):
        """Monitor the tranfer by copying remote gateway logs and show transfer stats by hooks"""
        # todo implement transfer monitoring to update job_complete_chunk_ids and job_pending_chunk_ids while the transfer is in progress

        # regions that are sinks for specific region tag
        # TODO: should eventualy map bucket to list of instances
        sinks = [n for nodes in self.dataplane.topology.sink_instances(region_tag).values() for n in nodes]

        while any([len(self.job_pending_chunk_ids[job_uuid][region_tag]) > 0 for job_uuid in self.job_pending_chunk_ids]):
            # refresh shutdown status by running noop
            do_parallel(lambda i: i.run_command("echo 1"), self.dataplane.bound_nodes.values(), n=8)

            # check for errors and exit if there are any (while setting debug flags)
            errors = self.dataplane.check_error_logs()
            if any(errors.values()):
                logger.warning("Copying gateway logs...")
                self.dataplane.copy_gateway_logs()
                self.errors = errors
                raise exceptions.SkyplaneGatewayException("Transfer failed with errors", errors)

            log_df = pd.DataFrame(self._query_chunk_status())
            if log_df.empty:
                logger.warning("No chunk status log entries yet")
                time.sleep(0.05)
                continue

            # TODO: have visualization for completition across all destinations
            is_complete_rec = (
                lambda row: row["state"] == ChunkState.complete
                and row["instance"] in [s.gateway_id for s in sinks]
                # and row["region_tag"] in region_sinks
            )
            sink_status_df = log_df[log_df.apply(is_complete_rec, axis=1)]
            completed_chunk_ids = list(set(sink_status_df.chunk_id.unique()))

            # update job_complete_chunk_ids and job_pending_chunk_ids
            # TODO: do chunk-tracking per-destination
            for job_uuid, job in self.jobs.items():
                try:
                    job_complete_chunk_ids = set(
                        chunk_id for chunk_id in completed_chunk_ids if self._chunk_to_job_map[chunk_id] == job_uuid
                    )
                except Exception as e:
                    raise e
                new_chunk_ids = (
                    self.job_complete_chunk_ids[job_uuid][region_tag]
                    .union(job_complete_chunk_ids)
                    .difference(self.job_complete_chunk_ids[job_uuid][region_tag])
                )
                completed_chunks = []
                for id in new_chunk_ids:
                    assert (
                        job_uuid in self.job_chunk_requests and id in self.job_chunk_requests[job_uuid]
                    ), f"Missing chunk id {id} for job {job_uuid}: {self.job_chunk_requests}"
                for id in new_chunk_ids:
                    completed_chunks.append(self.job_chunk_requests[job_uuid][id])
                self.hooks.on_chunk_completed(completed_chunks, region_tag)
                self.job_complete_chunk_ids[job_uuid][region_tag] = self.job_complete_chunk_ids[job_uuid][region_tag].union(
                    job_complete_chunk_ids
                )
                self.job_pending_chunk_ids[job_uuid][region_tag] = self.job_pending_chunk_ids[job_uuid][region_tag].difference(
                    job_complete_chunk_ids
                )
            # sleep
            time.sleep(0.05)

    @property
    # TODO: this is a very slow function, but we can't cache it since self.job_chunk_requests changes over time
    # do not call it more often than necessary
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
                log_entry["region_tag"] = node.region_tag
                log_entry["instance"] = node.gateway_id
                log_entry["time"] = datetime.fromisoformat(log_entry["time"])
                log_entry["state"] = ChunkState.from_str(log_entry["state"])
                logs.append(log_entry)

            return logs

        rows = []
        for result in do_parallel(get_chunk_status, self.dataplane.bound_nodes.items(), n=8, return_args=False):
            rows.extend(result)
        return rows

    @property
    def is_complete(self, region_tag: str):
        """Return if the transfer is complete"""
        return all([len(self.job_pending_chunk_ids[job_uuid][region_tag]) == 0 for job_uuid in self.jobs.keys()])

    def query_bytes_remaining(self, region_tag: Optional[str] = None):
        """Query the total number of bytes remaining in all the transfer jobs"""
        if region_tag is None:
            assert len(list(self.job_pending_chunk_ids.keys())) == 1, "Must specify region_tag if there are multiple regions"
            region_tag = list(self.job_pending_chunk_ids.keys())[0]
        if len(self.job_chunk_requests) == 0:
            return None
        bytes_remaining_per_job = {}
        for job_uuid in self.job_pending_chunk_ids.keys():
            bytes_remaining_per_job[job_uuid] = sum(
                [
                    cr.chunk_length_bytes
                    for cr in self.job_chunk_requests[job_uuid].values()
                    if cr.chunk_id in self.job_pending_chunk_ids[job_uuid][region_tag]
                ]
            )
        logger.fs.debug(f"[TransferProgressTracker] Bytes remaining per job: {bytes_remaining_per_job}")
        print(f"[TransferProgressTracker] Bytes remaining per job: {bytes_remaining_per_job}")
        return sum(bytes_remaining_per_job.values())

    def query_bytes_dispatched(self):
        """Query the total number of bytes dispatched to chunks ready for transfer"""
        if len(self.job_chunk_requests) == 0:
            return 0
        bytes_total_per_job = {}
        for job_uuid in self.job_complete_chunk_ids.keys():
            bytes_total_per_job[job_uuid] = sum([cr.chunk_length_bytes for cr in self.job_chunk_requests[job_uuid].values()])
        return sum(bytes_total_per_job.values())
