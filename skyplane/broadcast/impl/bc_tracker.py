import time
from pprint import pprint

import urllib3
import pandas as pd
from typing import TYPE_CHECKING, Dict, List, Optional, Set
import functools
from skyplane import exceptions
from skyplane.api.config import TransferConfig
from skyplane.chunk import ChunkRequest, ChunkState
from skyplane.utils import logger, imports
from skyplane.utils.fn import do_parallel
from skyplane.api.usage import UsageClient
from skyplane.api.tracker import TransferProgressTracker, TransferHook, EmptyTransferHook
from concurrent.futures import ThreadPoolExecutor, as_completed
from skyplane.utils.definitions import GB, tmp_log_dir
from datetime import datetime

if TYPE_CHECKING:
    from skyplane.broadcast.impl.bc_transfer_job import BCTransferJob


class BCTransferProgressTracker(TransferProgressTracker):
    def __init__(self, dataplane, jobs: List["BCTransferJob"], transfer_config: TransferConfig, hooks: TransferHook):
        super().__init__(dataplane, jobs, transfer_config, hooks)

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
        self.job_chunk_requests: Dict[str, Dict[str, ChunkRequest]] = {}

        # each job can have multiple destination
        self.dst_regions = set([sink.region for sink in self.dataplane.topology.sink_instances()])
        self.dst_job_pending_chunk_ids: Dict[str, Dict[str, Set[str]]] = {k: {} for k in self.dst_regions}
        self.dst_job_complete_chunk_ids: Dict[str, Dict[str, Set[str]]] = {k: {} for k in self.dst_regions}

        self.errors: Optional[Dict[str, List[str]]] = None

        # http_pool
        self.http_pool = urllib3.PoolManager(retries=urllib3.Retry(total=3))

        # store the chunk status log
        self.transfer_dir = tmp_log_dir / "transfer_logs" / datetime.now().strftime("%Y%m%d_%H%M%S")
        self.transfer_dir.mkdir(exist_ok=True, parents=True)

    def __str__(self):
        return f"TransferProgressTracker({self.dataplane}, {self.jobs})"

    def calculate_size(self, dst_region):
        if len(self.job_chunk_requests) == 0:
            return 0
        bytes_total_per_job = {}
        for job_uuid in self.dst_job_complete_chunk_ids[dst_region].keys():
            bytes_total_per_job[job_uuid] = sum(
                [
                    cr.chunk.chunk_length_bytes
                    for cr in self.job_chunk_requests[job_uuid].values()
                    if cr.chunk.chunk_id in self.dst_job_complete_chunk_ids[dst_region][job_uuid]
                ]
            )
        return sum(bytes_total_per_job.values()) / GB

    def run(self):
        src_cloud_provider = self.dataplane.src_region_tag.split(":")[0]
        dst_cloud_providers = [dst_region_tag.split(":")[0] for dst_region_tag in self.dataplane.dst_region_tags]

        args = {
            "cmd": ",".join(self.type_list),
            "recursive": ",".join(self.recursive_list),
            "multipart": self.transfer_config.multipart_enabled,
            "instances_per_region": self.dataplane.max_instances,
            "src_instance_type": getattr(self.transfer_config, f"{src_cloud_provider}_instance_class"),
            "dst_instance_type": [
                getattr(self.transfer_config, f"{dst_cloud_provider}_instance_class") for dst_cloud_provider in dst_cloud_providers
            ],
            "src_spot_instance": getattr(self.transfer_config, f"{src_cloud_provider}_use_spot_instances"),
            "dst_spot_instance": [
                getattr(self.transfer_config, f"{dst_cloud_provider}_use_spot_instances") for dst_cloud_provider in dst_cloud_providers
            ],
        }
        session_start_timestamp_ms = int(time.time() * 1000)
        try:
            # pre-dispatch chunks to begin pre-buffering chunks
            cr_streams = {
                job_uuid: job.broadcast_dispatch(self.dataplane, transfer_config=self.transfer_config)
                for job_uuid, job in self.jobs.items()
            }
            for job_uuid, job in self.jobs.items():
                logger.fs.debug(f"[TransferProgressTracker] Dispatching job {job.uuid}")

                self.job_chunk_requests[job_uuid] = {}

                job_pending_chunk_ids = set()
                for cr in cr_streams[job_uuid]:
                    chunks_dispatched = [cr.chunk]
                    self.job_chunk_requests[job_uuid][cr.chunk.chunk_id] = cr
                    job_pending_chunk_ids.add(cr.chunk.chunk_id)
                    self.hooks.on_chunk_dispatched(chunks_dispatched)

                for dst_region in self.dst_regions:
                    self.dst_job_complete_chunk_ids[dst_region][job_uuid] = set()
                    self.dst_job_pending_chunk_ids[dst_region][job_uuid] = set()

                    self.dst_job_pending_chunk_ids[dst_region][job_uuid].update([i for i in job_pending_chunk_ids])

                logger.fs.debug(f"[TransferProgressTracker] Job {job.uuid} dispatched with {len(self.job_chunk_requests[job_uuid])}")

        except Exception as e:
            UsageClient.log_exception("dispatch job", e, args, self.dataplane.src_region_tag, ":", session_start_timestamp_ms)
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
                    self.dataplane.src_region_tag,
                    dst_region,
                    session_start_timestamp_ms,
                )
                raise err
            except Exception as e:
                UsageClient.log_exception(
                    "monitor transfer", e, args, self.dataplane.src_region_tag, dst_region, session_start_timestamp_ms
                )
                raise e
            end_time = time.time()

            runtime_s = end_time - start_time
            # transfer successfully completed
            transfer_stats = {
                "dst_region": dst_region,
                "total_runtime_s": round(runtime_s, 4),
                "throughput_gbits": round(self.calculate_size(dst_region) * 8 / runtime_s, 4),
            }
            print("Individual transfer statistics")
            pprint(transfer_stats)

            try:
                for job in self.jobs.values():
                    logger.fs.debug(f"[TransferProgressTracker] Finalizing job {job.uuid}")
                    job.bc_finalize(dst_region)
            except Exception as e:
                UsageClient.log_exception("finalize job", e, args, self.dataplane.src_region_tag, dst_region, session_start_timestamp_ms)
                raise e

            try:
                for job in self.jobs.values():
                    logger.fs.debug(f"[TransferProgressTracker] Verifying job {job.uuid}")
                    job.bc_verify(dst_region)
            except Exception as e:
                UsageClient.log_exception("verify job", e, args, self.dataplane.src_region_tag, dst_region, session_start_timestamp_ms)
                raise e

            UsageClient.log_transfer(transfer_stats, args, self.dataplane.src_region_tag, dst_region, session_start_timestamp_ms)
            return transfer_stats

        # Record only the transfer time per destination

        results = []
        with ThreadPoolExecutor(max_workers=100) as executor:
            e2e_start_time = time.time()
            future_list = [executor.submit(monitor_single_dst_helper, dst) for dst in self.dst_regions]
            for future in as_completed(future_list):
                results.append(future.result())
            e2e_end_time = time.time()
        print(f"End to end time: {round(e2e_end_time - e2e_start_time, 4)}s\n")
        print(f"Transfer result:")
        for i in results:
            pprint(i)
            print()

        size_of_transfer = self.calculate_size(list(self.dst_regions)[0])
        cost_per_gb = self.dataplane.topology.cost_per_gb
        print(f"Cost per gb: ${round(cost_per_gb, 4)}")
        print(f"GB transferred: ${round(size_of_transfer, 8)}GB")
        print(f"Total cost: ${round(cost_per_gb * size_of_transfer, 8)}\n")

        # write chunk status
        print(f"Writing chunk profiles to {self.transfer_dir}/chunk_status_df.csv")
        chunk_status_df = pd.DataFrame(self._query_chunk_status())
        (self.transfer_dir / "chunk_status_df.csv").write_text(chunk_status_df.to_csv(index=False))

    def copy_log(self, instance):
        print("COPY DATA TO", self.transfer_dir  + f"gateway_{instance.uuid()}.stdout")
        instance.run_command("sudo docker logs -t skyplane_gateway 2> /tmp/gateway.stderr > /tmp/gateway.stdout")
        pprint(f"Copying gateway std out files to gateway_{instance.uuid()}.stdout")
        instance.download_file("/tmp/gateway.stdout", self.transfer_dir / f"gateway_{instance.uuid()}.stdout")
        pprint(f"Copying gateway std err files to gateway_{instance.uuid()}.stderr")
        instance.download_file("/tmp/gateway.stderr", self.transfer_dir / f"gateway_{instance.uuid()}.stderr")

    @property
    @functools.lru_cache(maxsize=None)
    def _chunk_to_job_map(self):
        return {chunk_id: job_uuid for job_uuid, cr_dict in self.job_chunk_requests.items() for chunk_id in cr_dict.keys()}

    @imports.inject("pandas")
    def monitor_transfer(pd, self, dst_region):
        # todo implement transfer monitoring to update job_complete_chunk_ids and job_pending_chunk_ids while the transfer is in progress
        sinks = {n for n in self.dataplane.topology.sink_instances() if n.region == dst_region}
        sink_regions = {dst_region}

        assert len(sink_regions) == 1  # BC: only monitor one sink region in this call

        # any of the jobs of this region is not complete
        while any(
            [len(self.dst_job_pending_chunk_ids[dst_region][job_uuid]) > 0 for job_uuid in self.dst_job_pending_chunk_ids[dst_region]]
        ):
            # refresh shutdown status by running noop
            do_parallel(lambda i: i.run_command("echo 1"), self.dataplane.bound_nodes.values(), n=-1)

            # check for errors and exit if there are any (while setting debug flags)
            errors = self.dataplane.check_error_logs()
            # print("ERRORS", errors)
            
            if any(errors.values()):
                print("copying gateway logs")
                logger.warning("Copying gateway logs")
                do_parallel(self.copy_log, self.dataplane.bound_nodes.values(), n=-1)
                self.errors = errors
                pprint(errors)
                raise exceptions.SkyplaneGatewayException("Transfer failed with errors", errors)

            log_df = pd.DataFrame(self._query_chunk_status())
            if log_df.empty:
                logger.warning("No chunk status log entries yet")
                time.sleep(0.05)
                continue

            is_complete_rec = (
                lambda row: row["state"] == ChunkState.complete
                and row["instance"] in [s.instance for s in sinks]
                and row["region"] in [s.region for s in sinks]
            )
            sink_status_df = log_df[log_df.apply(is_complete_rec, axis=1)]
            completed_status = sink_status_df.groupby("chunk_id").apply(lambda x: set(x["region"].unique()) == set(sink_regions))
            completed_chunk_ids = completed_status[completed_status].index

            # update job_complete_chunk_ids and job_pending_chunk_ids
            for job_uuid, job in self.jobs.items():
                job_complete_chunk_ids = set(chunk_id for chunk_id in completed_chunk_ids if self._chunk_to_job_map[chunk_id] == job_uuid)

                # print("Completed chunk ids: ", job_complete_chunk_ids)

                # TODO: this is wrong, should wait until these chunks finish
                new_chunk_ids = (
                    self.dst_job_complete_chunk_ids[dst_region][job_uuid]
                    .union(job_complete_chunk_ids)
                    .difference(self.dst_job_complete_chunk_ids[dst_region][job_uuid])
                )
                completed_chunks = []
                for id in new_chunk_ids:
                    completed_chunks.append(self.job_chunk_requests[job_uuid][id].chunk)
                self.hooks.on_chunk_completed(completed_chunks)

                self.dst_job_complete_chunk_ids[dst_region][job_uuid] = self.dst_job_complete_chunk_ids[dst_region][job_uuid].union(
                    job_complete_chunk_ids
                )
                self.dst_job_pending_chunk_ids[dst_region][job_uuid] = self.dst_job_pending_chunk_ids[dst_region][job_uuid].difference(
                    job_complete_chunk_ids
                )
                # print(f"Complete chunk id for {dst_region} and {job_uuid}: ", self.dst_job_complete_chunk_ids[dst_region][job_uuid])
                # print(f"Pending chunk id for {dst_region} and {job_uuid}: ", self.dst_job_pending_chunk_ids[dst_region][job_uuid])

            # TODO: FIX THIS, can't call it from the outside script as it gets stuck
            try:
                bytes_remaining, bytes_remaining_dict = self.query_bytes_remaining()
                print(f"MAX: {( bytes_remaining / (2 ** 30)):.5f}GB left")
                print(f"Remaining bytes per destination (GB): ")

                for key, value in bytes_remaining_dict.items():
                    bytes_remaining_dict[key] = [round(v / (2 ** 30), 5) for v in value]
                pprint(bytes_remaining_dict)
            except Exception as e:
                print(e)
                print("copying gateway logs")
                logger.warning("Copying gateway logs")
                do_parallel(self.copy_log, self.dataplane.bound_nodes.values(), n=-1)
                self.errors = errors
                pprint(errors)
                raise exceptions.SkyplaneGatewayException("Transfer failed with errors", errors)

            # sleep
            time.sleep(0.05)

    @property
    def is_complete(self):
        return all(
            [
                len(self.dst_job_pending_chunk_ids[dst_region][job_uuid]) == 0
                for dst_region in self.dst_regions
                for job_uuid in self.jobs.keys()
            ]
        )

    def query_bytes_remaining(self):
        if len(self.job_chunk_requests) == 0:
            return None

        bytes_remaining_per_job = {}
        for job_uuid in self.jobs.keys():
            bytes_remaining_per_job[job_uuid] = []

        dst_order = []
        for dst_region in self.dst_regions:
            dst_order.append(dst_region)
            for job_uuid in self.dst_job_pending_chunk_ids[dst_region].keys():
                # job_uuid --> List[dst1_remaining_bytes, dst2_remaining_bytes, ...]
                li_of_bytes = [
                            cr.chunk.chunk_length_bytes
                            for cr in self.job_chunk_requests[job_uuid].values()
                            if cr.chunk.chunk_id in self.dst_job_pending_chunk_ids[dst_region][job_uuid]
                        ]
                bytes_remaining_per_job[job_uuid].append(sum(li_of_bytes))
        logger.fs.debug(f"[TransferProgressTracker] Bytes remaining per job: {bytes_remaining_per_job}")
        # return the max remaining byte among dsts for each job

        # for each dst, there can be a list of bytes remaining 
        bytes_remaining_per_dst = {}
        for i in range(len(dst_order)):
            dst = dst_order[i]
            bytes_remaining_per_dst[dst] = []
            for job_uuid in self.dst_job_complete_chunk_ids[dst].keys():
                bytes_remaining_per_dst[dst].append(bytes_remaining_per_job[job_uuid][i])

