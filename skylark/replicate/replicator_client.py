from datetime import datetime
import itertools
import json
from logging import log
import time
from typing import Dict, List, Optional

import requests
from loguru import logger
from tqdm import tqdm

from skylark.benchmark.utils import refresh_instance_list
from skylark.compute.aws.aws_cloud_provider import AWSCloudProvider
from skylark.compute.gcp.gcp_cloud_provider import GCPCloudProvider
from skylark.compute.server import Server, ServerState
from skylark.gateway.chunk import Chunk, ChunkRequest, ChunkRequestHop, ChunkState
from skylark.obj_store.s3_interface import S3Interface
from skylark.replicate.replication_plan import ReplicationJob, ReplicationTopology
from skylark.utils.utils import PathLike, Timer, do_parallel, wait_for


class ReplicatorClient:
    def __init__(
        self,
        topology: ReplicationTopology,
        gcp_project: str,
        gateway_docker_image: str = "ghcr.io/parasj/skylark:latest",
        aws_instance_class: Optional[str] = "m5.4xlarge",  # set to None to disable AWS
        gcp_instance_class: Optional[str] = "n2-standard-16",  # set to None to disable GCP
        gcp_use_premium_network: bool = True,
    ):
        self.topology = topology
        self.gateway_docker_image = gateway_docker_image
        self.aws_instance_class = aws_instance_class
        self.gcp_instance_class = gcp_instance_class
        self.gcp_use_premium_network = gcp_use_premium_network

        # provisioning
        self.aws = AWSCloudProvider() if aws_instance_class is not None else None
        self.gcp = GCPCloudProvider(gcp_project) if gcp_instance_class is not None else None
        self.bound_paths: Optional[List[List[Server]]] = None

        # init clouds
        jobs = []
        if self.aws is not None:
            jobs.extend([lambda: self.aws.add_ip_to_security_group(r) for r in self.aws.region_list()])
        if self.gcp is not None:
            jobs.append(lambda: self.gcp.create_ssh_key())
            jobs.append(lambda: self.gcp.configure_default_network())
            jobs.append(lambda: self.gcp.configure_default_firewall())
        with Timer(f"Cloud SSH key initialization"):
            do_parallel(lambda fn: fn(), jobs)

    def provision_gateways(
        self,
        reuse_instances=False,
        log_dir: Optional[PathLike] = None,
        authorize_ssh_pub_key: Optional[PathLike] = None,
        num_outgoing_connections=8,
    ):
        regions_to_provision = [r for path in self.topology.paths for r in path]
        aws_regions_to_provision = [r for r in regions_to_provision if r.startswith("aws:")]
        gcp_regions_to_provision = [r for r in regions_to_provision if r.startswith("gcp:")]

        assert len(aws_regions_to_provision) == 0 or self.aws is not None, "AWS not enabled"
        assert len(gcp_regions_to_provision) == 0 or self.gcp is not None, "GCP not enabled"

        # reuse existing AWS instances
        if reuse_instances:
            if self.aws is not None:
                aws_instance_filter = {
                    "tags": {"skylark": "true"},
                    "instance_type": self.aws_instance_class,
                    "state": [ServerState.PENDING, ServerState.RUNNING],
                }
                with Timer("Refresh AWS instances"):
                    current_aws_instances = refresh_instance_list(
                        self.aws, set([r.split(":")[1] for r in aws_regions_to_provision]), aws_instance_filter
                    )
                for r, ilist in current_aws_instances.items():
                    for i in ilist:
                        if f"aws:{r}" in aws_regions_to_provision:
                            aws_regions_to_provision.remove(f"aws:{r}")
                            logger.debug(f"Found existing instance {i}")
            else:
                current_aws_instances = {}

            if self.gcp is not None:
                gcp_instance_filter = {
                    "tags": {"skylark": "true"},
                    "instance_type": self.gcp_instance_class,
                    "state": [ServerState.PENDING, ServerState.RUNNING],
                }
                current_gcp_instances = refresh_instance_list(
                    self.gcp, set([r.split(":")[1] for r in gcp_regions_to_provision]), gcp_instance_filter
                )
                for r, ilist in current_gcp_instances.items():
                    for i in ilist:
                        if f"gcp:{r}" in gcp_regions_to_provision:
                            gcp_regions_to_provision.remove(f"gcp:{r}")
            else:
                current_gcp_instances = {}

        with Timer("Provision gateways"):
            # provision instances
            def provision_gateway_instance(region: str) -> Server:
                provider, subregion = region.split(":")
                if provider == "aws":
                    assert self.aws is not None
                    server = self.aws.provision_instance(subregion, self.aws_instance_class)
                elif provider == "gcp":
                    assert self.gcp is not None
                    # todo specify network tier in ReplicationTopology
                    server = self.gcp.provision_instance(subregion, self.gcp_instance_class, premium_network=self.gcp_use_premium_network)
                else:
                    raise NotImplementedError(f"Unknown provider {provider}")
                return server

            results = do_parallel(
                provision_gateway_instance,
                list(aws_regions_to_provision + gcp_regions_to_provision),
                progress_bar=True,
                desc="Provisioning gateways",
            )
            instances_by_region = {
                r: [instance for instance_region, instance in results if instance_region == r] for r in set(regions_to_provision)
            }

        # add existing instances
        if reuse_instances:
            for r, ilist in current_aws_instances.items():
                if f"aws:{r}" not in instances_by_region:
                    instances_by_region[f"aws:{r}"] = []
                instances_by_region[f"aws:{r}"].extend(ilist)
            for r, ilist in current_gcp_instances.items():
                if f"gcp:{r}" not in instances_by_region:
                    instances_by_region[f"gcp:{r}"] = []
                instances_by_region[f"gcp:{r}"].extend(ilist)

        # setup instances
        def setup(server: Server):
            if log_dir:
                server.init_log_files(log_dir)
            if authorize_ssh_pub_key:
                server.copy_public_key(authorize_ssh_pub_key)

        do_parallel(setup, itertools.chain(*instances_by_region.values()), n=-1)

        # bind instances to paths
        bound_paths = []
        for path in self.topology.paths:
            bound_paths.append([instances_by_region[r].pop() for r in path])
        self.bound_paths = bound_paths

        with Timer("Configure gateways"):
            # start gateways
            do_parallel(
                lambda s: s.start_gateway(
                    gateway_docker_image=self.gateway_docker_image, num_outgoing_connections=num_outgoing_connections
                ),
                list(itertools.chain(*self.bound_paths)),
                progress_bar=True,
                desc="Starting up gateways",
            )

    def deprovision_gateways(self):
        def deprovision_gateway_instance(server: Server):
            logger.warning(f"Deprovisioning gateway {server.instance_name()}")
            server.terminate_instance()

        instances = [instance for path in self.bound_paths for instance in path]
        do_parallel(deprovision_gateway_instance, instances, n=len(instances))

    def run_replication_plan(self, job: ReplicationJob):
        # assert all(len(path) == 2 for path in self.bound_paths), f"Only two-hop replication is supported"

        # todo support GCP
        assert job.source_region.split(":")[0] == "aws", f"Only AWS is supported for now, got {job.source_region}"
        assert job.dest_region.split(":")[0] == "aws", f"Only AWS is supported for now, got {job.dest_region}"

        # make list of chunks
        chunks = []
        for idx, obj in enumerate(job.objs):
            file_size_bytes = 128 * 1000 * 1000
            chunks.append(
                Chunk(
                    key=obj,
                    chunk_id=idx,
                    file_offset_bytes=0,
                    chunk_length_bytes=file_size_bytes,
                )
            )

        # partition chunks into roughly equal-sized batches (by bytes)
        n_src_instances = len(self.bound_paths)
        chunk_lens = [c.chunk_length_bytes for c in chunks]
        approx_bytes_per_connection = sum(chunk_lens) / n_src_instances
        batch_bytes = 0
        chunk_batches = []
        current_batch = []
        for chunk in chunks:
            current_batch.append(chunk)
            batch_bytes += chunk.chunk_length_bytes
            if batch_bytes >= approx_bytes_per_connection and len(chunk_batches) < n_src_instances:
                chunk_batches.append(current_batch)
                batch_bytes = 0
                current_batch = []
        if current_batch:  # add remaining chunks to the smallest batch by total bytes
            smallest_batch = min(chunk_batches, key=lambda b: sum([c.chunk_length_bytes for c in b]))
            smallest_batch.extend(current_batch)
        assert len(chunk_batches) == n_src_instances, f"{len(chunk_batches)} batches, expected {n_src_instances}"

        # make list of ChunkRequests
        chunk_requests_sharded: Dict[Server, List[ChunkRequest]] = {}
        with tqdm(total=len(chunks), desc="Building chunk requests") as pbar:
            for batch, path in zip(chunk_batches, self.bound_paths):
                chunk_requests_sharded[path[0]] = []
                for chunk in batch:
                    # make ChunkRequestHop list
                    cr_path = []
                    for hop_idx, hop_instance in enumerate(path):
                        if hop_idx == 0:  # source gateway
                            location = f"random_{job.random_chunk_size_mb}MB"
                        elif hop_idx == len(path) - 1:  # destination gateway
                            location = "save_local"
                        else:  # intermediate gateway
                            location = "relay"
                        cr_path.append(
                            ChunkRequestHop(
                                hop_cloud_region=hop_instance.region_tag,
                                hop_ip_address=hop_instance.public_ip(),
                                chunk_location_type=location,
                            )
                        )
                    chunk_requests_sharded[path[0]].append(ChunkRequest(chunk, cr_path))
                    pbar.update()

        # send chunk requests to source gateway
        for instance, chunk_requests in chunk_requests_sharded.items():
            logger.debug(f"Sending {len(chunk_requests)} chunk requests to {instance.public_ip()}")
            reply = requests.post(f"http://{instance.public_ip()}:8080/api/v1/chunk_requests", json=[cr.as_dict() for cr in chunk_requests])
            if reply.status_code != 200:
                raise Exception(f"Failed to send chunk requests to gateway instance {instance.instance_name()}: {reply.text}")

        return [cr for crlist in chunk_requests_sharded.values() for cr in crlist]

    def get_chunk_status(self, crs: List[ChunkRequest]):
        chunk_logs = {}

        # add entry for each chunk
        for cr in crs:
            chunk_logs[cr.chunk.chunk_id] = []

        # load status
        for path_idx, path in enumerate(self.bound_paths):
            for hop_idx, hop_instance in enumerate(path):
                reply = requests.get(f"http://{hop_instance.public_ip()}:8080/api/v1/chunk_status_log")
                if reply.status_code != 200:
                    raise Exception(f"Failed to get chunk status from gateway instance {hop_instance.instance_name()}: {reply.text}")
                for log_entry in reply.json()["chunk_status_log"]:
                    log_entry["hop_cloud_region"] = hop_instance.region_tag
                    log_entry["path_idx"] = path_idx
                    log_entry["hop_idx"] = hop_idx
                    log_entry["time"] = datetime.fromisoformat(log_entry["time"])
                    log_entry["state"] = ChunkState.from_str(log_entry["state"])
                    chunk_logs[log_entry["chunk_id"]].append(log_entry)

        return chunk_logs

    def monitor_transfer(self, crs: List[ChunkRequest]):
        total_bytes = sum([cr.chunk.chunk_length_bytes for cr in crs])
        with tqdm(total=total_bytes * 8, desc="Replication", unit="bit", unit_scale=True, unit_divisor=1000) as pbar:
            while True:
                # get chunk status
                # todo fix this disgusting code
                chunk_last_position = {}
                chunk_last_status = {}
                for chunk_id, chunk_log in self.get_chunk_status(crs).items():
                    last_entry_idx = -1
                    for entry_idx, entry in enumerate(chunk_log):
                        if entry["state"] != ChunkState.registered:
                            if last_entry_idx == -1 or entry["hop_idx"] >= chunk_log[last_entry_idx]["hop_idx"]:
                                if last_entry_idx == -1 or entry["path_idx"] >= chunk_log[last_entry_idx]["path_idx"]:
                                    if last_entry_idx == -1 or entry["time"] > chunk_log[last_entry_idx]["time"]:
                                        last_entry_idx = entry_idx
                                        chunk_last_position[chunk_id] = (entry["path_idx"], entry["hop_idx"])
                                        chunk_last_status[chunk_id] = entry["state"]

                # print info on chunk 0
                # tqdm.write(f"Chunk 0: last position: {chunk_last_position[0]}, last status: {chunk_last_status[0]}")

                # count completed chunks
                completed_chunks = 0
                completed_bytes = 0
                for chunk_id in chunk_last_status.keys():
                    last_path_idx, last_hop_idx = chunk_last_position[chunk_id]
                    last_status = chunk_last_status[chunk_id]
                    if (
                        last_status == ChunkState.downloaded
                        and last_path_idx == len(self.bound_paths) - 1
                        and last_hop_idx == len(self.bound_paths[last_path_idx]) - 1
                    ):
                        completed_chunks += 1
                        completed_bytes += crs[chunk_id].chunk.chunk_length_bytes

                # update progress bar
                pbar.update(completed_bytes * 8 - pbar.n)

                if completed_chunks == len(crs):
                    break
                else:
                    tqdm.write(f"remaining chunks: {len(crs) - completed_chunks}")

                time.sleep(0.5)

    def write_chrome_trace(self, crs: List[ChunkRequest], out_file: str):
        trace = {"traceEvents": []}

        chunk_logs = self.get_chunk_status(crs)
        min_time = min([min(entry["time"] for entry in chunk_log) for chunk_log in chunk_logs.values()])

        for chunk_id, chunk_log in chunk_logs.items():
            for entry_in in chunk_log:
                # entry example
                # {"name": "Asub", "cat": "PERF", "ph": "B", "pid": 22630, "tid": 22630, "ts": 829}
                # name: The name of the event, as displayed in Trace Viewer
                # cat: The event categories. This is a comma separated list of categories for the event. The categories can be used to hide events in the Trace Viewer UI.
                # ph: The event type. This is a single character which changes depending on the type of event being output. The valid values are listed in the table below. We will discuss each phase type below. B = Begin, E = End
                # ts: The tracing clock timestamp of the event. The timestamps are provided at microsecond granularity.
                # tts: Optional. The thread clock timestamp of the event. The timestamps are provided at microsecond granularity.
                # pid: The process ID for the process that output this event.
                # tid: The thread ID for the thread that output this event.
                # args: Any arguments provided for the event. Some of the event types have required argument fields, otherwise, you can put any information you wish in here. The arguments are displayed in Trace Viewer when you view an event in the analysis section.
                state = entry_in["state"]
                entry = {
                    "pid": f"({entry_in['path_idx']}, {entry_in['hop_idx']})",
                    "ts": (entry_in["time"] - min_time).total_seconds() * 1000000,
                }

                if state == ChunkState.download_in_progress:
                    entry["tid"] = "DL"
                    entry["name"] = f"Downloading chunk {chunk_id}"
                    entry["ph"] = "B"
                    entry["cat"] = "DOWNLOAD"
                elif state == ChunkState.downloaded:
                    entry["tid"] = "DL"
                    entry["name"] = f"Downloading chunk {chunk_id}"
                    entry["ph"] = "E"
                    entry["cat"] = "DOWNLOAD"
                elif state == ChunkState.upload_queued:
                    continue
                    # entry["name"] = f"Queue chunk {chunk_id}"
                    # entry["ph"] = "B"
                    # entry["cat"] = "UPLOAD_QUEUED"
                elif state == ChunkState.upload_in_progress:
                    # entry["name"] = f"Queue chunk {chunk_id}"
                    # entry["ph"] = "E"
                    # entry["cat"] = "UPLOAD_QUEUED"
                    # trace["traceEvents"].append(entry)
                    entry["tid"] = "UL"
                    entry["name"] = f"Uploading chunk {chunk_id}"
                    entry["ph"] = "B"
                    entry["cat"] = "UPLOAD"
                elif state == ChunkState.upload_complete:
                    entry["tid"] = "UL"
                    entry["name"] = f"Uploading chunk {chunk_id}"
                    entry["ph"] = "E"
                    entry["cat"] = "UPLOAD"
                elif state == ChunkState.failed:
                    entry["tid"] = "UL"
                    entry["name"] = f"Fail; chunk {chunk_id}"
                    entry["ph"] = "I"
                    entry["cat"] = "UPLOAD"

                trace["traceEvents"].append(entry)

        with open(out_file, "w") as f:
            json.dump(trace, f)
