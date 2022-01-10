import atexit
from contextlib import closing
from datetime import datetime
import itertools
import json
from logging import log
from functools import partial
import time
from typing import Dict, List, Optional, Tuple

import requests
from loguru import logger
from tqdm import tqdm
import pandas as pd
from skylark import GB, KB, MB

from skylark.benchmark.utils import refresh_instance_list
from skylark.compute.aws.aws_cloud_provider import AWSCloudProvider
from skylark.compute.gcp.gcp_cloud_provider import GCPCloudProvider
from skylark.compute.server import Server, ServerState
from skylark.gateway.chunk import Chunk, ChunkRequest, ChunkRequestHop, ChunkState
from skylark.obj_store.s3_interface import S3Interface
from skylark.replicate.replication_plan import ReplicationJob, ReplicationTopology
from skylark.replicate.replicator_client_dashboard import ReplicatorClientDashboard
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
            for r in self.aws.region_list():
                jobs.append(partial(self.aws.add_ip_to_security_group, r))
        if self.gcp is not None:
            jobs.append(self.gcp.create_ssh_key)
            jobs.append(self.gcp.configure_default_network)
            jobs.append(self.gcp.configure_default_firewall)
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
                current_aws_instances = refresh_instance_list(
                    self.aws, set([r.split(":")[1] for r in aws_regions_to_provision]), aws_instance_filter
                )
                for r, ilist in current_aws_instances.items():
                    for i in ilist:
                        if f"aws:{r}" in aws_regions_to_provision:
                            aws_regions_to_provision.remove(f"aws:{r}")
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

        with Timer("Provisioning instances and waiting to boot"):
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
                desc="Provisioning instances and waiting to boot",
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

        with Timer("Install gateway package on instances"):
            # start gateways
            do_parallel(
                lambda s: s.start_gateway(
                    gateway_docker_image=self.gateway_docker_image, num_outgoing_connections=num_outgoing_connections
                ),
                list(itertools.chain(*self.bound_paths)),
                progress_bar=True,
                desc="Install gateway package on instances",
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
            file_size_bytes = job.random_chunk_size_mb * MB  # todo support object store objects
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
        with tqdm(total=len(chunks), desc="Building chunk requests", leave=False) as pbar:
            for batch, path in zip(chunk_batches, self.bound_paths):
                chunk_requests_sharded[path[0]] = []
                for chunk in batch:
                    # make ChunkRequestHop list
                    cr_path = []
                    for hop_idx, hop_instance in enumerate(path):
                        # todo support object stores
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

    def get_chunk_status_log_df(self) -> pd.DataFrame:
        chunk_logs = []
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
                    chunk_logs.append(log_entry)
        df = pd.DataFrame(chunk_logs)
        return df

    def monitor_transfer(
        self,
        crs: List[ChunkRequest],
        completed_state=ChunkState.upload_complete,
        serve_web_dashboard=False,
        dash_host="0.0.0.0",
        dash_port="8080",
    ) -> Dict:
        total_bytes = sum([cr.chunk.chunk_length_bytes for cr in crs])
        if serve_web_dashboard:
            dash = ReplicatorClientDashboard(dash_host, dash_port)
            dash.start()
            atexit.register(dash.shutdown)
            logger.info(f"Web dashboard running at {dash.dashboard_url}")
        with tqdm(total=total_bytes * 8, desc="Replication", unit="bit", unit_scale=True, unit_divisor=KB) as pbar:
            while True:
                log_df = self.get_chunk_status_log_df()
                if serve_web_dashboard:
                    dash.update_status_df(log_df)

                # count completed bytes
                last_log_df = (
                    log_df.groupby(["chunk_id"])
                    .apply(lambda df: df.sort_values(["path_idx", "hop_idx", "time"], ascending=False).head(1))
                    .reset_index(drop=True)
                )
                is_complete_fn = (
                    lambda row: row["state"] >= completed_state
                    and row["path_idx"] == len(self.bound_paths) - 1
                    and row["hop_idx"] == len(self.bound_paths[row["path_idx"]]) - 1
                )
                completed_chunk_ids = last_log_df[last_log_df.apply(is_complete_fn, axis=1)].chunk_id.values
                completed_bytes = sum([cr.chunk.chunk_length_bytes for cr in crs if cr.chunk.chunk_id in completed_chunk_ids])

                # update progress bar
                pbar.update(completed_bytes * 8 - pbar.n)
                total_runtime_s = (log_df.time.max() - log_df.time.min()).total_seconds()
                throughput_gbits = completed_bytes * 8 / GB / total_runtime_s
                pbar.set_description(f"Replication: average {throughput_gbits:.2f}Gbit/s")

                if len(completed_chunk_ids) == len(crs):
                    if serve_web_dashboard:
                        dash.shutdown()
                    return dict(
                        completed_chunk_ids=completed_chunk_ids,
                        total_runtime_s=total_runtime_s,
                        throughput_gbits=throughput_gbits,
                    )
                else:
                    time.sleep(0.25)
