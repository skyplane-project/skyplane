import atexit
from datetime import datetime
import itertools
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
from skylark.compute.azure.azure_cloud_provider import AzureCloudProvider
from skylark.compute.gcp.gcp_cloud_provider import GCPCloudProvider
from skylark.compute.server import Server, ServerState
from skylark.chunk import Chunk, ChunkRequest, ChunkRequestHop, ChunkState
from skylark.replicate.replication_plan import ReplicationJob, ReplicationTopology
from skylark.replicate.replicator_client_dashboard import ReplicatorClientDashboard
from skylark.utils.utils import PathLike, Timer, do_parallel


class ReplicatorClient:
    def __init__(
        self,
        topology: ReplicationTopology,
        azure_subscription: Optional[str],
        gcp_project: Optional[str],
        gateway_docker_image: str = "ghcr.io/parasj/skylark:latest",
        aws_instance_class: Optional[str] = "m5.4xlarge",  # set to None to disable AWS
        azure_instance_class: Optional[str] = "Standard_D2_v5",  # set to None to disable Azure
        gcp_instance_class: Optional[str] = "n2-standard-16",  # set to None to disable GCP
        gcp_use_premium_network: bool = True,
    ):
        self.topology = topology
        self.gateway_docker_image = gateway_docker_image
        self.aws_instance_class = aws_instance_class
        self.azure_instance_class = azure_instance_class
        self.azure_subscription = azure_subscription
        self.gcp_instance_class = gcp_instance_class
        self.gcp_use_premium_network = gcp_use_premium_network

        # provisioning
        self.aws = AWSCloudProvider() if aws_instance_class != "None" else None
        self.azure = AzureCloudProvider(azure_subscription) if azure_instance_class != "None" and azure_subscription is not None else None
        self.gcp = GCPCloudProvider(gcp_project) if gcp_instance_class != "None" and gcp_project is not None else None
        self.bound_paths: Optional[List[List[Server]]] = None

        # init clouds
        jobs = []
        if self.aws is not None:
            for r in self.aws.region_list():
                jobs.append(partial(self.aws.add_ip_to_security_group, r))
        if self.azure is not None:
            jobs.append(self.azure.create_ssh_key)
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
        azure_regions_to_provision = [r for r in regions_to_provision if r.startswith("azure:")]
        gcp_regions_to_provision = [r for r in regions_to_provision if r.startswith("gcp:")]

        assert len(aws_regions_to_provision) == 0 or self.aws is not None, "AWS not enabled"
        assert len(azure_regions_to_provision) == 0 or self.azure is not None, "Azure not enabled"
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

            if self.azure is not None:
                azure_instance_filter = {
                    "tags": {"skylark": "true"},
                    "instance_type": self.azure_instance_class,
                    "state": [ServerState.PENDING, ServerState.RUNNING],
                }
                current_azure_instances = refresh_instance_list(
                    self.azure, set([r.split(":")[1] for r in azure_regions_to_provision]), azure_instance_filter
                )
                for r, ilist in current_azure_instances.items():
                    for i in ilist:
                        if f"azure:{r}" in azure_regions_to_provision:
                            azure_regions_to_provision.remove(f"azure:{r}")
            else:
                current_azure_instances = {}

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
                elif provider == "azure":
                    assert self.azure is not None
                    server = self.azure.provision_instance(subregion, self.azure_instance_class)
                elif provider == "gcp":
                    assert self.gcp is not None
                    # todo specify network tier in ReplicationTopology
                    server = self.gcp.provision_instance(subregion, self.gcp_instance_class, premium_network=self.gcp_use_premium_network)
                else:
                    raise NotImplementedError(f"Unknown provider {provider}")
                return server

            results = do_parallel(
                provision_gateway_instance, list(aws_regions_to_provision + azure_regions_to_provision + gcp_regions_to_provision)
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
            for r, ilist in current_azure_instances.items():
                if f"azure:{r}" not in instances_by_region:
                    instances_by_region[f"azure:{r}"] = []
                instances_by_region[f"azure:{r}"].extend(ilist)
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
            )

    def deprovision_gateways(self):
        def deprovision_gateway_instance(server: Server):
            if server.instance_state() == ServerState.RUNNING:
                server.terminate_instance()

        instances = [instance for path in self.bound_paths for instance in path]
        do_parallel(deprovision_gateway_instance, instances, n=len(instances))

    def run_replication_plan(self, job: ReplicationJob):
        # assert all(len(path) == 2 for path in self.bound_paths), f"Only two-hop replication is supported"

        assert job.source_region.split(":")[0] in [
            "aws",
            "azure",
            "gcp",
        ], f"Only AWS, Azure, and GCP are supported, but got {job.source_region}"
        assert job.dest_region.split(":")[0] in [
            "aws",
            "azure",
            "gcp",
        ], f"Only AWS, Azure, and GCP are supported, but got {job.dest_region}"

        # pre-fetch instance IPs for all gateways
        gateway_ips: Dict[Server, str] = {s: s.public_ip() for path in self.bound_paths for s in path}

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
        chunk_requests_sharded: Dict[int, List[ChunkRequest]] = {}
        with Timer("Building chunk requests"):
            for batch, (path_idx, path_instances) in zip(chunk_batches, enumerate(self.bound_paths)):
                chunk_requests_sharded[path_idx] = []
                for chunk in batch:
                    cr_path = []
                    for hop_idx, hop_instance in enumerate(path_instances):
                        # todo support object stores
                        if hop_idx == 0:  # source gateway
                            location = f"random_{job.random_chunk_size_mb}MB"
                        elif hop_idx == len(path_instances) - 1:  # destination gateway
                            location = "save_local"
                        else:  # intermediate gateway
                            location = "relay"
                        cr_path.append(
                            ChunkRequestHop(
                                hop_cloud_region=hop_instance.region_tag,
                                hop_ip_address=gateway_ips[hop_instance],
                                chunk_location_type=location,
                            )
                        )
                    chunk_requests_sharded[path_idx].append(ChunkRequest(chunk, cr_path))

        # send chunk requests to start gateways in parallel
        with Timer("Dispatch chunk requests"):

            def send_chunk_requests(args: Tuple[Server, List[ChunkRequest]]):
                hop_instance, chunk_requests = args
                ip = gateway_ips[hop_instance]
                logger.debug(f"Sending {len(chunk_requests)} chunk requests to {ip}")
                reply = requests.post(f"http://{ip}:8080/api/v1/chunk_requests", json=[cr.as_dict() for cr in chunk_requests])
                if reply.status_code != 200:
                    raise Exception(f"Failed to send chunk requests to gateway instance {hop_instance.instance_name()}: {reply.text}")

            start_instances = [(path[0], chunk_requests_sharded[path_idx]) for path_idx, path in enumerate(self.bound_paths)]
            do_parallel(send_chunk_requests, start_instances, n=-1)

        return [cr for crlist in chunk_requests_sharded.values() for cr in crlist]

    def get_chunk_status_log_df(self) -> pd.DataFrame:
        def get_chunk_status(args):
            hop_instance, path_idx, hop_idx = args
            reply = requests.get(f"http://{hop_instance.public_ip()}:8080/api/v1/chunk_status_log")
            if reply.status_code != 200:
                raise Exception(f"Failed to get chunk status from gateway instance {hop_instance.instance_name()}: {reply.text}")
            logs = []
            for log_entry in reply.json()["chunk_status_log"]:
                log_entry["hop_cloud_region"] = hop_instance.region_tag
                log_entry["path_idx"] = path_idx
                log_entry["hop_idx"] = hop_idx
                log_entry["time"] = datetime.fromisoformat(log_entry["time"])
                log_entry["state"] = ChunkState.from_str(log_entry["state"])
                logs.append(log_entry)
            return logs

        reqs = []
        for path_idx, path in enumerate(self.bound_paths):
            for hop_idx, hop_instance in enumerate(path):
                reqs.append((hop_instance, path_idx, hop_idx))

        # aggregate results
        rows = []
        for _, result in do_parallel(get_chunk_status, reqs, n=-1):
            rows.extend(result)
        return pd.DataFrame(rows)

    def monitor_transfer(
        self,
        crs: List[ChunkRequest],
        completed_state=ChunkState.upload_complete,
        show_pbar=False,
        log_interval_s: Optional[float] = None,
        serve_web_dashboard=False,
        dash_host="0.0.0.0",
        dash_port="8080",
        time_limit_seconds: Optional[float] = None,
        cancel_pending: bool = True,
    ) -> Dict:
        total_bytes = sum([cr.chunk.chunk_length_bytes for cr in crs])
        if serve_web_dashboard:
            dash = ReplicatorClientDashboard(dash_host, dash_port)
            dash.start()
            atexit.register(dash.shutdown)
            logger.info(f"Web dashboard running at {dash.dashboard_url}")
        last_log = None

        # register atexit handler to cancel pending chunk requests (force shutdown gateways)
        if cancel_pending:

            def shutdown_handler():
                def fn(s: Server):
                    logger.warning(f"Cancelling pending chunk requests to {s.public_ip()}")
                    try:
                        requests.post(f"http://{s.public_ip()}:8080/api/v1/shutdown")
                    except requests.exceptions.ConnectionError as e:
                        return  # ignore connection errors since server may be shutting down

                do_parallel(fn, [hop for path in self.bound_paths for hop in path], n=-1)
                logger.warning("Cancelled pending chunk requests")

            atexit.register(shutdown_handler)

        with Timer() as t:
            with tqdm(
                total=total_bytes * 8, desc="Replication", unit="bit", unit_scale=True, unit_divisor=KB, disable=not show_pbar
            ) as pbar:
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
                        lambda row: row["state"] >= completed_state and row["hop_idx"] == len(self.bound_paths[row["path_idx"]]) - 1
                    )
                    completed_chunk_ids = last_log_df[last_log_df.apply(is_complete_fn, axis=1)].chunk_id.values
                    completed_bytes = sum([cr.chunk.chunk_length_bytes for cr in crs if cr.chunk.chunk_id in completed_chunk_ids])

                    # update progress bar
                    pbar.update(completed_bytes * 8 - pbar.n)
                    total_runtime_s = (log_df.time.max() - log_df.time.min()).total_seconds()
                    throughput_gbits = completed_bytes * 8 / GB / total_runtime_s if total_runtime_s > 0 else 0.0
                    pbar.set_description(f"Replication: average {throughput_gbits:.2f}Gbit/s")

                    if len(completed_chunk_ids) == len(crs) or time_limit_seconds is not None and t.elapsed > time_limit_seconds:
                        if serve_web_dashboard:
                            dash.shutdown()
                        if cancel_pending:
                            atexit.unregister(shutdown_handler)
                        return dict(
                            completed_chunk_ids=completed_chunk_ids,
                            total_runtime_s=total_runtime_s,
                            throughput_gbits=throughput_gbits,
                            monitor_status="completed" if len(completed_chunk_ids) == len(crs) else "timed_out",
                        )
                    else:
                        current_time = datetime.now()
                        if log_interval_s and (not last_log or (current_time - last_log).seconds > float(log_interval_s)):
                            last_log = current_time
                            gbits_remaining = (total_bytes - completed_bytes) * 8 / GB
                            eta = int(gbits_remaining / throughput_gbits) if throughput_gbits > 0 else None
                            logger.debug(
                                f"{len(completed_chunk_ids)}/{len(crs)} chunks done ({completed_bytes / GB:.2f} / {total_bytes / GB:.2f}GB, {throughput_gbits:.2f}Gbit/s, ETA={str(eta) + 's' if eta is not None else 'unknown'})"
                            )
                        elif t.elapsed > 20 and completed_bytes == 0:
                            logger.error(f"No chunks completed after {int(t.elapsed)}s! There is probably a bug, check logs. Exiting...")
                            return dict(
                                completed_chunk_ids=completed_chunk_ids,
                                total_runtime_s=total_runtime_s,
                                throughput_gbits=throughput_gbits,
                                monitor_status="timed_out",
                            )
                        time.sleep(0.01 if show_pbar else 0.25)
