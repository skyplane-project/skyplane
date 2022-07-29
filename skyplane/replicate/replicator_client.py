import json
import pickle
import time
import uuid
from datetime import datetime
from functools import partial
from typing import Dict, List, Optional, Tuple, Iterable
import nacl.secret
import nacl.utils

import pandas as pd
from rich.progress import Progress, SpinnerColumn, TextColumn, TimeRemainingColumn, DownloadColumn, BarColumn, TransferSpeedColumn
import urllib3

from skyplane import GB, MB, gateway_docker_image, tmp_log_dir
from skyplane import exceptions
from skyplane.chunk import Chunk, ChunkRequest, ChunkState
from skyplane.compute.aws.aws_cloud_provider import AWSCloudProvider
from skyplane.compute.azure.azure_cloud_provider import AzureCloudProvider
from skyplane.compute.azure.azure_server import AzureServer
from skyplane.compute.cloud_providers import CloudProvider
from skyplane.compute.gcp.gcp_cloud_provider import GCPCloudProvider
from skyplane.compute.server import Server, ServerState
from skyplane.obj_store.object_store_interface import ObjectStoreInterface, NoSuchObjectException
from skyplane.replicate.profiler import status_df_to_traceevent
from skyplane.replicate.replication_plan import ReplicationJob, ReplicationTopology, ReplicationTopologyGateway
from skyplane.utils import logger
from skyplane.utils.fn import PathLike, do_parallel
from skyplane.utils.timer import Timer


class ReplicatorClient:
    def __init__(
        self,
        topology: ReplicationTopology,
        gateway_docker_image: str = gateway_docker_image(),
        aws_instance_class: Optional[str] = "m5.4xlarge",  # set to None to disable AWS
        azure_instance_class: Optional[str] = "Standard_D2_v5",  # set to None to disable Azure
        gcp_instance_class: Optional[str] = "n2-standard-16",  # set to None to disable GCP
        gcp_use_premium_network: bool = True,
    ):
        self.http_pool = urllib3.PoolManager(retries=urllib3.Retry(total=3))
        self.topology = topology
        self.gateway_docker_image = gateway_docker_image
        self.aws_instance_class = aws_instance_class
        self.azure_instance_class = azure_instance_class
        self.gcp_instance_class = gcp_instance_class
        self.gcp_use_premium_network = gcp_use_premium_network

        # provisioning
        self.aws = AWSCloudProvider()
        self.azure = AzureCloudProvider()
        self.gcp = GCPCloudProvider()
        self.bound_nodes: Dict[ReplicationTopologyGateway, Server] = {}
        self.temp_nodes: List[Server] = []  # saving nodes that are not yet bound so they can be deprovisioned later

        # logging
        self.transfer_dir = tmp_log_dir / "transfer_logs" / datetime.now().strftime("%Y%m%d_%H%M%S")
        self.transfer_dir.mkdir(exist_ok=True, parents=True)
        logger.open_log_file(self.transfer_dir / "client.log")

        # upload requests
        self.multipart_upload_requests = []

    def provision_gateways(
        self,
        reuse_instances=False,
        log_dir: Optional[PathLike] = None,
        authorize_ssh_pub_key: Optional[PathLike] = None,
        use_bbr=True,
        use_compression=True,
        use_e2ee=True,
        use_socket_tls=False,
    ):
        regions_to_provision = [node.region for node in self.topology.gateway_nodes]
        aws_regions_to_provision = [r for r in regions_to_provision if r.startswith("aws:")]
        azure_regions_to_provision = [r for r in regions_to_provision if r.startswith("azure:")]
        gcp_regions_to_provision = [r for r in regions_to_provision if r.startswith("gcp:")]

        assert (
            len(aws_regions_to_provision) == 0 or self.aws.auth.enabled()
        ), "AWS credentials not configured but job provisions AWS gateways"
        assert (
            len(azure_regions_to_provision) == 0 or self.azure.auth.enabled()
        ), "Azure credentials not configured but job provisions Azure gateways"
        assert (
            len(gcp_regions_to_provision) == 0 or self.gcp.auth.enabled()
        ), "GCP credentials not configured but job provisions GCP gateways"

        # reuse existing AWS instances
        if reuse_instances:
            if self.aws.auth.enabled():
                aws_instance_filter = {
                    "tags": {"skyplane": "true"},
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

            if self.azure.auth.enabled():
                azure_instance_filter = {
                    "tags": {"skyplane": "true"},
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

            if self.gcp.auth.enabled():
                gcp_instance_filter = {
                    "tags": {"skyplane": "true"},
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

        # init clouds
        jobs = []
        if aws_regions_to_provision:
            jobs.append(partial(self.aws.create_iam, attach_policy_arn="arn:aws:iam::aws:policy/AmazonS3FullAccess"))
            for r in set(aws_regions_to_provision):

                def init_aws_vpc(r):
                    self.aws.make_vpc(r)
                    self.aws.authorize_client(r, "0.0.0.0/0")

                jobs.append(partial(init_aws_vpc, r.split(":")[1]))
                jobs.append(partial(self.aws.ensure_keyfile_exists, r.split(":")[1]))
        if azure_regions_to_provision:
            jobs.append(self.azure.create_ssh_key)
            jobs.append(self.azure.set_up_resource_group)
        if gcp_regions_to_provision:
            jobs.append(self.gcp.create_ssh_key)
            jobs.append(self.gcp.configure_skyplane_network)
            jobs.append(self.gcp.configure_skyplane_firewall)
        do_parallel(lambda fn: fn(), jobs, spinner=True, spinner_persist=True, desc="Initializing cloud keys")

        # provision instances
        def provision_gateway_instance(region: str) -> Server:
            provider, subregion = region.split(":")
            if provider == "aws":
                assert self.aws.auth.enabled()
                server = self.aws.provision_instance(subregion, self.aws_instance_class)
            elif provider == "azure":
                assert self.azure.auth.enabled()
                server = self.azure.provision_instance(subregion, self.azure_instance_class)
            elif provider == "gcp":
                assert self.gcp.auth.enabled()
                # todo specify network tier in ReplicationTopology
                server = self.gcp.provision_instance(subregion, self.gcp_instance_class, premium_network=self.gcp_use_premium_network)
            else:
                raise NotImplementedError(f"Unknown provider {provider}")
            server.enable_auto_shutdown()
            self.temp_nodes.append(server)
            return server

        results = do_parallel(
            provision_gateway_instance,
            list(aws_regions_to_provision + azure_regions_to_provision + gcp_regions_to_provision),
            spinner=True,
            spinner_persist=True,
            desc="Provisioning gateway instances",
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
                self.temp_nodes.extend(ilist)
            for r, ilist in current_azure_instances.items():
                if f"azure:{r}" not in instances_by_region:
                    instances_by_region[f"azure:{r}"] = []
                instances_by_region[f"azure:{r}"].extend(ilist)
                self.temp_nodes.extend(ilist)
            for r, ilist in current_gcp_instances.items():
                if f"gcp:{r}" not in instances_by_region:
                    instances_by_region[f"gcp:{r}"] = []
                instances_by_region[f"gcp:{r}"].extend(ilist)
                self.temp_nodes.extend(ilist)

        # bind instances to nodes
        for node in self.topology.gateway_nodes:
            instance = instances_by_region[node.region].pop()
            self.bound_nodes[node] = instance
            self.temp_nodes.remove(instance)

        # Firewall rules
        # todo add firewall rules for Azure
        public_ips = [self.bound_nodes[n].public_ip() for n in self.topology.gateway_nodes]
        authorize_ip_jobs = []
        authorize_ip_jobs.extend(
            [partial(self.aws.add_ips_to_security_group, r.split(":")[1], public_ips) for r in set(aws_regions_to_provision)]
        )
        if gcp_regions_to_provision:
            authorize_ip_jobs.append(partial(self.gcp.add_ips_to_firewall, public_ips))
        do_parallel(lambda fn: fn(), authorize_ip_jobs, spinner=True, desc="Applying firewall rules")

        # generate E2EE key
        if use_e2ee:
            e2ee_key_bytes = nacl.utils.random(nacl.secret.SecretBox.KEY_SIZE)
        else:
            e2ee_key_bytes = None

        # setup instances
        def setup(args: Tuple[Server, Dict[str, int], bool, bool]):
            server, outgoing_ports, am_source, am_sink = args
            if log_dir:
                server.init_log_files(log_dir)
            if authorize_ssh_pub_key:
                server.copy_public_key(authorize_ssh_pub_key)
            server.start_gateway(
                outgoing_ports,
                gateway_docker_image=self.gateway_docker_image,
                use_bbr=use_bbr,
                use_compression=use_compression,
                e2ee_key_bytes=e2ee_key_bytes if (am_source or am_sink) else None,
                use_socket_tls=use_socket_tls,
            )

        args = []
        sources = self.topology.source_instances()
        sinks = self.topology.sink_instances()
        for node, server in self.bound_nodes.items():
            setup_args = {
                self.bound_nodes[n].public_ip(): v
                for n, v in self.topology.get_outgoing_paths(node).items()
                if isinstance(n, ReplicationTopologyGateway)
            }
            args.append((server, setup_args, node in sources, node in sinks))
        do_parallel(setup, args, n=-1, spinner=True, spinner_persist=True, desc="Installing gateway package")

    def deprovision_gateways(self):
        # This is a good place to tear down Security Groups and the instance since this is invoked by CLI too.
        def deprovision_gateway_instance(server: Server):
            if server.instance_state() == ServerState.RUNNING:
                server.terminate_instance()
                logger.fs.warning(f"Deprovisioned {server.uuid()}")

        # Clear IPs from security groups
        # todo remove firewall rules for Azure
        public_ips = [i.public_ip() for i in self.bound_nodes.values()] + [i.public_ip() for i in self.temp_nodes]
        aws_regions = [node.region for node in self.topology.gateway_nodes if node.region.startswith("aws:")]
        aws_jobs = [partial(self.aws.remove_ips_from_security_group, r.split(":")[1], public_ips) for r in set(aws_regions)]
        gcp_regions = [node.region for node in self.topology.gateway_nodes if node.region.startswith("gcp:")]
        gcp_jobs = [partial(self.gcp.remove_ips_from_firewall, public_ips)] if gcp_regions else []
        do_parallel(lambda fn: fn(), aws_jobs + gcp_jobs, desc="Removing firewall rules")

        # Terminate instances
        instances = list(self.bound_nodes.values()) + self.temp_nodes
        logger.fs.warning(f"Deprovisioning {len(instances)} instances")
        if any(i.provider == "azure" for i in instances):
            logger.warning(
                f"NOTE: Azure is very slow to terminate instances. Consider using --reuse-instances and then deprovisioning the instances manually with `skyplane deprovision`."
            )
        do_parallel(deprovision_gateway_instance, instances, n=-1, spinner=True, spinner_persist=True, desc="Deprovisioning instances")
        self.temp_nodes = []
        logger.fs.info("Deprovisioned instances")

    def run_replication_plan(
        self,
        job: ReplicationJob,
        multipart_enabled: bool = False,
        multipart_max_chunk_size_mb: int = 8,
    ) -> ReplicationJob:
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

        # assign source and destination gateways permission to buckets
        assign_jobs = []
        if job.source_region.split(":")[0] == "azure":
            for gateway in self.bound_nodes.values():
                if isinstance(gateway, AzureServer):
                    assign_jobs.append(gateway.authorize_subscription)
        do_parallel(lambda fn: fn(), assign_jobs, spinner=True, spinner_persist=True, desc="Assigning gateways permissions to buckets")

        with Progress(
            SpinnerColumn(),
            TextColumn("Preparing replication plan{task.description}"),
            transient=True,
        ) as progress:
            prepare_task = progress.add_task("", total=None)

            # pre-fetch instance IPs for all gateways
            progress.update(prepare_task, description=": Fetching instance IPs")
            gateway_ips: Dict[Server, str] = {s: s.public_ip() for s in self.bound_nodes.values()}

            # make list of chunks
            progress.update(prepare_task, description=": Querying source object store for matching keys")
            chunks = []
            idx = 0
            for (src_object, dest_object) in job.transfer_pairs:
                if not job.random_chunk_size_mb:
                    if multipart_enabled:
                        chunk_size_bytes = int(multipart_max_chunk_size_mb * MB)
                        num_chunks = int(src_object.size / chunk_size_bytes) + 1

                        # TODO: figure out what to do on # part limits per object
                        # TODO: only do if num_chunks > 1
                        # TODO: potentially do this in a seperate thread, and/or after chunks sent
                        obj_store_interface = ObjectStoreInterface.create(job.dest_region, job.dest_bucket)
                        logger.fs.info(f"Initiate multipart upload on {dest_object}")
                        upload_id = obj_store_interface.initiate_multipart_upload(dest_object.key)

                        offset = 0
                        part_num = 1
                        parts = []
                        for chunk in range(num_chunks):
                            # size is min(chunk_size, remaining data)
                            file_size_bytes = min(chunk_size_bytes, src_object.size - offset)
                            assert file_size_bytes > 0, f"File size <= 0 {file_size_bytes}"
                            chunks.append(
                                Chunk(
                                    src_key=src_object.key,
                                    dest_key=dest_object.key,
                                    chunk_id=idx,
                                    file_offset_bytes=offset,
                                    chunk_length_bytes=file_size_bytes,
                                    part_number=part_num,
                                    upload_id=upload_id,
                                )
                            )
                            parts.append(part_num)

                            idx += 1
                            part_num += 1
                            offset += chunk_size_bytes
                        # add multipart upload request
                        self.multipart_upload_requests.append(
                            {
                                "region": job.dest_region,
                                "bucket": job.dest_bucket,
                                "upload_id": upload_id,
                                "key": dest_object.key,
                                "parts": parts,
                            }
                        )
                    # transfer entire object
                    else:
                        chunks.append(
                            Chunk(
                                src_key=src_object.key,
                                dest_key=dest_object.key,
                                chunk_id=idx,
                                file_offset_bytes=0,
                                chunk_length_bytes=src_object.size,
                            )
                        )
                        idx += 1
                # random data replication
                else:
                    chunks.append(
                        Chunk(
                            src_key=src_object.key,
                            dest_key=dest_object.key,
                            chunk_id=idx,
                            file_offset_bytes=0,
                            chunk_length_bytes=job.random_chunk_size_mb * MB,
                        )
                    )
                    idx += 1

            # partition chunks into roughly equal-sized batches (by bytes)
            def partition(items: List[Chunk], n_batches: int) -> List[List[Chunk]]:
                batches = [[] for _ in range(n_batches)]
                items.sort(key=lambda c: c.chunk_length_bytes, reverse=True)
                batch_sizes = [0 for _ in range(n_batches)]
                for item in items:
                    min_batch = batch_sizes.index(min(batch_sizes))
                    batches[min_batch].append(item)
                    batch_sizes[min_batch] += item.chunk_length_bytes
                return batches

            progress.update(prepare_task, description=": Partitioning chunks into batches")
            src_instances = [self.bound_nodes[n] for n in self.topology.source_instances()]
            chunk_batches = partition(chunks, len(src_instances))
            assert (len(chunk_batches) == (len(src_instances) - 1)) or (
                len(chunk_batches) == len(src_instances)
            ), f"{len(chunk_batches)} batches, expected {len(src_instances)}"
            for batch_idx, batch in enumerate(chunk_batches):
                logger.fs.info(f"Batch {batch_idx} size: {sum(c.chunk_length_bytes for c in batch)} with {len(batch)} chunks")

            # make list of ChunkRequests
            with Timer("Building chunk requests"):
                # make list of ChunkRequests
                progress.update(prepare_task, description=": Building list of chunk requests")
                chunk_requests_sharded: Dict[int, List[ChunkRequest]] = {}
                for batch_idx, batch in enumerate(chunk_batches):
                    chunk_requests_sharded[batch_idx] = []
                    for chunk in batch:
                        chunk_requests_sharded[batch_idx].append(
                            ChunkRequest(
                                chunk=chunk,
                                src_region=job.source_region,
                                dst_region=job.dest_region,
                                src_type="object_store" if job.dest_bucket else "random",
                                dst_type="object_store" if job.dest_bucket else "save_local",
                                src_random_size_mb=job.random_chunk_size_mb,
                                src_object_store_bucket=job.source_bucket,
                                dst_object_store_bucket=job.dest_bucket,
                            )
                        )
                    logger.fs.debug(f"Batch {batch_idx} size: {sum(c.chunk_length_bytes for c in batch)} with {len(batch)} chunks")

                # send chunk requests to start gateways in parallel
                progress.update(prepare_task, description=": Dispatching chunk requests to source gateways")

                def send_chunk_requests(args: Tuple[Server, List[ChunkRequest]]):
                    hop_instance, chunk_requests = args
                    while chunk_requests:
                        batch, chunk_requests = chunk_requests[: 1024 * 16], chunk_requests[1024 * 16 :]
                        reply = self.http_pool.request(
                            "POST",
                            f"{hop_instance.gateway_api_url}/api/v1/chunk_requests",
                            body=json.dumps([c.as_dict() for c in batch]).encode("utf-8"),
                            headers={"Content-Type": "application/json"},
                        )
                        if reply.status != 200:
                            raise Exception(
                                f"Failed to send chunk requests to gateway instance {hop_instance.instance_name()}: {reply.data.decode('utf-8')}"
                            )
                        logger.fs.debug(
                            f"Sent {len(batch)} chunk requests to {hop_instance.instance_name()}, {len(chunk_requests)} remaining"
                        )

                start_instances = list(zip(src_instances, chunk_requests_sharded.values()))
                do_parallel(send_chunk_requests, start_instances, n=-1)

        job.chunk_requests = [cr for crlist in chunk_requests_sharded.values() for cr in crlist]
        return job

    def get_chunk_status_log_df(self) -> pd.DataFrame:
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
        for result in do_parallel(get_chunk_status, self.bound_nodes.items(), n=-1, return_args=False):
            rows.extend(result)
        return pd.DataFrame(rows)

    def check_error_logs(self) -> Dict[str, List[str]]:
        def get_error_logs(args):
            _, instance = args
            reply = self.http_pool.request("GET", f"{instance.gateway_api_url}/api/v1/errors")
            if reply.status != 200:
                raise Exception(f"Failed to get error logs from gateway instance {instance.instance_name()}: {reply.data.decode('utf-8')}")
            return json.loads(reply.data.decode("utf-8"))["errors"]

        errors: Dict[str, List[str]] = {}
        for (_, instance), result in do_parallel(get_error_logs, self.bound_nodes.items(), n=-1):
            errors[instance] = result
        return errors

    def monitor_transfer(
        self,
        job: ReplicationJob,
        show_spinner=False,
        log_interval_s: Optional[float] = None,
        time_limit_seconds: Optional[float] = None,
        cleanup_gateway: bool = True,
        save_log: bool = True,
        write_profile: bool = False,
        write_socket_profile: bool = False,  # slow but useful for debugging
        copy_gateway_logs: bool = False,
        multipart: bool = False,  # multipart object uploads/downloads
    ) -> Optional[Dict]:
        assert job.chunk_requests is not None
        total_bytes = sum([cr.chunk.chunk_length_bytes for cr in job.chunk_requests])
        last_log = None

        sources = self.topology.source_instances()
        source_regions = set(s.region for s in sources)
        sinks = self.topology.sink_instances()
        sink_regions = set(s.region for s in sinks)

        completed_chunk_ids = []

        if save_log:
            (self.transfer_dir / "job.pkl").write_bytes(pickle.dumps(job))
        try:
            with Progress(
                SpinnerColumn(),
                TextColumn("Transfer progress{task.description}"),
                BarColumn(),
                DownloadColumn(binary_units=True),
                TransferSpeedColumn(),
                TimeRemainingColumn(),
                disable=not show_spinner,
            ) as progress:
                copy_task = progress.add_task("", total=total_bytes)
                with Timer() as t:
                    while True:
                        # refresh shutdown status by running noop
                        do_parallel(lambda i: i.run_command("echo 1"), self.bound_nodes.values(), n=-1)

                        # check for errors and exit if there are any (while setting debug flags)
                        errors = self.check_error_logs()
                        if any(errors.values()):
                            copy_gateway_logs = True
                            write_profile = True
                            write_socket_profile = True

                            return {
                                "errors": errors,
                                "monitor_status": "error",
                            }

                        log_df = self.get_chunk_status_log_df()
                        if log_df.empty:
                            logger.warning("No chunk status log entries yet")
                            time.sleep(0.5)
                            continue

                        is_complete_rec = (
                            lambda row: row["state"] == ChunkState.upload_complete
                            and row["instance"] in [s.instance for s in sinks]
                            and row["region"] in [s.region for s in sinks]
                        )
                        sink_status_df = log_df[log_df.apply(is_complete_rec, axis=1)]
                        completed_status = sink_status_df.groupby("chunk_id").apply(
                            lambda x: set(x["region"].unique()) == set(sink_regions)
                        )
                        completed_chunk_ids = completed_status[completed_status].index
                        completed_bytes = sum(
                            [cr.chunk.chunk_length_bytes for cr in job.chunk_requests if cr.chunk.chunk_id in completed_chunk_ids]
                        )

                        # update progress bar
                        total_runtime_s = (log_df.time.max() - log_df.time.min()).total_seconds()
                        throughput_gbits = completed_bytes * 8 / GB / total_runtime_s if total_runtime_s > 0 else 0.0

                        # make log line
                        progress.update(
                            copy_task,
                            description=f" ({len(completed_chunk_ids)} of {len(job.chunk_requests)} chunks)",
                            completed=completed_bytes,
                        )
                        if len(completed_chunk_ids) == len(job.chunk_requests):
                            if multipart:
                                # Complete multi-part uploads
                                def complete_upload(req):
                                    obj_store_interface = ObjectStoreInterface.create(req["region"], req["bucket"])
                                    succ = obj_store_interface.complete_multipart_upload(req["key"], req["upload_id"])
                                    if not succ:
                                        raise ValueError(f"Failed to complete upload {req['upload_id']}")

                                do_parallel(
                                    complete_upload,
                                    self.multipart_upload_requests,
                                    n=-1,
                                    desc="Completing multipart uploads",
                                    spinner=False,
                                )
                            return dict(
                                completed_chunk_ids=completed_chunk_ids,
                                total_runtime_s=total_runtime_s,
                                throughput_gbits=throughput_gbits,
                                monitor_status="completed",
                            )
                        elif time_limit_seconds is not None and t.elapsed > time_limit_seconds or t.elapsed > 600 and completed_bytes == 0:
                            logger.error("Transfer timed out without progress, please check the debug log!")
                            logger.fs.error("Transfer timed out! Please retry.")
                            logger.error(f"Please share debug logs from: {self.transfer_dir}")
                            return dict(
                                completed_chunk_ids=completed_chunk_ids,
                                total_runtime_s=total_runtime_s,
                                throughput_gbits=throughput_gbits,
                                monitor_status="timed_out",
                            )
                        else:
                            current_time = datetime.now()
                            if log_interval_s and (not last_log or (current_time - last_log).seconds > float(log_interval_s)):
                                last_log = current_time
                            time.sleep(0.01 if show_spinner else 0.25)
        # always run cleanup, even if there's an exception
        finally:
            with Progress(
                SpinnerColumn(),
                TextColumn("Cleaning up after transfer{task.description}"),
                transient=True,
            ) as progress:
                cleanup_task = progress.add_task("", total=None)

                # get compression ratio information from destination gateways using "/api/v1/profile/compression"
                progress.update(cleanup_task, description=": Getting compression ratio information")
                total_sent_compressed, total_sent_uncompressed = 0, 0
                for gateway in {v for v in self.bound_nodes.values() if v.region_tag in source_regions}:
                    stats = self.http_pool.request("GET", f"{gateway.gateway_api_url}/api/v1/profile/compression")
                    if stats.status == 200:
                        stats = json.loads(stats.data.decode("utf-8"))
                        total_sent_compressed += stats.get("compressed_bytes_sent", 0)
                        total_sent_uncompressed += stats.get("uncompressed_bytes_sent", 0)
                compression_ratio = total_sent_compressed / total_sent_uncompressed if total_sent_uncompressed > 0 else 0
                if compression_ratio > 0:
                    logger.fs.info(f"Total compressed bytes sent: {total_sent_compressed / GB:.2f}GB")
                    logger.fs.info(f"Total uncompressed bytes sent: {total_sent_uncompressed / GB:.2f}GB")
                    logger.fs.info(f"Compression ratio: {compression_ratio}")
                    progress.console.print(f"[bold yellow]Compression saved {(1. - compression_ratio)*100.:.2f}% of egress fees")

                if copy_gateway_logs:

                    def copy_log(instance):
                        instance.run_command("sudo docker logs -t skyplane_gateway 2> /tmp/gateway.stderr > /tmp/gateway.stdout")
                        instance.download_file("/tmp/gateway.stdout", self.transfer_dir / f"gateway_{instance.uuid()}.stdout")
                        instance.download_file("/tmp/gateway.stderr", self.transfer_dir / f"gateway_{instance.uuid()}.stderr")

                    progress.update(cleanup_task, description=": Copying gateway logs")
                    do_parallel(copy_log, self.bound_nodes.values(), n=-1)
                if write_profile:
                    progress.update(cleanup_task, description=": Writing chunk profiles")
                    chunk_status_df = self.get_chunk_status_log_df()
                    (self.transfer_dir / "chunk_status_df.csv").write_text(chunk_status_df.to_csv(index=False))
                    traceevent = status_df_to_traceevent(chunk_status_df)
                    profile_out = self.transfer_dir / f"traceevent_{uuid.uuid4()}.json"
                    profile_out.parent.mkdir(parents=True, exist_ok=True)
                    profile_out.write_text(json.dumps(traceevent))
                if write_socket_profile:

                    def write_socket_profile(instance):
                        receiver_reply = self.http_pool.request("GET", f"{instance.gateway_api_url}/api/v1/profile/socket/receiver")
                        text = receiver_reply.data.decode("utf-8")
                        if receiver_reply.status != 200:
                            logger.fs.error(
                                f"Failed to get receiver socket profile from {instance.gateway_api_url}: {receiver_reply.status} {text}"
                            )
                        (self.transfer_dir / f"receiver_socket_profile_{instance.uuid()}.json").write_text(text)

                    progress.update(cleanup_task, description=": Writing socket profiles")
                    do_parallel(write_socket_profile, self.bound_nodes.values(), n=-1)
                if cleanup_gateway:

                    def fn(s: Server):
                        try:
                            self.http_pool.request("POST", f"{s.gateway_api_url}/api/v1/shutdown")
                        except:
                            return  # ignore connection errors since server may be shutting down

                    do_parallel(fn, self.bound_nodes.values(), n=-1)
                    progress.update(cleanup_task, description=": Shutting down gateways")

    def verify_transfer(self, job: ReplicationJob):
        """Check that all objects to copy are present in the destination"""
        src_interface = ObjectStoreInterface.create(job.source_region, job.source_bucket)
        dst_interface = ObjectStoreInterface.create(job.dest_region, job.dest_bucket)

        # only check metadata (src.size == dst.size) && (src.modified <= dst.modified)
        def verify(tup):
            src_key, dst_key = tup[0].key, tup[1].key
            try:
                if src_interface.get_obj_size(src_key) != dst_interface.get_obj_size(dst_key):
                    return False
                elif src_interface.get_obj_last_modified(src_key) > dst_interface.get_obj_last_modified(dst_key):
                    return False
                else:
                    return True
            except NoSuchObjectException:
                return False

        # verify that all objects in src_interface are present in dst_interface
        matches = do_parallel(verify, job.transfer_pairs, n=512, spinner=True, spinner_persist=True, desc="Verifying transfer")
        failed_src_objs = [src_key for (src_key, dst_key), match in matches if not match]
        if len(failed_src_objs) > 0:
            raise exceptions.TransferFailedException(
                f"{len(failed_src_objs)} objects failed verification",
                failed_src_objs,
            )


def refresh_instance_list(provider: CloudProvider, region_list: Iterable[str] = (), instance_filter=None, n=-1) -> Dict[str, List[Server]]:
    if instance_filter is None:
        instance_filter = {"tags": {"skyplane": "true"}}
    results = do_parallel(
        lambda region: provider.get_matching_instances(region=region, **instance_filter),
        region_list,
        spinner=True,
        n=n,
        desc="Querying clouds for active instances",
    )
    return {r: ilist for r, ilist in results if ilist}
