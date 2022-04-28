from datetime import datetime
import itertools
from functools import partial
import json
import pickle
import time
from typing import Dict, List, Optional, Tuple
import uuid

from skylark.replicate.profiler import status_df_to_traceevent
from skylark.utils import logger
from tqdm import tqdm
import pandas as pd
from skylark import GB, KB, MB, tmp_log_dir

from skylark.benchmark.utils import refresh_instance_list
from skylark.compute.aws.aws_cloud_provider import AWSCloudProvider
from skylark.compute.azure.azure_cloud_provider import AzureCloudProvider
from skylark.compute.gcp.gcp_cloud_provider import GCPCloudProvider
from skylark.compute.server import Server, ServerState
from skylark.chunk import Chunk, ChunkRequest, ChunkState
from skylark.replicate.replication_plan import ReplicationJob, ReplicationTopology, ReplicationTopologyGateway
from skylark.utils.net import retry_requests
from skylark.utils.utils import PathLike, Timer, do_parallel

from skylark.obj_store.object_store_interface import ObjectStoreInterface


class ReplicatorClient:
    def __init__(
        self,
        topology: ReplicationTopology,
        gateway_docker_image: str = "ghcr.io/skyplane-project/skyplane:latest",
        aws_instance_class: Optional[str] = "m5.4xlarge",  # set to None to disable AWS
        azure_instance_class: Optional[str] = "Standard_D2_v5",  # set to None to disable Azure
        gcp_instance_class: Optional[str] = "n2-standard-16",  # set to None to disable GCP
        gcp_use_premium_network: bool = True,
    ):
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

        # upload requests 
        self.multipart_upload_requests = []

    def provision_gateways(
        self, reuse_instances=False, log_dir: Optional[PathLike] = None, authorize_ssh_pub_key: Optional[PathLike] = None
    ):
        regions_to_provision = [node.region for node in self.topology.nodes]
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

        # init clouds
        jobs = []
        jobs.append(partial(self.aws.create_iam, attach_policy_arn="arn:aws:iam::aws:policy/AmazonS3FullAccess"))
        for r in set(aws_regions_to_provision):
            jobs.append(partial(self.aws.add_ip_to_security_group, r.split(":")[1]))
            jobs.append(partial(self.aws.ensure_keyfile_exists, r.split(":")[1]))
        if azure_regions_to_provision:
            jobs.append(self.azure.create_ssh_key)
            jobs.append(self.azure.set_up_resource_group)
        if gcp_regions_to_provision:
            jobs.append(self.gcp.create_ssh_key)
            jobs.append(self.gcp.configure_default_network)
            jobs.append(self.gcp.configure_default_firewall)
        with Timer(f"Cloud SSH key initialization"):
            do_parallel(lambda fn: fn(), jobs)

        # reuse existing AWS instances
        if reuse_instances:
            if self.aws.auth.enabled():
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

            if self.azure.auth.enabled():
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

            if self.gcp.auth.enabled():
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

        do_parallel(setup, set(itertools.chain(*instances_by_region.values())), n=-1)

        # bind instances to nodes
        for node in self.topology.nodes:
            self.bound_nodes[node] = instances_by_region[node.region].pop()

        with Timer("Install gateway package on instances"):
            args = []
            for node, server in self.bound_nodes.items():
                args.append((server, {self.bound_nodes[n].public_ip(): v for n, v in self.topology.get_outgoing_paths(node).items()}))
            do_parallel(lambda arg: arg[0].start_gateway(arg[1], gateway_docker_image=self.gateway_docker_image), args, n=-1)

    def deprovision_gateways(self):
        def deprovision_gateway_instance(server: Server):
            if server.instance_state() == ServerState.RUNNING:
                server.terminate_instance()
                logger.warning(f"Deprovisioned {server.uuid()}")

        instances = self.bound_nodes.values()
        logger.warning(f"Deprovisioning {len(instances)} instances")
        do_parallel(deprovision_gateway_instance, instances, n=-1)

    def run_replication_plan(self, job: ReplicationJob) -> ReplicationJob:
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
        gateway_ips: Dict[Server, str] = {s: s.public_ip() for s in self.bound_nodes.values()}

        # make list of chunks
        chunks = []
        obj_file_size_bytes = job.src_obj_sizes()
        idx = 0
        for (src_obj, dest_obj) in zip(job.src_objs, job.dest_objs):
            if obj_file_size_bytes:
                if job.random_chunk_size_mb: # split objects into sub-chunks
                    chunk_size_bytes = int(job.random_chunk_size_mb*1e6)
                    num_chunks = int(obj_file_size_bytes[src_obj]/chunk_size_bytes) + 1
                    print(f"splitting file into {num_chunks} chunks")

                    # TODO: figure out what to do with size limits

                    # TODO: only do if num_chunks > 1
                    obj_store_interface = ObjectStoreInterface.create(job.dest_region, job.dest_bucket)
                    upload_id = obj_store_interface.initiate_multipart_upload(dest_obj)

                    # add multipart upload request
                    self.multipart_upload_requests.append((job.dest_region, job.dest_bucket, upload_id, dest_obj))

                    offset = 0
                    part_num = 1
                    for chunk in range(num_chunks):
                        # size is min(chunk_size, remaining data)
                        file_size_bytes = min(chunk_size_bytes, obj_file_size_bytes[src_obj] - offset)
                        assert file_size_bytes > 0, f"File size <= 0 {file_size_bytes}"
                        chunks.append(Chunk(
                            src_key=src_obj, 
                            dest_key=dest_obj, 
                            chunk_id=idx, 
                            file_offset_bytes=offset, 
                            chunk_length_bytes=file_size_bytes, 
                            part_number=part_num,
                            upload_id=upload_id
                        ))

                        idx += 1
                        part_num += 1
                        offset += chunk_size_bytes

                else: # transfer entire object
                    file_size_bytes = obj_file_size_bytes[src_obj]
                    chunks.append(Chunk(src_key=src_obj, dest_key=dest_obj, chunk_id=idx, file_offset_bytes=0, chunk_length_bytes=file_size_bytes))
                    idx += 1
            else:
                file_size_bytes = job.random_chunk_size_mb * MB
                chunks.append(Chunk(src_key=src_obj, dest_key=dest_obj, chunk_id=idx, file_offset_bytes=0, chunk_length_bytes=file_size_bytes))
                idx += 1

        #print(f"Created {idx} chunks from {len(job.src_objs)}")

        # partition chunks into roughly equal-sized batches (by bytes)
        # iteratively adds chunks to the batch with the smallest size
        def partition(items: List[Chunk], n_batches: int) -> List[List[Chunk]]:
            batches = [[] for _ in range(n_batches)]
            items.sort(key=lambda c: c.chunk_length_bytes, reverse=True)
            for item in items:
                batch_sizes = [sum(b.chunk_length_bytes for b in bs) for bs in batches]
                batches[batch_sizes.index(min(batch_sizes))].append(item)
            return batches

        src_instances = [self.bound_nodes[n] for n in self.topology.source_instances()]
        chunk_batches = partition(chunks, len(src_instances))
        assert (len(chunk_batches) == (len(src_instances) - 1)) or (
            len(chunk_batches) == len(src_instances)
        ), f"{len(chunk_batches)} batches, expected {len(src_instances)}"
        for batch_idx, batch in enumerate(chunk_batches):
            logger.info(f"Batch {batch_idx} size: {sum(c.chunk_length_bytes for c in batch)} with {len(batch)} chunks")

        # make list of ChunkRequests
        chunk_requests_sharded: Dict[int, List[ChunkRequest]] = {}
        with Timer("Building chunk requests"):
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

        with Timer("Dispatch chunk requests"):
            # send chunk requests to start gateways in parallel
            def send_chunk_requests(args: Tuple[Server, List[ChunkRequest]]):
                hop_instance, chunk_requests = args
                ip = gateway_ips[hop_instance]
                logger.debug(f"Sending {len(chunk_requests)} chunk requests to {ip}")
                reply = retry_requests().post(f"http://{ip}:8080/api/v1/chunk_requests", json=[cr.as_dict() for cr in chunk_requests])
                if reply.status_code != 200:
                    raise Exception(f"Failed to send chunk requests to gateway instance {hop_instance.instance_name()}: {reply.text}")

            start_instances = list(zip(src_instances, chunk_requests_sharded.values()))
            do_parallel(send_chunk_requests, start_instances, n=-1)

        job.chunk_requests = [cr for crlist in chunk_requests_sharded.values() for cr in crlist]
        return job

    def get_chunk_status_log_df(self) -> pd.DataFrame:
        def get_chunk_status(args):
            node, instance = args
            reply = retry_requests().get(f"http://{instance.public_ip()}:8080/api/v1/chunk_status_log")
            if reply.status_code != 200:
                raise Exception(f"Failed to get chunk status from gateway instance {instance.instance_name()}: {reply.text}")
            logs = []
            for log_entry in reply.json()["chunk_status_log"]:
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

    def monitor_transfer(
        self,
        job: ReplicationJob,
        show_pbar=False,
        log_interval_s: Optional[float] = None,
        time_limit_seconds: Optional[float] = None,
        cleanup_gateway: bool = True,
        save_log: bool = True,
        write_profile: bool = True,
        copy_gateway_logs: bool = True,
        multipart: bool = True, # multipart object uploads/downloads
    ) -> Optional[Dict]:
        assert job.chunk_requests is not None
        total_bytes = sum([cr.chunk.chunk_length_bytes for cr in job.chunk_requests])
        last_log = None

        sinks = self.topology.sink_instances()
        sink_regions = set(s.region for s in sinks)

        try:
            with Timer() as t:
                with tqdm(
                    total=total_bytes * 8, desc="Replication", unit="bit", unit_scale=True, unit_divisor=KB, disable=not show_pbar
                ) as pbar:
                    log_fn = tqdm.write if show_pbar else logger.debug
                    while True:
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
                        pbar.update(completed_bytes * 8 - pbar.n)
                        total_runtime_s = (log_df.time.max() - log_df.time.min()).total_seconds()
                        throughput_gbits = completed_bytes * 8 / GB / total_runtime_s if total_runtime_s > 0 else 0.0
                        pbar.set_description(f"Replication: average {throughput_gbits:.2f}Gbit/s")

                        if len(completed_chunk_ids) == len(job.chunk_requests):
                            return dict(
                                completed_chunk_ids=completed_chunk_ids,
                                total_runtime_s=total_runtime_s,
                                throughput_gbits=throughput_gbits,
                                monitor_status="completed",
                            )
                        elif time_limit_seconds is not None and t.elapsed > time_limit_seconds or t.elapsed > 600 and completed_bytes == 0:
                            logger.error("Transfer timed out! Please retry.")
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
                                gbits_remaining = (total_bytes - completed_bytes) * 8 / GB
                                eta = int(gbits_remaining / throughput_gbits) if throughput_gbits > 0 else None
                                log_fn(
                                    f"{len(completed_chunk_ids)}/{len(job.chunk_requests)} chunks done ({completed_bytes / GB:.2f} / {total_bytes / GB:.2f}GB, {throughput_gbits:.2f}Gbit/s, ETA={str(eta) + 's' if eta is not None else 'unknown'})"
                                )
                            time.sleep(0.01 if show_pbar else 0.25)
        # always run cleanup, even if there's an exception
        finally:
            transfer_dir = tmp_log_dir / f"transfer_{datetime.now().isoformat()}"
            transfer_dir.mkdir(exist_ok=True, parents=True)
            if save_log:
                (transfer_dir / "job.pkl").write_bytes(pickle.dumps(job))
            if copy_gateway_logs:

                def copy_log(instance):
                    logger.info(f"Copying gateway logs from {instance.uuid()}")
                    instance.run_command("sudo docker logs -t skylark_gateway 2> /tmp/gateway.stderr > /tmp/gateway.stdout")
                    instance.download_file("/tmp/gateway.stdout", transfer_dir / f"gateway_{instance.uuid()}.stdout")
                    instance.download_file("/tmp/gateway.stderr", transfer_dir / f"gateway_{instance.uuid()}.stderr")

                do_parallel(copy_log, self.bound_nodes.values(), n=-1)
            if write_profile:
                chunk_status_df = self.get_chunk_status_log_df()
                (transfer_dir / "chunk_status_df.csv").write_text(chunk_status_df.to_csv(index=False))
                traceevent = status_df_to_traceevent(chunk_status_df)
                profile_out = transfer_dir / f"traceevent_{uuid.uuid4()}.json"
                profile_out.parent.mkdir(parents=True, exist_ok=True)
                profile_out.write_text(json.dumps(traceevent))
                logger.debug(f"Wrote profile to {profile_out}, visualize using `about://tracing` in Chrome")
            if cleanup_gateway:

                def fn(s: Server):
                    try:
                        retry_requests().post(f"http://{s.public_ip()}:8080/api/v1/shutdown")
                    except:
                        return  # ignore connection errors since server may be shutting down

                do_parallel(fn, self.bound_nodes.values(), n=-1)

            if multipart: 
                # Complete multi-part uploads
                for region, bucket, upload_id, key in self.multipart_upload_requests:
                    obj_store_interface = ObjectStoreInterface.create(region, bucket)
                    obj_store_interface.complete_multipart_upload(key, upload_id)
