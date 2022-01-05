import itertools
from typing import List, Optional

import requests
from loguru import logger

from skylark.benchmark.utils import refresh_instance_list
from skylark.compute.aws.aws_cloud_provider import AWSCloudProvider
from skylark.compute.gcp.gcp_cloud_provider import GCPCloudProvider
from skylark.compute.server import Server, ServerState
from skylark.gateway.chunk import Chunk, ChunkRequest, ChunkRequestHop
from skylark.obj_store.s3_interface import S3Interface
from skylark.replicate.replication_plan import ReplicationJob, ReplicationTopology
from skylark.utils import PathLike, Timer, do_parallel, wait_for


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
        self.init_clouds()
        self.bound_paths: Optional[List[List[Server]]] = None

    def init_clouds(self):
        """Initialize AWS and GCP clouds."""
        jobs = []
        if self.aws is not None:
            jobs.extend([lambda: self.aws.add_ip_to_security_group(r) for r in self.aws.region_list()])
        if self.gcp is not None:
            jobs.append(lambda: self.gcp.create_ssh_key())
            jobs.append(lambda: self.gcp.configure_default_network())
            jobs.append(lambda: self.gcp.configure_default_firewall())
        with Timer(f"Cloud SSH key initialization"):
            do_parallel(lambda fn: fn(), jobs)

    def provision_gateway_instance(self, region: str) -> Server:
        provider, subregion = region.split(":")
        if provider == "aws":
            assert self.aws is not None
            server = self.aws.provision_instance(subregion, self.aws_instance_class)
            logger.info(f"Provisioned AWS gateway {server.instance_id} in {server.region()} (ip = {server.public_ip()})")
        elif provider == "gcp":
            assert self.gcp is not None
            # todo specify network tier in ReplicationTopology
            server = self.gcp.provision_instance(subregion, self.gcp_instance_class, premium_network=self.gcp_use_premium_network)
            logger.info(f"Provisioned GCP gateway {server.instance_name()} in {server.region()} (ip = {server.public_ip()})")
        else:
            raise NotImplementedError(f"Unknown provider {provider}")
        return server

    def deprovision_gateway_instance(self, server: Server):
        logger.warning(f"Deprovisioning gateway {server.instance_name()}")
        server.terminate_instance()

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
            results = do_parallel(
                self.provision_gateway_instance,
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
        instances = [instance for path in self.bound_paths for instance in path]
        do_parallel(self.deprovision_gateway_instance, instances, n=len(instances))

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
        chunk_requests_sharded = {}
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

        # send chunk requests to source gateway
        for instance, chunk_requests in chunk_requests_sharded.items():
            logger.debug(f"Sending {len(chunk_requests)} chunk requests to {instance.public_ip()}")
            reply = requests.post(f"http://{instance.public_ip()}:8080/api/v1/chunk_requests", json=[cr.as_dict() for cr in chunk_requests])
            if reply.status_code != 200:
                raise Exception(f"Failed to send chunk requests to gateway instance {instance.instance_name()}: {reply.text}")
