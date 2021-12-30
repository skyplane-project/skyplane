import concurrent.futures
import functools
import itertools
import atexit
import os
import signal
import sys
import tempfile
import time
from typing import List, Optional

import requests
from tqdm import tqdm
from loguru import logger
from skylark.benchmark.utils import refresh_instance_list
from skylark.compute.aws.aws_cloud_provider import AWSCloudProvider
from skylark.compute.gcp.gcp_cloud_provider import GCPCloudProvider
from skylark.compute.server import Server, ServerState
from skylark.gateway.chunk_store import Chunk, ChunkRequest, ChunkRequestHop
from skylark.replicate.obj_store import S3Interface
from skylark.replicate.replication_plan import ReplicationJob, ReplicationPlan, ReplicationTopology
from skylark.utils import PathLike, do_parallel, wait_for
from tqdm import trange


class ReplicatorClient:
    def __init__(
        self,
        topology: ReplicationTopology,
        gcp_project: str,
        gateway_docker_image: str = "ghcr.io/parasj/skylark:latest",
        aws_instance_class: str = "m5.4xlarge",
        gcp_instance_class: str = "n2-standard-16",
        gcp_use_premium_network: bool = True,
    ):
        self.topology = topology
        self.gateway_docker_image = gateway_docker_image
        self.aws_instance_class = aws_instance_class
        self.gcp_instance_class = gcp_instance_class
        self.gcp_use_premium_network = gcp_use_premium_network

        # provisioning
        self.aws = AWSCloudProvider()
        self.gcp = GCPCloudProvider(gcp_project)
        self.init_clouds()
        self.bound_paths: List[List[Server]] = None

    def init_clouds(self):
        """Initialize AWS and GCP clouds."""
        do_parallel(self.aws.add_ip_to_security_group, self.aws.region_list())
        self.gcp.create_ssh_key()
        self.gcp.configure_default_network()
        self.gcp.configure_default_firewall()

    def provision_gateway_instance(self, region: str) -> Server:
        provider, subregion = region.split(":")
        if provider == "aws":
            server = self.aws.provision_instance(subregion, self.aws_instance_class)
        elif provider == "gcp":
            server = self.gcp.provision_instance(subregion, self.gcp_instance_class, premium_network=self.gcp_use_premium_network)
        else:
            raise NotImplementedError(f"Unknown provider {provider}")
        logger.info(f"Provisioned gateway {server.instance_id} in {server.region}")
        return server

    def start_gateway_instance(self, server: Server):
        server.wait_for_ready()
        server.run_command("sudo apt-get update && sudo apt-get install -y iperf3")
        docker_installed = "Docker version" in server.run_command(f"sudo docker --version")[0]
        if not docker_installed:
            server.run_command("curl -fsSL https://get.docker.com -o get-docker.sh && sudo sh get-docker.sh")
        out, err = server.run_command("sudo docker run --rm hello-world")
        assert "Hello from Docker!" in out
        server.run_command("sudo docker kill $(docker ps -q)")
        docker_out, docker_err = server.run_command(f"sudo docker pull {self.gateway_docker_image}")
        assert "Status: Downloaded newer image" in docker_out or "Status: Image is up to date" in docker_out, docker_out

        # delete old gateway containers that are not self.gateway_docker_image
        out, err = server.run_command("sudo docker ps -a -q")
        for container_id in out.splitlines():
            container_image = server.run_command(f"sudo docker inspect --format='{{.Config.Image}}' {container_id}")[0]
            if container_image != self.gateway_docker_image:
                logger.info(f"Deleting old container {container_id}")
                server.run_command(f"sudo docker rm {container_id}")

        gateway_cmd = f"sudo docker run -d --rm --ipc=host --network=host {self.gateway_docker_image} /env/bin/python /pkg/skylark/gateway/gateway_daemon.py"
        server.run_command(gateway_cmd)

        # wait for gateways to start (check status API)
        def is_ready():
            api_url = f"http://{server.public_ip}:8080/api/v1/status"
            try:
                return requests.get(api_url).json().get("status") == "ok"
            except Exception as e:
                logger.error(f"Failed to check status of {server.instance_id}, {e}")
                return False

        wait_for(is_ready, timeout=60, interval=1)

    def kill_gateway_instance(self, server: Server):
        logger.warning(f"Killing gateway container on {server.instance_id}")
        server.run_command("sudo docker kill $(sudo docker ps -q)")

    def deprovision_gateway_instance(self, server: Server):
        logger.warning(f"Deprovisioning gateway {server.instance_id}")
        server.terminate_instance()

    def provision_gateways(
        self, reuse_instances=False, log_dir: Optional[PathLike] = None, authorize_ssh_pub_key: Optional[PathLike] = None
    ):
        regions_to_provision = [r for path in self.topology.paths for r in path]
        aws_regions_to_provision = [r for r in regions_to_provision if r.startswith("aws:")]
        gcp_regions_to_provision = [r for r in regions_to_provision if r.startswith("gcp:")]

        # reuse existing AWS instances
        if reuse_instances:
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
                    aws_regions_to_provision.remove(f"aws:{r}")

            # reuse existing GCP
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
                    gcp_regions_to_provision.remove(f"gcp:{r}")

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

        do_parallel(setup, itertools.chain(*instances_by_region.values()), n=-1, progress_bar=True, desc="Setting up gateways")

        # bind instances to paths
        bound_paths = []
        for path in self.topology.paths:
            bound_paths.append([instances_by_region[r].pop() for r in path])
        self.bound_paths = bound_paths

    def start_gateways(self):
        do_parallel(
            self.start_gateway_instance,
            list(itertools.chain(*self.bound_paths)),
            progress_bar=True,
            desc="Starting up gateways",
        )

    def deprovision_gateways(self):
        instances = [instance for path in self.bound_paths for instance in path]
        do_parallel(self.deprovision_gateway_instance, instances, n=len(instances))

    def kill_gateways(self):
        do_parallel(self.kill_gateway_instance, itertools.chain(*self.bound_paths))

    def run_replication_plan(self, job: ReplicationJob):
        # todo support more than one gateway instance per region
        assert len(self.topology.paths) == 1, f"Replication plan requires exactly one path but got {len(self.topology.paths)} paths"

        # todo support more than one direct path
        assert len(self.topology.paths[0]) == 2, f"Only two-hop replication is supported but {len(self.topology.paths[0])} hops found"

        # todo support GCP
        assert job.source_region.split(":")[0] == "aws", f"Only AWS is supported for now, got {job.source_region}"
        assert job.dest_region.split(":")[0] == "aws", f"Only AWS is supported for now, got {job.dest_region}"
        src_obj_interface = S3Interface(job.source_region.split(":")[1], job.source_bucket)

        src_instance = self.bound_paths[0][0]
        dst_instance = self.bound_paths[0][1]

        # make list of ChunkRequests
        chunk_reqs = []
        for idx, obj in enumerate(job.objs):
            # todo support multipart files
            # todo support multiple paths
            file_size_bytes = src_obj_interface.get_obj_size(obj)
            chunk = Chunk(
                key=obj,
                chunk_id=idx,
                file_offset_bytes=0,
                chunk_length_bytes=file_size_bytes,
                chunk_hash_sha256=None,
            )
            src_path = ChunkRequestHop(
                hop_cloud_region=src_instance.region_tag,
                hop_ip_address=src_instance.public_ip,
                chunk_location_type="src_object_store",
                src_object_store_region=src_instance.region_tag,
                src_object_store_bucket=job.source_bucket,
            )
            dst_path = ChunkRequestHop(
                hop_cloud_region=dst_instance.region_tag,
                hop_ip_address=dst_instance.public_ip,
                chunk_location_type="dst_object_store",
                dst_object_store_region=dst_instance.region_tag,
                dst_object_store_bucket=job.dest_bucket,
            )
            chunk_reqs.append(ChunkRequest(chunk=chunk, path=[src_path, dst_path]))

        # partition chunks into roughly equal-sized batches (by bytes)
        n_src_instances = len(self.bound_paths)
        chunk_lens = [c.chunk.chunk_length_bytes for c in chunk_reqs]
        approx_bytes_per_connection = sum(chunk_lens) / n_src_instances
        batch_bytes = 0
        chunk_batches = []
        current_batch = []
        for req in chunk_reqs:
            current_batch.append(req)
            batch_bytes += req.chunk.chunk_length_bytes
            if batch_bytes >= approx_bytes_per_connection and len(chunk_batches) < n_src_instances:
                chunk_batches.append(current_batch)
                batch_bytes = 0
                current_batch = []

        # add remaining chunks to the smallest batch by total bytes
        if current_batch:
            smallest_batch = min(chunk_batches, key=lambda b: sum([c.chunk.chunk_length_bytes for c in b]))
            smallest_batch.extend(current_batch)

        # send ChunkRequests to each gateway instance
        def send_chunk_req(instance: Server, chunk_reqs: List[ChunkRequest]):
            body = [c.as_dict() for c in chunk_reqs]
            reply = requests.post(f"http://{instance.public_ip}:8080/api/v1/chunk_requests/pending", json=body)
            if reply.status_code != 200:
                raise Exception(f"Failed to send chunk requests to gateway instance {instance.instance_id}: {reply.text}")
            return reply

        assert len(chunk_batches) == len(self.bound_paths)
        send_chunk_req(src_instance, chunk_batches[0])


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run a replication job")
    parser.add_argument("--src-region", default="aws:us-east-1", help="AWS region of source bucket")
    parser.add_argument("--dest-region", default="aws:us-west-1", help="AWS region of destination bucket")
    parser.add_argument("--key-prefix", default="/test/direct_replication", help="S3 key prefix for all objects")
    parser.add_argument("--chunk-size-mb", default=128, type=int, help="Chunk size in MB")
    parser.add_argument("--n-chunks", default=16, type=int, help="Number of chunks in bucket")
    parser.add_argument("--gcp-project", default="skylark-333700", help="GCP project ID")
    parser.add_argument("--gateway-docker-image", default="ghcr.io/parasj/skylark:main", help="Docker image for gateway instances")
    parser.add_argument("--aws-instance-class", default="m5.4xlarge", help="AWS instance class")
    parser.add_argument("--gcp-instance-class", default="n2-standard-16", help="GCP instance class")
    parser.add_argument("--copy-ssh-key", default=None, help="SSH public key to add to server")
    parser.add_argument("--log-dir", default=None, help="Directory to write instance SSH logs to")
    # parser.add_argument("--gcp-use-premium-network", store_true=True, help="Use GCP premium network")
    args = parser.parse_args()

    src_bucket, dst_bucket = f"skylark-{args.src_region.split(':')[1]}", f"skylark-{args.dest_region.split(':')[1]}"
    s3_interface_src = S3Interface(args.src_region.split(":")[1], src_bucket)
    s3_interface_dst = S3Interface(args.dest_region.split(":")[1], dst_bucket)
    s3_interface_src.create_bucket()
    s3_interface_dst.create_bucket()

    # logger.info("Deleting all objects in source bucket")
    # matching_src_keys = list(s3_interface_src.list_objects(prefix=args.key_prefix))
    # matching_dst_keys = list(s3_interface_dst.list_objects(prefix=args.key_prefix))
    # if matching_src_keys:
    #     logger.warning(f"Deleting objects from source bucket: {matching_src_keys}")
    #     s3_interface_src.delete_objects(matching_src_keys)
    # if matching_dst_keys:
    #     logger.warning(f"Deleting objects from destination bucket: {matching_dst_keys}")
    #     s3_interface_dst.delete_objects(matching_dst_keys)

    # # create test objects w/ random data
    # logger.info("Creating test objects")
    # obj_keys = []
    # futures = []
    # with tempfile.NamedTemporaryFile() as f:
    #     f.write(os.urandom(int(1e6 * args.chunk_size_mb)))
    #     f.seek(0)
    #     for i in trange(args.n_chunks):
    #         k = f"{args.key_prefix}/{i}"
    #         futures.append(s3_interface_src.upload_object(f.name, k))
    #         logger.info(f"Uploaded object {f.name} -> {k}")
    #         obj_keys.append(k)
    # concurrent.futures.wait(futures)
    obj_keys = [f"{args.key_prefix}/{i}" for i in range(args.n_chunks)]

    # define the replication job and topology
    topo = ReplicationTopology(paths=[[args.src_region, args.dest_region]])
    rc = ReplicatorClient(
        topo,
        gcp_project=args.gcp_project,
        gateway_docker_image=args.gateway_docker_image,
        aws_instance_class=args.aws_instance_class,
        gcp_instance_class=args.gcp_instance_class,
        # gcp_use_premium_network=args.gcp_use_premium_network,
    )

    # provision the gateway instances
    rc.provision_gateways(reuse_instances=True, log_dir=args.log_dir, authorize_ssh_pub_key=args.copy_ssh_key)
    rc.start_gateways()

    def exit_handler():
        logger.warning("Exiting, closing gateways")
        rc.kill_gateways()

    atexit.register(exit_handler)

    # run the replication job
    logger.info(f"Source gateway API endpoint: http://{rc.bound_paths[0][0].public_ip}:8080/api/v1")
    logger.info(f"Destination gateway API endpoint: http://{rc.bound_paths[0][1].public_ip}:8080/api/v1")
    job = ReplicationJob(
        source_region=args.src_region,
        source_bucket=src_bucket,
        dest_region=args.dest_region,
        dest_bucket=dst_bucket,
        objs=obj_keys,
    )
    rc.run_replication_plan(job)

    # monitor the replication job until it is complete
    with tqdm(total=args.n_chunks * args.chunk_size_mb, unit="MB", desc="Replication progress") as pbar:
        while True:
            dst_objs = list(s3_interface_dst.list_objects(prefix=args.key_prefix))
            pbar.update(len(dst_objs) * args.chunk_size_mb - pbar.n)
            if len(dst_objs) == args.n_chunks:
                break
            time.sleep(0.5)

    # deprovision the gateway instances
    logger.info("Deprovisioning gateway instances")
    rc.deprovision_gateways()
