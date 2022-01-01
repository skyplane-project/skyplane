import itertools
from typing import List, Optional

import requests
from loguru import logger

from skylark.benchmark.utils import refresh_instance_list
from skylark.compute.aws.aws_cloud_provider import AWSCloudProvider
from skylark.compute.gcp.gcp_cloud_provider import GCPCloudProvider
from skylark.compute.server import Server, ServerState
from skylark.gateway.chunk_store import Chunk, ChunkRequest, ChunkRequestHop
from skylark.replicate.obj_store import S3Interface
from skylark.replicate.replication_plan import ReplicationJob, ReplicationTopology
from skylark.utils import PathLike, do_parallel, wait_for


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
        assert "Status: Downloaded newer image" in docker_out or "Status: Image is up to date" in docker_out, (docker_out, docker_err)

        # delete old gateway containers that are not self.gateway_docker_image
        server.run_command(f"sudo docker kill $(sudo docker ps -q)")
        server.run_command(f"sudo docker rm -f $(sudo docker ps -a -q)")

        # launch dozzle log viewer
        server.run_command(
            f"sudo docker run --name dozzle -d --volume=/var/run/docker.sock:/var/run/docker.sock -p 8888:8080 amir20/dozzle:latest --filter name=skylark_gateway"
        )

        # launch glances web interface to monitor CPU and memory usage
        server.run_command(
            f"sudo docker run --name glances -d -p 61208-61209:61208-61209 -e GLANCES_OPT='-w' -v /var/run/docker.sock:/var/run/docker.sock:ro --pid host nicolargo/glances:latest-full"
        )

        docker_run_flags = "-d --log-driver=local --ipc=host --network=host"
        # todo add other launch flags for gateway daemon
        gateway_daemon_cmd = (
            "/env/bin/python /pkg/skylark/gateway/gateway_daemon.py --debug --chunk-dir /dev/shm/skylark/chunks --outgoing-connections 8"
        )
        docker_launch_cmd = f"sudo docker run {docker_run_flags} --name skylark_gateway {self.gateway_docker_image} {gateway_daemon_cmd}"
        start_out, start_err = server.run_command(docker_launch_cmd)
        assert not start_err, f"Error starting gateway: {start_err}"
        container_id = start_out.strip()

        # wait for gateways to start (check status API)
        def is_ready():
            api_url = f"http://{server.public_ip}:8080/api/v1/status"
            try:
                return requests.get(api_url).json().get("status") == "ok"
            except Exception as e:
                return False

        try:
            wait_for(is_ready, timeout=10, interval=0.1)
        except Exception as e:
            logger.error(f"Gateway {server.instance_name} is not ready")
            logs, err = server.run_command(f"sudo docker logs skylark_gateway --tail=100")
            logger.error(f"Docker logs: {logs}\nerr: {err}")
            raise e

    def kill_gateway_instance(self, server: Server):
        logger.warning(f"Killing gateway container on {server.instance_name}")
        server.run_command("sudo docker kill $(sudo docker ps -q)")

    def deprovision_gateway_instance(self, server: Server):
        logger.warning(f"Deprovisioning gateway {server.instance_name}")
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
            # file_size_bytes = src_obj_interface.get_obj_size(obj)
            file_size_bytes = -1
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
                chunk_location_type="random_128MB",  # todo src_object_store
                src_object_store_region=src_instance.region_tag,
                src_object_store_bucket=job.source_bucket,
            )
            dst_path = ChunkRequestHop(
                hop_cloud_region=dst_instance.region_tag,
                hop_ip_address=dst_instance.public_ip,
                chunk_location_type="save_local",  # dst_object_store
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
            logger.debug(f"Sending {len(chunk_reqs)} chunk requests to {instance.public_ip}")
            body = [c.as_dict() for c in chunk_reqs]
            reply = requests.post(f"http://{instance.public_ip}:8080/api/v1/chunk_requests", json=body)
            if reply.status_code != 200:
                raise Exception(f"Failed to send chunk requests to gateway instance {instance.instance_name}: {reply.text}")
            return reply

        assert len(chunk_batches) == len(self.bound_paths)
        send_chunk_req(src_instance, chunk_batches[0])
