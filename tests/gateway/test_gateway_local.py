from typing import Any, Iterator, Tuple, Dict, Optional, List
import shutil
from skyplane.api.config import TransferConfig
import time
import docker
import argparse
from skyplane.obj_store.object_store_interface import ObjectStoreInterface, ObjectStoreObject
from skyplane.api.config import TransferConfig
from skyplane.utils.fn import PathLike
from skyplane.api.dataplane import Dataplane
import os
from skyplane.compute.server import Server
from skyplane.planner.planner import MulticastDirectPlanner
from skyplane.api.transfer_job import CopyJob, TestCopyJob
from skyplane.api.provisioner import Provisioner
import subprocess

"""

This test module creates local docker containers that run gateway code. 

For testing, we create an artificial region call "test", and create a TestInterface (the ObjectStore interface) that simulates interacting with an object store, and 
a "test:region" region tag that identifies artificial regions to deploy gateway docker containers to. 
"""


class TestServer(Server):

    """Test Server runs a gateway container locally to simulate deployed gateways"""

    def __init__(self, region_tag, log_dir=None, auto_shutdown_timeout_minutes: Optional[int] = None, local_port = None, gateway_api_url = None):
        print("CREATE TEST SERVER")
        super().__init__(region_tag, log_dir, auto_shutdown_timeout_minutes)
        self.command_log = []
        self.gateway_log_viewer_url = None
        self.gateway_api_url = None
        self.init_log_files(log_dir)
        self.ssh_tunnels: Dict = {}
        self.local_port = local_port
        self.gateway_api_url = f"http://127.0.0.1:{self.local_port}"

    def run_command(self, command) -> Tuple[str, str]:
        # execute command locally
        print("command:", command)
        result = subprocess.run(command, shell=True, capture_output=True, encoding='utf8')
        print("resutl", result, result.stderr, result.stdout)
        return result.stdout, result.stderr

    def download_file(self, remote_path, local_path):
        """Pretend to download a file from the server"""
        # Create the necessary directories in the local path
        local_directory = os.path.dirname(local_path)
        os.makedirs(local_directory, exist_ok=True)

        # Copy the file from the remote path to the local path
        shutil.copy2(remote_path, local_path)

    def upload_file(self, local_path, remote_path):
        """Pretend to upload a file to the server"""
        #shutil.copyfile(local_path, remote_path)

        # Create the necessary directories in the local path
        remote_directory = os.path.dirname(remote_path)
        if remote_directory != remote_path: 
            os.makedirs(remote_directory, exist_ok=True)

        # Copy the file from the remote path to the local path
        shutil.copy2(local_path, remote_path)

    def write_file(self, content_bytes, remote_path):
        """Write a file on the server"""
        print(remote_path)
        with open(remote_path, "wb") as f:
            f.write(content_bytes)

    def copy_public_key(self, pub_key_path: PathLike):
        """Pretend to append public key to authorized_keys file on server."""
        pass

    def uuid(self):
        return f"test:{self.region_tag}"

    def public_ip(self):
        return f"localhost:{self.local_port}"

    def private_ip(self):
        return f"localhost:{self.local_port}"


def wait_container_running(container_name):
    client = docker.from_env()
    while True:
        container = client.containers.get(container_name)
        print("container", container, container.status)
        if container.status == "running":
            return True
        print(f"Waiting for container {container_name} to start")
        time.sleep(1)
     
def remove_container_if_running(container_name): 
    client = docker.from_env()
    if check_container_running(container_name):
        container = client.containers.get(container_name)
        container.stop()
        container.remove()

def check_container_running(container_name):
    # TODO: also check image version that is being run 
    client = docker.from_env()
    try:
        container = client.containers.get(container_name)
        print("container", container, container.status)
        if container.status == "running":
            return True
        else:
            print("removing", container_name)
            container.remove()
            return False
    except docker.errors.NotFound:
        return False
 

class TestObject(ObjectStoreObject):
    def full_path(self):
        return f"test://{self.bucket}/{self.key}"

class TestInterface(ObjectStoreInterface):
    """Test interface simulates an object store interface"""

    def __init__(self, region_tag, bucket_name):
        self.bucket_name = bucket_name
        self._region_tag = region_tag
        self.provider = "test"

    def path(self):
        return f"{self.provider}://{self.bucket_name}"

    def bucket(self):
        return self.bucket_name
    
    def bucket_exists(self) -> bool:
        return True

    def region_tag(self):
        return self._region_tag
    
    def list_objects(self, prefix="") -> Iterator[Any]:
        for key in ["obj1", "obj2", "obj3"]: 
            obj = self.create_object_repr(key)
            obj.size = 100 
            yield obj

    def get_obj_size(self, obj_name) -> int:
        return 100

    def get_obj_last_modified(self, obj_name):
        return "2020-01-01"

    def get_obj_mime_type(self, obj_name):
        return None

    def download_object(
        self, src_object_name, dst_file_path, offset_bytes=None, size_bytes=None, write_at_offset=False, generate_md5: bool = False
    ) -> Tuple[Optional[str], Optional[bytes]]:
        return

    def upload_object(
        self,
        src_file_path,
        dst_object_name,
        part_number=None,
        upload_id=None,
        check_md5: Optional[bytes] = None,
        mime_type: Optional[str] = None,
    ):
        return

    def delete_objects(self, keys: List[str]):
        return

    def initiate_multipart_upload(self, dst_object_name: str, mime_type: Optional[str] = None) -> str:
        return

    def complete_multipart_upload(self, dst_object_name: str, upload_id: str) -> None:
        return
    
    def create_object_repr(self, key: str) -> TestObject:
        return TestObject(provider="test", bucket=self.bucket_name, key=key)

def run(gateway_docker_image, restart_gateways):
    #job = TestCopyJob("test://source", ["test://dest"], recursive=True)
    #job._src_iface = TestInterface("test:source_region", "source")
    #job._dst_ifaces = [TestInterface("test:dest_region", "dest")]

    #print("PREFIX", job.src_prefix, job.dst_prefixes)

    # this does not work since docker bridge network cannot access internet
    job = CopyJob("s3://feature-store-datasets/yahoo/processed_yahoo/A1/", ["gs://38046a6749df436886491a95cacdebb8/yahoo/"], recursive=True)

    topology = MulticastDirectPlanner(1, 64, TransferConfig()).plan([job])
    print([g.region_tag for g in topology.get_gateways()])
    #print(topology.generate_gateway_program("test:source"))

    source_server = TestServer(topology.src_region_tag, local_port=8081)
    dest_server = TestServer(topology.dest_region_tags[0], local_port=8082)

    test_log_dir = "test_logs/"

    # bind dataplane to docker servers
    dataplane = Dataplane("test", topology, Provisioner("test"), transfer_config=TransferConfig(), log_dir=test_log_dir, local=True)
    dataplane.bound_nodes[topology.get_region_gateways(topology.src_region_tag)[0]] = source_server
    dataplane.bound_nodes[topology.get_region_gateways(topology.dest_region_tags[0])[0]] = dest_server
    print("GATEWAYS", topology.gateways)
    print("dataplane nodes", dataplane.bound_nodes)
    print("dest", topology.get_region_gateways(topology.dest_region_tags[0])[0])

    # set ip address for docker containers (for gateway info file generation)
    topology.set_ip_addresses(topology.get_region_gateways(topology.src_region_tag)[0].gateway_id, "skyplane_source", "skyplane_source")
    topology.set_ip_addresses(topology.get_region_gateways(topology.dest_region_tags[0])[0].gateway_id, "skyplane_dest", "skyplane_dest")

    # create docker container
    if restart_gateways or not check_container_running("skyplane_source"):
        if check_container_running("skyplane_source"):
            remove_container_if_running("skyplane_source")

        # TODO: set network field 
        dataplane._start_gateway(
            gateway_docker_image=gateway_docker_image,
            gateway_node=topology.get_region_gateways(topology.src_region_tag)[0],
            gateway_server=source_server,
            gateway_log_dir=test_log_dir,
            container_name="skyplane_source", 
            port=8081
        )
    if restart_gateways or not check_container_running("skyplane_dest"):
        if check_container_running("skyplane_dest"):
            remove_container_if_running("skyplane_dest")
        print("dest", topology.get_region_gateways(topology.dest_region_tags[0])[0])
        dataplane._start_gateway(
            gateway_docker_image=gateway_docker_image,
            gateway_node=topology.get_region_gateways(topology.dest_region_tags[0])[0],
            gateway_server=dest_server,
            gateway_log_dir=test_log_dir,
            container_name="skyplane_dest", 
            port=8082
        )

    
    dataplane.provisioned = True

    # dispatch chunks to source gateway
    try:
        dataplane.run([job])
    except Exception as e: 
        print("Transfer error, copying logs...")
        dataplane.copy_logs()


# check chunk status on destination gateway

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--docker-image", type=str, required=True)
    parser.add_argument("--restart-gateways", action="store_true", default=False)
    args = parser.parse_args()

    return_code = run(args.docker_image, args.restart_gateways)
    exit(return_code)
