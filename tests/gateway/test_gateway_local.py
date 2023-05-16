from typing import Tuple, Dict, Optional, List
from skyplane.obj_store.object_store_interface import ObjectStoreInterface
from skyplane.api.config import TransferConfig
from skyplane.utils.fn import PathLike
from skyplane.api.dataplane import Dataplane
import os
from skyplane.compute.server import Server
from skyplane.planner.planner import MulticastDirectPlanner
from skyplane.api.transfer_job import CopyJob
from skyplane.api.provisioner import Provisioner
import subprocess

"""

This test module creates local docker containers that run gateway code. 

For testing, we create an artificial region call "test", and create a TestInterface (the ObjectStore interface) that simulates interacting with an object store, and 
a "test:region" region tag that identifies artificial regions to deploy gateway docker containers to. 
"""


class TestServer(Server):

    """Test Server runs a gateway container locally to simulate deployed gateways"""

    def __init__(self, region_tag, log_dir=None, auto_shutdown_timeout_minutes: Optional[int] = None, local_port = None):
        self.region_tag = region_tag  # format provider:region
        self.auto_shutdown_timeout_minutes = None
        self.command_log = []
        self.gateway_log_viewer_url = None
        self.gateway_api_url = None
        self.init_log_files(log_dir)
        self.ssh_tunnels: Dict = {}
        self.local_port = local_port

    def run_command(self, command) -> Tuple[str, str]:
        # execute command locally
        print("command:", command)
        result = subprocess.run(command, shell=True, capture_output=True, encoding='utf8')
        print("resutl", result, result.stderr, result.stdout)
        return result.stdout, result.stderr

    def download_file(self, remote_path, local_path):
        """Pretend to download a file from the server"""
        pass

    def upload_file(self, local_path, remote_path):
        """Pretend to upload a file to the server"""
        return 

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
    


class TestInterface(ObjectStoreInterface):
    """Test interface simulates an object store interface"""

    def __init__(self, region_tag, bucket_name):
        self.bucket_name = bucket_name
        self._region_tag = region_tag
        self.provider = "test"

    def bucket(self):
        return self.bucket_name

    def region_tag(self):
        return self._region_tag

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


job = CopyJob("test://source", "test://dest")
job._src_iface = TestInterface("test:source_region", "source")
job._dst_ifaces = [TestInterface("test:dest_region", "dest")]

topology = MulticastDirectPlanner(1, 64).plan([job])
print([g.region_tag for g in topology.get_gateways()])
print(topology.generate_gateway_program("test:source"))

source_server = TestServer("test:source_region", local_port=8081)
dest_server = TestServer("test:dest_region", local_port=8082)

gateway_docker_image = "ghcr.io/sarahwooders/skyplane:local-2b6813cf236e4cf830e6bc00308eeb8e"
print("docker image", gateway_docker_image)
test_log_dir = "test_logs/"


dataplane = Dataplane("test", topology, Provisioner("test"), transfer_config=TransferConfig(), log_dir=test_log_dir, local=True)
dataplane.bound_nodes[topology.get_region_gateways("test:source_region")[0]] = source_server
dataplane.bound_nodes[topology.get_region_gateways("test:dest_region")[0]] = dest_server

# TODO: create local network 


# set ip address for docker containers (for gateway info file generation)
topology.set_ip_addresses(topology.get_region_gateways("test:source_region")[0].gateway_id, "skyplane_source", "skyplane_source")
topology.set_ip_addresses(topology.get_region_gateways("test:dest_region")[0].gateway_id, "skyplane_dest", "skyplane_dest")


# create docker container
# TODO: set network field 
dataplane._start_gateway(
    gateway_docker_image=gateway_docker_image,
    gateway_node=topology.get_region_gateways("test:source_region")[0],
    gateway_server=source_server,
    gateway_log_dir=test_log_dir,
    container_name="skyplane_source", 
    port=8081
)
dataplane._start_gateway(
    gateway_docker_image=gateway_docker_image,
    gateway_node=topology.get_region_gateways("test:dest_region")[0],
    gateway_server=dest_server,
    gateway_log_dir=test_log_dir,
    container_name="skyplane_dest", 
    port=8082
)

# dispatch chunks to source gateway


# check chunk status on destination gateway
