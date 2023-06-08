import os
import shutil
import subprocess
from typing import Any, Iterator, Tuple, Dict, Optional, List
from skyplane.utils.fn import PathLike
from skyplane.compute.server import Server


class TestServer(Server):

    """Test Server runs a gateway container locally to simulate deployed gateways"""

    def __init__(
        self, region_tag, log_dir=None, auto_shutdown_timeout_minutes: Optional[int] = None, local_port=None, gateway_api_url=None
    ):
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
        result = subprocess.run(command, shell=True, capture_output=True, encoding="utf8")
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
        # shutil.copyfile(local_path, remote_path)

        # Create the necessary directories in the local path
        remote_directory = os.path.dirname(remote_path)
        if remote_directory != remote_path:
            os.makedirs(remote_directory, exist_ok=True)

        # Copy the file from the remote path to the local path
        shutil.copy2(local_path, remote_path)

    def write_file(self, content_bytes, remote_path):
        """Write a file on the server"""
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

    def instance_name(self):
        return self.region_tag
