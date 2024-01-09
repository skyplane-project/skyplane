# (C) Copyright Samsung SDS. 2023

#

# Licensed under the Apache License, Version 2.0 (the "License");

# you may not use this file except in compliance with the License.

# You may obtain a copy of the License at

#

#     http://www.apache.org/licenses/LICENSE-2.0

#

# Unless required by applicable law or agreed to in writing, software

# distributed under the License is distributed on an "AS IS" BASIS,

# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

# See the License for the specific language governing permissions and

# limitations under the License.
import time
import warnings
from typing import Optional
from pathlib import Path
from skyplane.utils import logger

from cryptography.utils import CryptographyDeprecationWarning

with warnings.catch_warnings():
    warnings.filterwarnings("ignore", category=CryptographyDeprecationWarning)
    import paramiko

from skyplane import exceptions
from skyplane.compute.server import Server, ServerState, key_root
from skyplane.compute.scp.scp_network import SCPNetwork
from skyplane.utils.fn import PathLike

from skyplane.compute.scp.scp_auth import SCPAuthentication
from skyplane.compute.scp.scp_utils import SCPClient


class SCPServer(Server):
    def __init__(
        self,
        region_tag: str,
        virtualServerName: str,
        # virtualServerId: Optional[str] = None,
        key_root: PathLike = key_root / "scp",
        virtualServerId: Optional[str] = None,
        log_dir: Optional[PathLike] = None,
        ssh_private_key=None,
        vpc_id=None,
    ):
        super().__init__(region_tag, log_dir=log_dir)
        self.auth = SCPAuthentication()
        self.network = SCPNetwork(self.auth)
        self.scp_client = SCPClient()

        assert self.region_tag.split(":")[0] == "scp", f"Region name doesn't match pattern scp:<region> {self.region_tag}"
        self.scp_region = self.region_tag.split(":")[1]
        self.virtualserver_name = virtualServerName
        # self.virtualserver_name = self.region_tag.split(":")[2] if virtualServerName is None else virtualServerName
        self.virtualserver_id = self.virtualserver_id() if virtualServerId is None else virtualServerId

        self.vpc_id = self.vpc_id() if vpc_id is None else vpc_id

        key_root = Path(key_root)
        key_root.mkdir(parents=True, exist_ok=True)
        if ssh_private_key is None:
            self.ssh_private_key = key_root / "scp_key"
        else:
            self.ssh_private_key = ssh_private_key

        self.internal_ip, self.external_ip = self._init_ips()

    def uuid(self):
        return f"{self.region_tag}:{self.virtualserver_name}"

    def _init_ips(self):
        internal_ip = self.network.get_vs_details(self.virtualserver_id)["ip"]
        external_ip = None
        nics = self.network.get_nic_details(self.virtualserver_id)
        for nic in nics:
            if nic["ip"] == internal_ip and nic["subnetType"] == "PUBLIC":
                # print(nic['natIp'])
                external_ip = nic["natIp"]
                break
        return internal_ip, external_ip

    def public_ip(self) -> str:
        return self.external_ip

    def private_ip(self):
        return self.internal_ip

    def instance_class(self):
        return self.network.get_vs_details(self.virtualserver_id)["serverType"]

    def instance_state(self):
        return ServerState.from_scp_state(self.network.get_vs_details(self.virtualserver_id)["virtualServerState"])

    def region(self):
        return self.scp_region

    def instance_name(self):
        return self.virtualserver_name

    def virtualserver_name(self):
        return self.virtualserver_name

    def virtualserver_id(self):
        url = f"/virtual-server/v2/virtual-servers?vitualServerName={self.virtualserver_name}"
        response = self.scp_client._get(url)
        return response[0]["virtualServerId"]

    def tags(self):
        url = f"/tag/v2/resources/{self.virtualserver_id}/tags"
        tags = self.scp_client._get(url)
        return {tag["tagKey"]: tag["tagValue"] for tag in tags} if tags else {}

    def network_tier(self):
        return "PREMIUM"

    def vpc_id(self):
        url = f"/virtual-server/v3/virtual-servers/{self.virtualserver_id}"
        response = self.scp_client._getDetail(url)
        return response["vpcId"]

    def terminate_instance_impl(self):
        self.network.terminate_instance(self.virtualserver_id)
        # pass

    def get_sftp_client(self):
        t = paramiko.Transport((self.public_ip(), 22))
        pkey = paramiko.RSAKey.from_private_key_file(str(self.ssh_private_key), password="test123$")
        t.connect(username="root", pkey=pkey)
        return paramiko.SFTPClient.from_transport(t)

    def get_ssh_client_impl(self):
        """Return paramiko client that connects to this instance."""
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
            ssh_client.connect(
                hostname=self.public_ip(),
                username="root",
                pkey=paramiko.RSAKey.from_private_key_file(str(self.ssh_private_key), password="test123$"),
                look_for_keys=False,
                banner_timeout=200,
            )
            return ssh_client
        except paramiko.AuthenticationException as e:
            raise exceptions.BadConfigException(
                f"Failed to connect to SCP server {self.uuid()}. Delete local SCP keys and retry: `rm -rf {key_root / 'scp'}`"
            ) from e

    def open_ssh_tunnel_impl(self, remote_port):
        import sshtunnel

        try:
            tunnel = sshtunnel.SSHTunnelForwarder(
                (self.public_ip(), 22),
                ssh_username="root",
                ssh_pkey=str(self.ssh_private_key),
                ssh_private_key_password="test123$",
                host_pkey_directories=[],
                local_bind_address=("127.0.0.1", 0),
                remote_bind_address=("127.0.0.1", remote_port),
            )
            tunnel.start()
            return tunnel
        except Exception as e:
            logger.error(f"Error opening SSH tunnel: {str(e)}")
            return None

    def get_ssh_cmd(self) -> str:
        return f"ssh -i {self.ssh_private_key} {'root'}@{self.public_ip()}"

    def install_docker(self):
        # print("install_docker in scp_server.py")
        try:
            return super().install_docker()
        except Exception as e:
            import re

            pid_pattern = re.compile(r"process (\d+) ")
            match_result = pid_pattern.search(str(e))
            if match_result:
                pid = match_result.group(1)
                self.run_command(f"sudo kill -9 {pid}; sudo rm /var/lib/dpkg/lock-frontend")
                # print(f"Killed process with PID {pid} on {self.region_tag}:{self.instance_name()}, {self.public_ip()}")
                logger.fs.debug(f"Killed process with PID {pid} on {self.region_tag}:{self.instance_name()}, {self.public_ip()}")
                time.sleep(1)
            else:
                if "sudo dpkg --configure -a" in str(e):
                    self.run_command("sudo dpkg --configure -a")
                    # print(f"Ran 'sudo dpkg --configure -a' on {self.region_tag}:{self.instance_name()}, {self.public_ip()}")
                    logger.fs.debug(f"Ran 'sudo dpkg --configure -a' on {self.region_tag}:{self.instance_name()}, {self.public_ip()}")
                    time.sleep(1)
                elif "metricbeat" in str(e):
                    # print(f"metricbeat error on {self.region_tag}:{self.instance_name()}, {self.public_ip()}")
                    logger.fs.debug(f"metricbeat on {self.region_tag}:{self.instance_name()}, {self.public_ip()}")
                    time.sleep(5)
                raise RuntimeError(f"Failed to install Docker on {self.region_tag}, {self.public_ip()}: error {e}")
            raise RuntimeError(f"Failed to install Docker on {self.region_tag}, {self.public_ip()}: error {e}")
