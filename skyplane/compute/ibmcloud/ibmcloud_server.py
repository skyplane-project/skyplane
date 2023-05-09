import logging
import warnings

from cryptography.utils import CryptographyDeprecationWarning
from typing import Dict, Optional

with warnings.catch_warnings():
    warnings.filterwarnings("ignore", category=CryptographyDeprecationWarning)
    import paramiko

from skyplane.compute.server import Server, ServerState
from skyplane.utils import imports
from skyplane.utils.cache import ignore_lru_cache


class IBMCloudServer(Server):
    """IBM Cloud Server class to support basic SSH operations"""

    def __init__(self, vpc_backend, region_tag, instance_id, vsi, log_dir=None):
        super().__init__(region_tag, log_dir=log_dir)
        assert self.region_tag.split(":")[0] == "ibmcloud"
        self.vpc_backend = vpc_backend
        self.instance_id = instance_id
        self.vsi = vsi
        self.cos_region = self.region_tag.split(":")[1]

    @property
    @imports.inject("boto3", pip_extra="ibmcloud")
    def boto3_session(boto3, self):
        if not hasattr(self, "_boto3_session"):
            self._boto3_session = self._boto3_session = boto3.Session(region_name=self.cos_region)
        return self._boto3_session

    def uuid(self):
        return f"{self.region_tag}:{self.instance_id}"

    @ignore_lru_cache()
    def public_ip(self) -> str:
        # todo maybe eventually support VPC peering?
        public_ip = self.vsi.public_ip
        return public_ip

    @ignore_lru_cache()
    def private_ip(self) -> str:
        return self.vsi.private_ip

    @ignore_lru_cache()
    def instance_class(self) -> str:
        return self.vsi.instance_id

    @ignore_lru_cache(ignored_value={})
    def tags(self) -> Dict[str, str]:
        tags = self.ibmcloud_provider.node_tags(self.instance_id)
        print(f"ibmcloud server tags {tags}")
        return tags
        # return {tag["Key"]: tag["Value"] for tag in tags} if tags else {}

    @ignore_lru_cache()
    def instance_name(self) -> Optional[str]:
        return self.vsi.name

    def network_tier(self):
        return "PREMIUM"

    def region(self):
        return self.vpc_backend.region

    def instance_state(self):
        return ServerState.from_ibmcloud_state(self.vpc_backend.get_node_status(self.instance_id))

    def __repr__(self):
        return f"IBMCloudServer(region_tag={self.region_tag}, instance_id={self.instance_id})"

    def terminate_instance_impl(self):
        self.vsi.delete()

    def get_ssh_client_impl(self):
        return self.vsi.get_ssh_client()

    def get_sftp_client(self):
        t = paramiko.Transport((self.public_ip(), 22))
        t.connect(
            username=self.vsi.ssh_credentials["username"],
            pkey=paramiko.RSAKey.from_private_key_file(self.vsi.ssh_credentials["key_filename"]),
            # look_for_keys=False
        )
        return paramiko.SFTPClient.from_transport(t)

    def open_ssh_tunnel_impl(self, remote_port):
        import sshtunnel

        sshtunnel.DEFAULT_LOGLEVEL = logging.FATAL

        return sshtunnel.SSHTunnelForwarder(
            (self.public_ip(), 22),
            # ssh_username="ec2-user",
            ssh_username=self.vsi.ssh_credentials["username"],
            ssh_pkey=str(self.vsi.ssh_credentials["key_filename"]),
            host_pkey_directories=[],
            local_bind_address=("127.0.0.1", 0),
            remote_bind_address=("127.0.0.1", remote_port),
        )

    def get_ssh_cmd(self):
        return f"ssh -i {self.vsi.ssh_credentials['key_filename']} {self.vsi.ssh_credentials['username']}@{self.public_ip()}"
