import functools
import logging
import warnings
import os

from cryptography.utils import CryptographyDeprecationWarning
from typing import Dict, Optional

from skyplane.compute.aws.aws_key_manager import AWSKeyManager

with warnings.catch_warnings():
    warnings.filterwarnings("ignore", category=CryptographyDeprecationWarning)
    import paramiko

from skyplane import exceptions
from skyplane.compute.ibmcloud.ibmcloud_auth import IBMCloudAuthentication
from skyplane.compute.server import Server, ServerState, key_root
from skyplane.utils import imports
from skyplane.utils.cache import ignore_lru_cache


class IBMCloudServer(Server):
    """IBM Cloud Server class to support basic SSH operations"""

    def __init__(self, ibmcloud_provider, region_tag, vsi_info, log_dir=None):
        super().__init__(region_tag, log_dir=log_dir)
        assert self.region_tag.split(":")[0] == "cos"
        self.auth = IBMCloudAuthentication()
        self.ibmcloud_provider = ibmcloud_provider
        for key in vsi_info:
            val = vsi_info[key]
            self.instance_id = val['id']
            print (self.instance_id)
        self.vsi_info = vsi_info
        self.cos_region = self.region_tag.split(":")[1]
        key_filename_tmp = self.auth.ssh_credentials['key_filename']
        self.key_filename = os.path.abspath(os.path.expanduser(key_filename_tmp))


    @property
    @imports.inject("boto3", pip_extra="aws")
    def boto3_session(boto3, self):
        if not hasattr(self, "_boto3_session"):
            self._boto3_session = self._boto3_session = boto3.Session(region_name=self.aws_region)
        return self._boto3_session

    def uuid(self):
        return f"{self.region_tag}:{self.instance_id}"

    @ignore_lru_cache()
    def public_ip(self) -> str:
        # todo maybe eventually support VPC peering?
        public_ip = self.ibmcloud_provider.external_ip(self.instance_id)
        return public_ip

    @ignore_lru_cache()
    def private_ip(self) -> str:
        return self.ibmcloud_provider.internal_ip(self.instance_id)['address']

    @ignore_lru_cache()
    def instance_class(self) -> str:
        return self.vsi_info['profile']['name']

    @ignore_lru_cache(ignored_value={})
    def tags(self) -> Dict[str, str]:
        tags = self.ibmcloud_provider.node_tags(self.instance_id)
        print (tags)
        return tags
        #return {tag["Key"]: tag["Value"] for tag in tags} if tags else {}

    @ignore_lru_cache()
    def instance_name(self) -> Optional[str]:
        return self.tags().get("Name", None)

    def network_tier(self):
        return "PREMIUM"

    def region(self):
        return self.vsi_info['zone']['name']

    def instance_state(self):
        return ServerState.from_aws_state(self.get_boto3_instance_resource().state["Name"])

    def __repr__(self):
        return f"IBMCloudServer(region_tag={self.region_tag}, instance_id={self.instance_id})"

    def terminate_instance_impl(self):
        iam = self.auth.get_boto3_resource("iam")

        # get instance profile name that is associated with this instance
        profile = self.get_boto3_instance_resource().iam_instance_profile
        if profile:
            profile = iam.InstanceProfile(profile["Arn"].split("/")[-1])

            # remove all roles from instance profile
            for role in profile.roles:
                profile.remove_role(RoleName=role.name)

            # delete instance profile
            profile.delete()

        # delete instance
        self.get_boto3_instance_resource().terminate()

    def get_ssh_client_impl(self):
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
            client.connect(
                self.public_ip(),
                username=self.auth.ssh_credentials['username'],
                pkey=paramiko.RSAKey.from_private_key_file(self.key_filename),
                look_for_keys=False,
                allow_agent=False,
                banner_timeout=200,
            )
            return client
        except paramiko.AuthenticationException as e:
            raise exceptions.BadConfigException(
                f"Failed to connect to IBM Cloud server {self.uuid()}. Delete local IBM Cloud keys and retry: `rm -rf {key_root / 'ibmcloud'}`"
            ) from e

    def get_sftp_client(self):
        t = paramiko.Transport((self.public_ip(), 22))

        t.connect(username=self.auth.ssh_credentials['username'], 
            pkey=paramiko.RSAKey.from_private_key_file(self.key_filename))
        return paramiko.SFTPClient.from_transport(t)

    def open_ssh_tunnel_impl(self, remote_port):
        import sshtunnel

        sshtunnel.DEFAULT_LOGLEVEL = logging.FATAL

        return sshtunnel.SSHTunnelForwarder(
            (self.public_ip(), 22),
            # ssh_username="ec2-user",
            ssh_username=self.auth.ssh_credentials['username'],
            ssh_pkey=str(self.key_filename),
            local_bind_address=("127.0.0.1", 0),
            remote_bind_address=("127.0.0.1", remote_port),
        )

    def get_ssh_cmd(self):
        # return f"ssh -i {self.local_keyfile} ec2-user@{self.public_ip()}"
        return f"ssh -i {self.key_filename} {self.auth.ssh_credentials['username']}@{self.public_ip()}"
