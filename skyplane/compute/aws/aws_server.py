import logging
from typing import Dict, Optional

import warnings
from cryptography.utils import CryptographyDeprecationWarning

with warnings.catch_warnings():
    warnings.filterwarnings("ignore", category=CryptographyDeprecationWarning)
    import paramiko

from skyplane import key_root, exceptions
from skyplane.compute.aws.aws_auth import AWSAuthentication
from skyplane.compute.server import Server, ServerState
from skyplane.utils.cache import ignore_lru_cache
from skyplane.utils import imports


class AWSServer(Server):
    """AWS Server class to support basic SSH operations"""

    def __init__(self, region_tag, instance_id, log_dir=None):
        super().__init__(region_tag, log_dir=log_dir)
        assert self.region_tag.split(":")[0] == "aws"
        self.auth = AWSAuthentication()
        self.aws_region = self.region_tag.split(":")[1]
        self.instance_id = instance_id
        self.local_keyfile = key_root / "aws" / f"skyplane-{self.aws_region}.pem"

    @property
    @imports.inject("boto3", pip_extra="aws")
    def boto3_session(boto3, self):
        if not hasattr(self, "_boto3_session"):
            self._boto3_session = self._boto3_session = boto3.Session(region_name=self.aws_region)
        return self._boto3_session

    def uuid(self):
        return f"{self.region_tag}:{self.instance_id}"

    def get_boto3_instance_resource(self):
        ec2 = self.auth.get_boto3_resource("ec2", self.aws_region)
        return ec2.Instance(self.instance_id)

    @ignore_lru_cache()
    def public_ip(self) -> str:
        # todo maybe eventually support VPC peering?
        return self.get_boto3_instance_resource().public_ip_address

    @ignore_lru_cache()
    def private_ip(self) -> str:
        return self.get_boto3_instance_resource().private_ip_address

    @ignore_lru_cache()
    def instance_class(self) -> str:
        return self.get_boto3_instance_resource().instance_type

    @ignore_lru_cache(ignored_value={})
    def tags(self) -> Dict[str, str]:
        tags = self.get_boto3_instance_resource().tags
        return {tag["Key"]: tag["Value"] for tag in tags} if tags else {}

    @ignore_lru_cache()
    def instance_name(self) -> Optional[str]:
        return self.tags().get("Name", None)

    def network_tier(self):
        return "PREMIUM"

    def region(self):
        return self.aws_region

    def instance_state(self):
        return ServerState.from_aws_state(self.get_boto3_instance_resource().state["Name"])

    def __repr__(self):
        return f"AWSServer(region_tag={self.region_tag}, instance_id={self.instance_id})"

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
                username="ec2-user",
                # todo generate keys with password "skyplane"
                pkey=paramiko.RSAKey.from_private_key_file(str(self.local_keyfile)),
                look_for_keys=False,
                allow_agent=False,
                banner_timeout=200,
            )
            return client
        except paramiko.AuthenticationException as e:
            raise exceptions.BadConfigException(
                f"Failed to connect to AWS server {self.uuid()}. Delete local AWS keys and retry: `rm -rf {key_root / 'aws'}`"
            ) from e

    def get_sftp_client(self):
        t = paramiko.Transport((self.public_ip(), 22))
        t.connect(username="ec2-user", pkey=paramiko.RSAKey.from_private_key_file(str(self.local_keyfile)))
        return paramiko.SFTPClient.from_transport(t)

    def open_ssh_tunnel_impl(self, remote_port):
        import sshtunnel

        sshtunnel.DEFAULT_LOGLEVEL = logging.FATAL
        return sshtunnel.SSHTunnelForwarder(
            (self.public_ip(), 22),
            ssh_username="ec2-user",
            ssh_pkey=str(self.local_keyfile),
            local_bind_address=("127.0.0.1", 0),
            remote_bind_address=("127.0.0.1", remote_port),
        )

    def get_ssh_cmd(self):
        return f"ssh -i {self.local_keyfile} ec2-user@{self.public_ip()}"
