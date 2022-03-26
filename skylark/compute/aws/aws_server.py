from typing import Dict, Optional

import boto3
import paramiko
from skylark.compute.aws.aws_auth import AWSAuthentication
from skylark import key_root

from skylark.compute.server import Server, ServerState
from skylark.utils.cache import ignore_lru_cache


class AWSServer(Server):
    """AWS Server class to support basic SSH operations"""

    def __init__(self, region_tag, instance_id, log_dir=None):
        super().__init__(region_tag, log_dir=log_dir)
        assert self.region_tag.split(":")[0] == "aws"
        self.auth = AWSAuthentication()
        self.aws_region = self.region_tag.split(":")[1]
        self.instance_id = instance_id
        self.boto3_session = boto3.Session(region_name=self.aws_region)
        self.local_keyfile = key_root / "aws" / f"skylark-{self.aws_region}.pem"  # TODO: don't hardcode this.

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
        self.auth.get_boto3_resource("ec2", self.aws_region).instances.filter(InstanceIds=[self.instance_id]).terminate()

    def get_ssh_client_impl(self):
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(
            self.public_ip(),
            username="ec2-user",
            # todo generate keys with password "skylark"
            pkey=paramiko.RSAKey.from_private_key_file(str(self.local_keyfile)),
            look_for_keys=False,
            allow_agent=False,
            banner_timeout=200,
        )
        return client

    def get_ssh_cmd(self):
        return f"ssh -i {self.local_keyfile} ec2-user@{self.public_ip()}"
