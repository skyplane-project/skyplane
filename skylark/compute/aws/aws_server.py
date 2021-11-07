import os
from functools import lru_cache

import boto3
import paramiko
from loguru import logger

from skylark.compute.server import Server, ServerState


class AWSServer(Server):
    """AWS Server class to support basic SSH operations"""

    def __init__(self, region_tag, instance_id, command_log_file=None):
        super().__init__(region_tag, command_log_file=command_log_file)
        assert self.region_tag.split(":")[0] == "aws"
        self.aws_region = self.region_tag.split(":")[1]
        self.instance_id = instance_id
        self.local_keyfile = self.make_keyfile()

    @classmethod
    def get_boto3_resource(cls, service_name, aws_region):
        """Get boto3 resource (cache in threadlocal)"""
        ns_key = f"boto3_resource_{service_name}_{aws_region}"
        if not hasattr(cls.ns, ns_key):
            setattr(
                cls.ns,
                ns_key,
                boto3.resource(service_name, region_name=aws_region),
            )
        return getattr(cls.ns, ns_key)

    @classmethod
    def get_boto3_client(cls, service_name, aws_region):
        """Get boto3 client (cache in threadlocal)"""
        ns_key = f"boto3_client_{service_name}_{aws_region}"
        if not hasattr(cls.ns, ns_key):
            setattr(
                cls.ns,
                ns_key,
                boto3.client(service_name, region_name=aws_region),
            )
        return getattr(cls.ns, ns_key)

    def make_keyfile(self):
        local_key_file = os.path.expanduser(f"~/.ssh/{self.aws_region}.pem")
        ec2 = AWSServer.get_boto3_resource("ec2", self.aws_region)
        if not os.path.exists(local_key_file):
            key_pair = ec2.create_key_pair(KeyName=self.aws_region)
            with open(local_key_file, "w") as f:
                f.write(key_pair.key_material)
            os.chmod(local_key_file, 0o600)
            logger.info(f"({self.aws_region}) Created keypair and saved to {local_key_file}")
        return local_key_file

    @property
    def public_ip(self):
        ec2 = AWSServer.get_boto3_resource("ec2", self.aws_region)
        instance = ec2.Instance(self.instance_id)
        return instance.public_ip_address

    @property
    @lru_cache(maxsize=1)
    def instance_class(self):
        ec2 = AWSServer.get_boto3_resource("ec2", self.aws_region)
        instance = ec2.Instance(self.instance_id)
        return instance.instance_type

    @property
    @lru_cache(maxsize=1)
    def tags(self):
        ec2 = AWSServer.get_boto3_resource("ec2", self.aws_region)
        instance = ec2.Instance(self.instance_id)
        return {tag["Key"]: tag["Value"] for tag in instance.tags}

    @property
    @lru_cache(maxsize=1)
    def instance_name(self):
        return self.tags.get("Name", None)

    @property
    @lru_cache(maxsize=1)
    def region(self):
        return self.aws_region

    @property
    def instance_state(self):
        ec2 = AWSServer.get_boto3_resource("ec2", self.aws_region)
        instance = ec2.Instance(self.instance_id)
        return ServerState.from_aws_state(instance.state["Name"])

    def __repr__(self):
        str_repr = f"AWSServer("
        str_repr += f"{self.region_tag}, "
        str_repr += f"{self.instance_id}, "
        str_repr += f"{self.command_log_file}"
        str_repr += f")"
        return str_repr

    def terminate_instance_impl(self):
        ec2 = AWSServer.get_boto3_resource("ec2", self.aws_region)
        ec2.instances.filter(InstanceIds=[self.instance_id]).terminate()
        logger.info(f"({self.aws_region}) Terminated instance {self.instance_id}")

    def get_ssh_client_impl(self):
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(self.public_ip, username="ubuntu", key_filename=self.local_keyfile)
        return client
