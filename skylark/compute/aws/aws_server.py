import os
from functools import lru_cache
from pathlib import Path

import boto3
import paramiko
from loguru import logger

from skylark.compute.server import Server, ServerState
from skylark import key_root


class AWSServer(Server):
    """AWS Server class to support basic SSH operations"""

    def __init__(self, region_tag, instance_id, log_dir=None):
        super().__init__(region_tag, log_dir=log_dir)
        assert self.region_tag.split(":")[0] == "aws"
        self.aws_region = self.region_tag.split(":")[1]
        self.instance_id = instance_id
        self.local_keyfile = self.make_keyfile(self.aws_region)

    def uuid(self):
        return f"{self.region_tag}:{self.instance_id}"

    @classmethod
    def get_boto3_session(cls, aws_region):
        ns_key = f"boto3_session_{aws_region}"
        if not hasattr(cls.ns, ns_key):
            setattr(cls.ns, ns_key, boto3.Session(region_name=aws_region))
        return getattr(cls.ns, ns_key)

    @classmethod
    def get_boto3_resource(cls, service_name, aws_region):
        """Get boto3 resource (cache in threadlocal)"""
        ns_key = f"boto3_resource_{service_name}_{aws_region}"
        if not hasattr(cls.ns, ns_key):
            session = cls.get_boto3_session(aws_region)
            resource = session.resource(service_name, region_name=aws_region)
            setattr(cls.ns, ns_key, resource)
        return getattr(cls.ns, ns_key)

    @classmethod
    def get_boto3_client(cls, service_name, aws_region):
        """Get boto3 client (cache in threadlocal)"""
        ns_key = f"boto3_client_{service_name}_{aws_region}"
        if not hasattr(cls.ns, ns_key):
            session = cls.get_boto3_session(aws_region)
            client = session.client(service_name, region_name=aws_region)
            setattr(cls.ns, ns_key, client)
        return getattr(cls.ns, ns_key)

    @staticmethod
    def make_keyfile(aws_region, prefix=key_root / "aws"):
        prefix = Path(prefix)
        key_name = f"skylark-{aws_region}"
        local_key_file = prefix / f"{key_name}.pem"
        ec2 = AWSServer.get_boto3_resource("ec2", aws_region)
        ec2_client = AWSServer.get_boto3_client("ec2", aws_region)
        if not local_key_file.exists():
            prefix.mkdir(parents=True, exist_ok=True)
            # delete key pair from ec2 if it exists
            keys_in_region = set(p["KeyName"] for p in ec2_client.describe_key_pairs()["KeyPairs"])
            if key_name in keys_in_region:
                logger.warning(f"Deleting key {key_name} in region {aws_region}")
                ec2_client.delete_key_pair(KeyName=key_name)
            key_pair = ec2.create_key_pair(KeyName=f"skylark-{aws_region}")
            with local_key_file.open("w") as f:
                f.write(key_pair.key_material)
            os.chmod(local_key_file, 0o600)
        return local_key_file

    @property
    def public_ip(self):
        ec2 = AWSServer.get_boto3_resource("ec2", self.aws_region)
        instance = ec2.Instance(self.instance_id)
        ip = instance.public_ip_address
        return ip

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
    def network_tier(self):
        return "STANDARD"

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

    def get_ssh_client_impl(self):
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(self.public_ip, username="ubuntu", key_filename=str(self.local_keyfile), look_for_keys=False, allow_agent=False)
        return client
