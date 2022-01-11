import os
import threading
from pathlib import Path
from typing import Dict, Optional

import boto3
from oslo_concurrency import lockutils
import paramiko
from loguru import logger

from skylark import key_root
from skylark.compute.server import Server, ServerState
from skylark.utils.cache import ignore_lru_cache


class AWSServer(Server):
    """AWS Server class to support basic SSH operations"""

    ns = threading.local()

    def __init__(self, region_tag, instance_id, log_dir=None):
        super().__init__(region_tag, log_dir=log_dir)
        assert self.region_tag.split(":")[0] == "aws"
        self.aws_region = self.region_tag.split(":")[1]
        self.instance_id = instance_id
        self.boto3_session = boto3.Session(region_name=self.aws_region)
        self.local_keyfile = self.ensure_keyfile_exists(self.aws_region)

    def uuid(self):
        return f"{self.region_tag}:{self.instance_id}"

    @classmethod
    def get_boto3_session(cls, aws_region) -> boto3.Session:
        # cache in thead-local storage
        key = f"{aws_region}_boto3_session"
        if not hasattr(cls.ns, key):
            setattr(cls.ns, key, boto3.Session(region_name=aws_region))
        return getattr(cls.ns, key)

    @classmethod
    def get_boto3_resource(cls, service_name, aws_region=None):
        return cls.get_boto3_session(aws_region).resource(service_name, region_name=aws_region)

    @classmethod
    def get_boto3_client(cls, service_name, aws_region=None):
        return cls.get_boto3_session(aws_region).client(service_name, region_name=aws_region)

    def get_boto3_instance_resource(self):
        ec2 = AWSServer.get_boto3_resource("ec2", self.aws_region)
        return ec2.Instance(self.instance_id)

    @staticmethod
    def ensure_keyfile_exists(aws_region, prefix=key_root / "aws"):
        prefix = Path(prefix)
        key_name = f"skylark-{aws_region}"
        local_key_file = prefix / f"{key_name}.pem"

        @lockutils.synchronized(f"aws_keyfile_lock_{aws_region}", external=True, lock_path="/tmp/skylark_locks")
        def create_keyfile():
            if not local_key_file.exists():  # we have to check again since another process may have created it
                ec2 = AWSServer.get_boto3_resource("ec2", aws_region)
                ec2_client = AWSServer.get_boto3_client("ec2", aws_region)
                local_key_file.parent.mkdir(parents=True, exist_ok=True)
                # delete key pair from ec2 if it exists
                keys_in_region = set(p["KeyName"] for p in ec2_client.describe_key_pairs()["KeyPairs"])
                if key_name in keys_in_region:
                    logger.warning(f"Deleting key {key_name} in region {aws_region}")
                    ec2_client.delete_key_pair(KeyName=key_name)
                key_pair = ec2.create_key_pair(KeyName=f"skylark-{aws_region}")
                with local_key_file.open("w") as f:
                    f.write(key_pair.key_material)
                    f.flush()  # sometimes generates keys with zero bytes, so we flush to ensure it's written
                os.chmod(local_key_file, 0o600)

        if not local_key_file.exists():
            create_keyfile(key_name, local_key_file)
            logger.info(f"Created key file {local_key_file}")
        return local_key_file

    @ignore_lru_cache()
    def public_ip(self) -> str:
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
        return "STANDARD"

    def region(self):
        return self.aws_region

    def instance_state(self):
        return ServerState.from_aws_state(self.get_boto3_instance_resource().state["Name"])

    def __repr__(self):
        return f"AWSServer(region_tag={self.region_tag}, instance_id={self.instance_id})"

    def terminate_instance_impl(self):
        ec2 = AWSServer.get_boto3_resource("ec2", self.aws_region)
        ec2.instances.filter(InstanceIds=[self.instance_id]).terminate()

    def get_ssh_client_impl(self):
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(self.public_ip(), username="ubuntu", key_filename=str(self.local_keyfile), look_for_keys=False, allow_agent=False)
        return client
