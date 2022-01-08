from functools import lru_cache
import uuid
from typing import List, Optional

import botocore
import pandas as pd
from loguru import logger

from skylark import skylark_root
from skylark.compute.aws.aws_server import AWSServer
from skylark.compute.cloud_providers import CloudProvider
from skylark.utils.utils import Timer


class AWSCloudProvider(CloudProvider):
    def __init__(self):
        super().__init__()

    @property
    def name(self):
        return "aws"

    @staticmethod
    def region_list():
        return [
            "ap-northeast-1",
            "ap-northeast-2",
            "ap-northeast-3",
            "ap-southeast-1",
            "ap-southeast-2",
            "ca-central-1",
            "eu-central-1",
            "eu-north-1",
            "eu-west-1",
            "eu-west-2",
            "eu-west-3",
            "us-east-1",
            "us-east-2",
            "us-west-1",
            "us-west-2",
            # "af-south-1",
            # "ap-south-1",
            # "ap-southeast-3",
            # "eu-south-1",
            # "me-south-1",
        ]

    @staticmethod
    def get_transfer_cost(src_key, dst_key):
        transfer_df = pd.read_csv(skylark_root / "profiles" / "aws_transfer_costs.csv").set_index(["src", "dst"])

        src_provider, src = src_key.split(":")
        dst_provider, dst = dst_key.split(":")

        assert src_provider == "aws"
        if dst_provider == "aws":
            if (src, dst) in transfer_df.index:
                return transfer_df.loc[src, dst]["cost"]
            else:
                logger.warning(f"No transfer cost found for {src_key} -> {dst_key}, using max of {src}")
                src_rows = transfer_df.loc[src]
                src_rows = src_rows[src_rows.index != "internet"]
                return src_rows.max()["cost"]
        elif dst_provider == "gcp":
            return transfer_df.loc[src, "internet"]["cost"]
        else:
            raise NotImplementedError

    def get_instance_list(self, region: str) -> List[AWSServer]:
        ec2 = AWSServer.get_boto3_resource("ec2", region)
        valid_states = ["pending", "running", "stopped", "stopping"]
        instances = ec2.instances.filter(Filters=[{"Name": "instance-state-name", "Values": valid_states}])
        instance_ids = [i.id for i in instances]
        return [AWSServer(f"aws:{region}", i) for i in instance_ids]

    def add_ip_to_security_group(
        self, aws_region: str, security_group_id: Optional[str] = None, ip="0.0.0.0/0", from_port=0, to_port=65535
    ):
        """Add IP to security group. If security group ID is None, use default."""
        ec2 = AWSServer.get_boto3_resource("ec2", aws_region)
        if security_group_id is None:
            security_group_id = [i for i in ec2.security_groups.filter(GroupNames=["default"]).all()][0].id
        sg = ec2.SecurityGroup(security_group_id)
        matches_ip = lambda rule: len(rule["IpRanges"]) > 0 and rule["IpRanges"][0]["CidrIp"] == ip
        matches_ports = lambda rule: ("FromPort" not in rule and "ToPort" not in rule) or (
            rule["FromPort"] <= from_port and rule["ToPort"] >= to_port
        )
        try:
            if not any(rule["IpProtocol"] == "-1" and matches_ip(rule) and matches_ports(rule) for rule in sg.ip_permissions):
                sg.authorize_ingress(IpProtocol="-1", FromPort=from_port, ToPort=to_port, CidrIp=ip)
                logger.info(f"({aws_region}) Added IP {ip} to security group {security_group_id}")
        except botocore.exceptions.ClientError as e:
            if str(e).endswith("already exists"):
                logger.warning(f"Error adding IPs to security group, {e}")
            else:
                raise e

    @lru_cache()
    def get_ubuntu_ami_id(self, region: str, store="hvm:ebs-ssd") -> str:
        client = AWSServer.get_boto3_resource("ec2", region)
        images = client.images.filter(
            Owners=["099720109477"], Filters=[{"Name": "name", "Values": ["ubuntu/images/hvm-ssd/ubuntu-focal-20.04-amd64-server-*"]}]
        )
        images = sorted(images, key=lambda i: i.creation_date, reverse=True)  # get newest image
        if len(images) == 0:
            raise Exception(f"No Ubuntu AMI found in {region}")
        return images[0].id

    def provision_instance(
        self,
        region: str,
        instance_class: str,
        name: Optional[str] = None,
        ami_id: Optional[str] = None,
        tags={"skylark": "true"},
        ebs_volume_size: int = 128,
    ) -> AWSServer:
        assert not region.startswith("aws:"), "Region should be AWS region"
        if name is None:
            name = f"skylark-aws-{str(uuid.uuid4()).replace('-', '')}"
        if ami_id is None:
            ami_id = self.get_ubuntu_ami_id(region)
        ec2 = AWSServer.get_boto3_resource("ec2", region)
        AWSServer.make_keyfile(region)

        logger.debug(f"[{region}] Provisioning instance w/ ami {ami_id}, instance class {instance_class}")
        # set instance storage to 128GB EBS
        instance = ec2.create_instances(
            ImageId=ami_id,
            InstanceType=instance_class,
            MinCount=1,
            MaxCount=1,
            KeyName=f"skylark-{region}",
            TagSpecifications=[
                {
                    "ResourceType": "instance",
                    "Tags": [{"Key": "Name", "Value": name}] + [{"Key": k, "Value": v} for k, v in tags.items()],
                }
            ],
            BlockDeviceMappings=[
                {
                    "DeviceName": "/dev/sda1",
                    "Ebs": {
                        "DeleteOnTermination": True,
                        "VolumeSize": ebs_volume_size,
                        "VolumeType": "gp2",
                    },
                }
            ],
        )
        server = AWSServer(f"aws:{region}", instance[0].id)
        logger.debug(f"[{region}] Started server {server}")
        server.wait_for_ready()
        return server
