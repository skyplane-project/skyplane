from typing import List
import uuid

import boto3
from loguru import logger

from skylark.compute.aws.aws_server import AWSServer
from skylark.compute.cloud_providers import CloudProvider


class AWSCloudProvider(CloudProvider):
    def __init__(self):
        super().__init__()

    @property
    def name(self):
        return "aws"

    @staticmethod
    def region_list():
        return [
            "us-east-1",
            "us-east-2",
            "us-west-1",
            "us-west-2",
            "ap-northeast-1",
            # "ap-northeast-2",
            "ap-southeast-1",
            # "ap-southeast-2",
            "eu-central-1",
            "eu-west-1",
            # "eu-west-2",
            "sa-east-1",
        ]

    def get_instance_list(self, region) -> List[AWSServer]:
        ec2 = boto3.resource("ec2", region_name=region)
        instances = ec2.instances.filter(
            Filters=[
                {
                    "Name": "instance-state-name",
                    "Values": ["pending", "running", "stopped", "stopping"],
                }
            ]
        )
        instance_ids = [i.id for i in instances]
        instances = [AWSServer(f"aws:{region}", i) for i in instance_ids]
        return instances

    def add_ip_to_security_group(self, aws_region, security_group_id: str = None, ip="0.0.0.0/0", from_port=0, to_port=65535):
        """Add IP to security group. If security group ID is None, use default."""
        ec2 = AWSServer.get_boto3_resource("ec2", aws_region)
        if security_group_id is None:
            security_group_id = [i for i in ec2.security_groups.filter(GroupNames=["default"]).all()][0].id
        sg = ec2.SecurityGroup(security_group_id)
        matches_ip = lambda rule: len(rule["IpRanges"]) > 0 and rule["IpRanges"][0]["CidrIp"] == ip
        matches_ports = lambda rule: ("FromPort" not in rule and "ToPort" not in rule) or (
            rule["FromPort"] <= from_port and rule["ToPort"] >= to_port
        )
        if not any(rule["IpProtocol"] == "-1" and matches_ip(rule) and matches_ports(rule) for rule in sg.ip_permissions):
            sg.authorize_ingress(IpProtocol="-1", FromPort=from_port, ToPort=to_port, CidrIp=ip)
            logger.info(f"({aws_region}) Added IP {ip} to security group {security_group_id}")

    @staticmethod
    def get_ubuntu_ami_id(region):
        client = AWSServer.get_boto3_client("ec2", region)
        response = client.describe_images(
            Filters=[
                {
                    "Name": "name",
                    "Values": [
                        "ubuntu/images/hvm-ssd/ubuntu-bionic-18.04-amd64-server-*",
                    ],
                },
                {
                    "Name": "owner-id",
                    "Values": [
                        "099720109477",
                    ],
                },
            ]
        )
        if len(response["Images"]) == 0:
            raise Exception("No AMI found for region {}".format(region))
        else:
            # Sort the images by date and return the last one
            image_list = sorted(response["Images"], key=lambda x: x["CreationDate"], reverse=True)
            return image_list[0]["ImageId"]

    def provision_instance(self, region, instance_class, name=None, ami_id=None, tags={"skylark": "true"}) -> AWSServer:
        assert not region.startswith("aws:"), "Region should be AWS region"
        if name is None:
            name = f"skylark-aws-{str(uuid.uuid4()).replace('-', '')}"
        if ami_id is None:
            ami_id = self.get_ubuntu_ami_id(region)
        ec2 = AWSServer.get_boto3_resource("ec2", region)
        instance = ec2.create_instances(
            ImageId=ami_id,
            InstanceType=instance_class,
            MinCount=1,
            MaxCount=1,
            KeyName=region,
            TagSpecifications=[
                {
                    "ResourceType": "instance",
                    "Tags": [{"Key": "Name", "Value": name}] + [{"Key": k, "Value": v} for k, v in tags.items()],
                }
            ],
        )
        server = AWSServer(f"aws:{region}", instance[0].id)
        server.wait_for_ready()
        return server
