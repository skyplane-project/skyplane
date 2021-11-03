import threading
from functools import lru_cache
from typing import Dict, List

from loguru import logger
import boto3

from skylark.server import AWSServer, Server


class CloudProvider:
    """CloudProvider allocates instances and queries regions."""

    ns = threading.local()

    def __init__(self):
        self.instance_prefix = "skylark"

    @property
    def region_list(self):
        raise NotImplementedError

    def get_instance_list(self, region) -> List[Server]:
        raise NotImplementedError

    def provision_instance(self, **kwargs) -> Server:
        raise NotImplementedError


class AWSCloudProvider(CloudProvider):
    """AWSCloudProvider allocates instances and queries regions."""

    def __init__(self):
        super().__init__()

    def get_boto3_resource(self, service_name, region):
        ns_key = f"boto3_resource_{region}_{service_name}"
        if not hasattr(self.ns, ns_key):
            setattr(self.ns, ns_key, boto3.resource(service_name, region_name=region))
        return getattr(self.ns, ns_key)

    def get_boto3_client(self, service_name, region):
        ns_key = f"boto3_client_{region}_{service_name}"
        if not hasattr(self.ns, ns_key):
            setattr(self.ns, ns_key, boto3.client(service_name, region_name=region))
        return getattr(self.ns, ns_key)

    def get_ubuntu_ami_id(self, region):
        client = self.get_boto3_client("ec2", region)
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

    @property
    def region_list(self):
        regions = [
            "us-east-1",
            "us-east-2",
            "us-west-1",
            "us-west-2",
            "ap-northeast-1",
            "ap-northeast-2",
            "ap-southeast-1",
            "ap-southeast-2",
            "eu-central-1",
            "eu-west-1",
            "eu-west-2",
            "sa-east-1",
        ]
        return regions

    @lru_cache(maxsize=None)
    def get_instance_list(self, region) -> List[AWSServer]:
        ec2 = self.get_boto3_resource("ec2", region)
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

    def get_matching_instances(self, region=None, instance_type=None, state=None, tags={"skylark": "true"}):
        if isinstance(region, str):
            region = [region]
        elif region is None:
            region = self.region_list

        matching_instances = []
        for r in region:
            instances = self.get_instance_list(r)
            for instance in instances:
                # logger.debug(f"Instance {instance}")
                # logger.debug(f"Checking instance {instance.instance_id}, {instance.instance_name}")
                if not (instance_type is None or instance_type == instance.instance_class):
                    continue
                if not (state is None or state == instance.instance_state):
                    continue
                # logger.debug(f"Instance tags = {instance.tags}")
                if not all(instance.tags.get(k, "") == v for k, v in tags.items()):
                    continue
                matching_instances.append(instance)
        return matching_instances

    def provision_instance(self, region, ami_id, instance_class, name, tags={"skylark": "true"}) -> AWSServer:
        ec2 = boto3.resource("ec2", region_name=region)
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
        return AWSServer(f"aws:{region}", instance[0].id)

