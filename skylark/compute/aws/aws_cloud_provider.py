from typing import List

import boto3

from skylark.compute.aws.aws_server import AWSServer
from skylark.compute.cloud_providers import CloudProvider


class AWSCloudProvider(CloudProvider):
    def __init__(self):
        super().__init__()

    @property
    def region_list(self):
        return [
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
