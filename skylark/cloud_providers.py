import threading
from typing import Dict, List

import boto3

from skylark.server import AWSServer, Server


class CloudProvider:
    """CloudProvider allocates instances and queries regions."""

    ns = threading.local()

    def __init__(self):
        instance_list = self.get_instance_list()
        self.instance_prefix = "skylark"

    @property
    def region_list(self):
        raise NotImplementedError

    def get_instance_list(self) -> Dict[str, List[Server]]:
        raise NotImplementedError

    def provision_instance(self) -> Server:
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
            image_list = sorted(
                response["Images"], key=lambda x: x["CreationDate"], reverse=True
            )
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

    def get_instance_list(self) -> Dict[str, List[AWSServer]]:
        instance_ids = {}
        for region in self.region_list:
            ec2 = self.get_boto3_resource("ec2", region)
            instances = ec2.instances.filter(
                Filters=[
                    {
                        "Name": "instance-state-name",
                        "Values": ["running", "stopped", "stopping"],
                    }
                ]
            )
            instance_ids[region] = [i.id for i in instances]
        return {
            r: [AWSServer(f"aws:{r}", i) for i in ids]
            for r, ids in instance_ids.items()
        }

    def get_matching_instances(
            self, region=None, instance_type=None, state=None, tags={"skylark": "true"}
    ):
        matching_instances = []
        for region, instances in self.get_instance_list().items():
            for instance in instances:
                if not (region is None or region == instance.region):
                    continue
                if not (
                        instance_type is None or instance_type == instance.instance_class
                ):
                    continue
                if not (state is None or state == instance.instance_state):
                    continue
                if not all(instance.tags.get(k, "") == v for k, v in tags.items()):
                    continue
                matching_instances.append(instance)
        return matching_instances

    def provision_instance(
            self, region, ami_id, instance_class, name, other_tags={"skylark": "true"}
    ) -> AWSServer:
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
                    "Tags": [{"Key": "Name", "Value": name}]
                            + [{"Key": k, "Value": v} for k, v in other_tags.items()],
                }
            ],
        )
        return AWSServer(f"aws:{region}", instance[0].id)


if __name__ == "__main__":
    aws = AWSCloudProvider()
    running_instances = aws.get_matching_instances(
        state="running", tags={"skylark": "true"}
    )
    print(running_instances)

    # for region in aws.region_list:
    #     ami = aws.get_ubuntu_ami_id(region)
    #     aws.provision_instance(region, ami, "i3en.large", "skylark-test")

    grouped_by_region = {
        r: [i for i in instances if i.region == r]
        for r, instances in aws.get_instance_list().items()
    }
    for region, instances in grouped_by_region.items():
        print(f"{region}")
        for instance in instances:
            print(f"  {instance}")