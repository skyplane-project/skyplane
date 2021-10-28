from typing import Dict, List
import boto3
import threading

from skylark.server import AWSServer

all_ec2_regions = list(
    set(
        ["us-east-1", "us-east-2", "us-west-1", "us-west-2", "eu-central-1", "sa-east-1", "eu-west-2"]
        + [
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
        ]
    )
)

class CloudProvider:
    """CloudProvider allocates instances and queries regions."""
    ns = threading.local()

    def __init__(self):
        instance_list = self.get_instance_list()

    @property
    def region_list(self):
        raise NotImplementedError
    
    def get_instance_list(self):
        raise NotImplementedError

class AWSCloudProvider(CloudProvider):
    """AWSCloudProvider allocates instances and queries regions."""

    def __init__(self):
        super().__init__()
    
    def get_boto3_resource(self, service_name, region):
        ns_key = f"boto3_{region}_{service_name}"
        if not hasattr(self.ns, ns_key):
            setattr(self.ns, ns_key, boto3.resource(service_name, region_name=region))
        return getattr(self.ns, ns_key)

    @property
    def region_list(self):
        return all_ec2_regions
    
    def get_instance_list(self) -> Dict[str, List[AWSServer]]:
        instance_ids = {}
        for region in all_ec2_regions:
            ec2 = self.get_boto3_resource("ec2", region)
            instance_ids[region] = [i.id for i in ec2.instances.all()]
        return {r: [AWSServer(f"aws:{r}", i) for i in ids] for r, ids in instance_ids.items()}
    
    def get_matching_instances(self, region=None, instance_type=None, name=None):
        matching_instances = []
        for region, instances in self.get_instance_list().items():
            for instance in instances:
                if region and region != instance.aws_region:
                    continue
                if instance_type and instance_type != instance.instance_type:
                    continue
                if name and name != instance.name:
                    continue
                matching_instances.append(instance)
