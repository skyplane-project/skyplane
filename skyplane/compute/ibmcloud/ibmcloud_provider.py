import json
import time
import uuid
import yaml
from multiprocessing import BoundedSemaphore
from typing import List, Optional

from skyplane import exceptions as skyplane_exceptions
from skyplane.compute.ibmcloud.ibmcloud_auth import IBMCloudAuthentication
from skyplane.compute.ibmcloud.ibmcloud_server import IBMCloudServer
from skyplane.compute.ibmcloud.gen2.main import create_vpc
from skyplane.compute.ibmcloud.gen2.vpc_node_provider import IBMVPCNodeProvider
from skyplane.compute.cloud_provider import CloudProvider
from skyplane.utils import imports, logger
from skyplane.utils.fn import do_parallel, wait_for


class IBMCloudProvider(CloudProvider):
    def __init__(self, key_prefix: str = "skyplane"):
        super().__init__()
        self.key_prefix = key_prefix
        self.auth = IBMCloudAuthentication()
        self.regions_vpc = {}
        self.regions_cloudprovider = {}
        self.provisioning_semaphore = BoundedSemaphore(16)

    @property
    def name(self):
        return "ibmcloud"

    @staticmethod
    def region_list() -> List[str]:
        print("Region list")

    @imports.inject("botocore.exceptions", pip_extra="ibmcloud")
    def get_instance_list(exceptions, self, region: str) -> List[IBMCloudServer]:
        ec2 = self.auth.get_boto3_resource("ec2", region)
        valid_states = ["pending", "running", "stopped", "stopping"]
        instances = ec2.instances.filter(Filters=[{"Name": "instance-state-name", "Values": valid_states}])
        try:
            instance_ids = [i.id for i in instances]
        except exceptions.ClientError as e:
            logger.error(f"error provisioning in {region}: {e}")
            return []
        return [IBMCloudServer(f"cos:{region}", i) for i in instance_ids]

    def setup_global(self, iam_name: str = "skyplane_gateway", attach_policy_arn: Optional[str] = None):
        # Not sure this should execute something. We will create VPC per refion
        pass

    def setup_region(self, region: str):
        # set up VPC per region? With net, subnets, floating ip, etc. ?
        vpc_config_file = create_vpc(iam_api_key=self.auth.iam_api_key, region=region)
        self.regions_vpc[region] = vpc_config_file
        config_dict = None
        with open(vpc_config_file) as f:
            config_dict = yaml.safe_load(f)

        # How to decide on the zone
        region_config = self.auth.get_region_config()[region]
        ibmcloud_provider = IBMVPCNodeProvider(
            self.auth.iam_api_key,
            self.auth.iam_endpoint,
            "skyplane",
            config_dict["provider"]["endpoint"],
            region_config["zones"][0]["zone_name"],
            config_dict,
        )
        self.regions_cloudprovider[region] = ibmcloud_provider

    def teardown_region(self, region):
        if region in self.regions_cloudprovider:
            self.regions_cloudprovider[region].delete_vpc()

    def teardown_global(self):
        for provider in self.regions_cloudprovider:
            provider.delete_vpc()

    def add_ips_to_security_group(self, cos_region: str, ips: Optional[List[str]] = None):
        pass

    def remove_ips_from_security_group(self, cos_region: str, ips: List[str]):
        pass

    @imports.inject("botocore.exceptions", pip_extra="ibmcloud")
    def provision_instance(
        exceptions,
        self,
        region: str,
        instance_class: str,
        zone_name: Optional[str] = None,
        name: Optional[str] = None,
        tags={"skyplane": "true"},
    ) -> IBMCloudServer:
        # provision VM in the region
        config_dict = None
        with open(self.regions_vpc[region]) as f:
            config_dict = yaml.safe_load(f)

        tags["node-type"] = "master"
        tags["node-name"] = "skyplane-master"

        resp = self.regions_cloudprovider[region].create_node(
            config_dict["available_node_types"]["ray_head_default"]["node_config"], tags, 1
        )
        return IBMCloudServer(self.regions_cloudprovider[region], f"cos:{region}", resp, self.regions_vpc[region])
