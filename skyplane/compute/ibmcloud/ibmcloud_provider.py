from multiprocessing import BoundedSemaphore
from typing import List, Optional

from skyplane.compute.ibmcloud.ibmcloud_auth import IBMCloudAuthentication
from skyplane.compute.ibmcloud.ibmcloud_server import IBMCloudServer
from skyplane.compute.ibmcloud.ibm_gen2.vpc_backend import IBMVPCBackend
from skyplane.compute.ibmcloud.ibm_gen2.config import load_config, REGIONS

from skyplane.compute.cloud_provider import CloudProvider
from skyplane.utils import imports


class IBMCloudProvider(CloudProvider):
    def __init__(
        self,
        key_prefix: str = "skyplane",
        auth: Optional[IBMCloudAuthentication] = None,
    ):
        super().__init__()
        self.key_prefix = key_prefix
        self.auth = auth if auth else IBMCloudAuthentication()
        self.regions_vpc = {}
        self.provisioning_semaphore = BoundedSemaphore(16)

    @property
    def name(self):
        return "ibmcloud"

    @staticmethod
    def region_list() -> List[str]:
        return REGIONS

    def setup_global(self, iam_name: str = "skyplane_gateway", attach_policy_arn: Optional[str] = None):
        # Not sure this should execute something. We will create VPC per region
        pass

    def setup_region(self, region: str):
        # set up VPC per region? With net, subnets, floating ip, etc. ?
        ibm_vpc_config = {
            "ibm": {"iam_api_key": self.auth.iam_api_key, "user_agent": self.auth.user_agent},
            "ibm_gen2": {"region": region, "resource_group_id": self.auth.ibmcloud_resource_group_id},
        }
        load_config(ibm_vpc_config)
        ibm_vpc_backend = IBMVPCBackend(ibm_vpc_config["ibm_gen2"])
        ibm_vpc_backend.init()
        self.regions_vpc[region] = ibm_vpc_backend

    def teardown_region(self, region):
        if region in self.regions_vpc:
            self.regions_vpc[region].clean(all=True)

    def teardown_global(self):
        for region in self.regions_vpc:
            self.regions_vpc[region].clean(all=True)

    def add_ips_to_security_group(self, cos_region: str, ips: Optional[List[str]] = None):
        return self.regions_vpc[cos_region].add_ips_to_security_group(ips)

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

        tags["node-type"] = "master"
        tags["node-name"] = "skyplane-master"
        instance_id, vsi = self.regions_vpc[region].create_vpc_instance()
        return IBMCloudServer(self.regions_vpc[region], f"ibmcloud:{region}", instance_id, vsi)
