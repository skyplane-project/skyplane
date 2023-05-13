from typing import Optional
from skyplane.config_paths import config_path, ibmcloud_config_path
from skyplane.config import SkyplaneConfig
from skyplane.utils import imports

PUBLIC_ENDPOINT = "https://s3.{}.cloud-object-storage.appdomain.cloud"
PRIVATE_ENDPOINT = "https://s3.private.{}.cloud-object-storage.appdomain.cloud"
DIRECT_ENDPOINT = "https://s3.direct.{}.cloud-object-storage.appdomain.cloud"

OBJ_REQ_RETRIES = 5
CONN_READ_TIMEOUT = 10
VPC_API_VERSION = "2021-09-21"


class IBMCloudAuthentication:
    def __init__(self, config: Optional[SkyplaneConfig] = None):
        """Loads IBM Cloud authentication details. If no access key is provided, it will try to load credentials using boto3"""
        if not config is None:
            self.config = config
        else:
            self.config = SkyplaneConfig.load_config(config_path)

        self.user_agent = self.config.ibmcloud_useragent if self.config.ibmcloud_useragent is not None else "skyplane-ibm"
        self._ibmcloud_resource_group_id = self.config.ibmcloud_resource_group_id

        if self.config.ibmcloud_access_id and self.config.ibmcloud_secret_key:
            self._access_key = self.config.ibmcloud_access_id
            self._secret_key = self.config.ibmcloud_secret_key
        if self.config.ibmcloud_iam_key:
            self._iam_key = self.config.ibmcloud_iam_key

    @imports.inject("ibm_cloud_sdk_core", "ibm_cloud_sdk_core.authenticators", pip_extra="ibmcloud")
    def get_iam_authenticator(ibm_cloud_sdk_core, self):
        return ibm_cloud_sdk_core.authenticators.IAMAuthenticator(self.config.ibmcloud_iam_key, url=self.config.ibmcloud_iam_endpoint)

    def get_ibmcloud_endpoint(self, region, compute_backend="public"):
        if region is not None:
            endpoint = PUBLIC_ENDPOINT.format(region)

            if compute_backend == "ibm_vpc":
                endpoint = DIRECT_ENDPOINT.format(region)

            return endpoint

    @imports.inject("ibm_cloud_sdk_core", "ibm_vpc", pip_extra="ibmcloud")
    def save_region_config(ibm_cloud_sdk_core, ibm_vpc, self, config: SkyplaneConfig):
        if not config.ibmcloud_enabled:
            self.clear_region_config()
            return
        with ibmcloud_config_path.open("w") as f:
            region_list = []
            authenticator = ibm_cloud_sdk_core.authenticators.IAMAuthenticator(config.ibmcloud_iam_key, url=config.ibmcloud_iam_endpoint)
            ibm_vpc_client = ibm_vpc.VpcV1(VPC_API_VERSION, authenticator=authenticator)
            res = ibm_vpc_client.list_regions()
            for region in res.result["regions"]:
                if region["status"] == "available":
                    zones = ibm_vpc_client.list_region_zones(region["name"])
                    for zone in zones.result["zones"]:
                        if zone["status"] == "available":
                            region_list.append("{},{},{},{}".format(zone["region"]["name"], region["href"], zone["name"], zone["href"]))
            f.write("\n".join(region_list))

    def clear_region_config(self):
        with ibmcloud_config_path.open("w") as f:
            f.write("")

    @staticmethod
    def get_region_config():
        try:
            f = open(ibmcloud_config_path, "r")
        except FileNotFoundError:
            return []
        region_list = {}
        for region in f.read().split("\n"):
            line = region.split(",")
            if line[0] not in region_list:
                region_list[line[0]] = {}
                region_list[line[0]]["zones"] = []
            region_list[line[0]]["href"] = line[1]
            region_list[line[0]]["zones"].append({"zone_name": line[2], "zone_href": line[3]})

        return region_list

    @property
    def access_key(self):
        return self._access_key

    @property
    def iam_api_key(self):
        return self._iam_key

    @property
    def iam_endpoint(self):
        return self.config.ibmcloud_iam_endpoint

    @property
    def ibmcloud_resource_group_id(self):
        return self._ibmcloud_resource_group_id

    @property
    def secret_key(self):
        return self._secret_key

    def enabled(self):
        return self.config.ibmcloud_enabled

    @imports.inject("ibm_boto3", pip_extra="ibmcloud")
    def get_boto3_session(ibm_boto3, self, cos_region: Optional[str] = None):
        return ibm_boto3.Session(aws_access_key_id=self.access_key, aws_secret_access_key=self.secret_key)

    def get_boto3_resource(self, service_name, cos_region=None):
        return self.get_boto3_session().resource(service_name, region_name=cos_region)

    @imports.inject("ibm_boto3", "ibm_botocore", pip_extra="ibmcloud")
    def get_boto3_client(ibm_boto3, ibm_botocore, self, service_name, cos_region=None):
        client_config = ibm_botocore.client.Config(
            max_pool_connections=128,
            user_agent_extra=self.user_agent,
            connect_timeout=CONN_READ_TIMEOUT,
            read_timeout=CONN_READ_TIMEOUT,
            retries={"max_attempts": OBJ_REQ_RETRIES},
        )

        if cos_region is None:
            return self.get_boto3_session().client(service_name, config=client_config)
        else:
            return self.get_boto3_session().client(service_name, endpoint_url=self.get_ibmcloud_endpoint(cos_region), config=client_config)
