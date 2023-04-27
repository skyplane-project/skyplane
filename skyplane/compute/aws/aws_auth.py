from typing import Optional, Dict
import json

from skyplane.config import SkyplaneConfig
from skyplane.config_paths import config_path, aws_config_path, aws_quota_path
from skyplane.utils import imports, fn, logger


class AWSAuthentication:
    def __init__(self, config: Optional[SkyplaneConfig] = None, access_key: Optional[str] = None, secret_key: Optional[str] = None):
        """Loads AWS authentication details. If no access key is provided, it will try to load credentials using boto3"""
        if not config is None:
            self.config = config
        else:
            self.config = SkyplaneConfig.load_config(config_path)

        if access_key and secret_key:
            self.config_mode = "manual"
            self._access_key = access_key
            self._secret_key = secret_key
        else:
            self.config_mode = "iam_inferred"
            self._access_key = None
            self._secret_key = None

    def _get_ec2_vm_quota(self, region) -> Dict[str, int]:
        """Given the region, get the maximum number of vCPU that can be launched.

        Returns:
            name_to_quota: a dictionary of quota name to quota value
        """
        # NOTE: the QuotaCode can be retried via
        # aws service-quotas list-service-quotas --service-code ec2
        # The items we are looking is
        # "Running On-Demand Standard (A, C, D, H, I, M, R, T, Z) instances" L-1216C47A
        # "All Standard (A, C, D, H, I, M, R, T, Z) Spot Instance Requests" L-34B43A08
        quotas_client = self.get_boto3_client("service-quotas", region)

        name_to_quota = {}
        for name, code in [("on_demand_standard_vcpus", "L-1216C47A"), ("spot_standard_vcpus", "L-34B43A08")]:
            try:
                retrieved_quota = quotas_client.get_service_quota(ServiceCode="ec2", QuotaCode=code)
                name_to_quota[name] = int(retrieved_quota["Quota"]["Value"])
            except Exception:
                logger.warning(
                    f"Failed to retrieve quota information for {name} in {region}. Skyplane will use a conservative configuration."
                )
        return name_to_quota

    @imports.inject("boto3", pip_extra="aws")
    def save_region_config(boto3, self, config: SkyplaneConfig):
        if not config.aws_enabled:
            self.clear_region_config()
            return
        with aws_config_path.open("w") as f:
            region_list = []

            # query regions
            describe_regions = boto3.client("ec2", region_name="us-east-1").describe_regions()
            for region in describe_regions["Regions"]:
                if region["OptInStatus"] == "opt-in-not-required" or region["OptInStatus"] == "opted-in":
                    region_text = region["Endpoint"]
                    region_name = region_text[region_text.find(".") + 1 : region_text.find(".amazon")]
                    region_list.append(region_name)
            f.write("\n".join(region_list))

        quota_infos = fn.do_parallel(
            self._get_ec2_vm_quota, region_list, return_args=False, spinner=True, desc="Retrieving EC2 Quota information"
        )
        region_infos = [dict(**info, **{"region_name": name}) for name, info in zip(region_list, quota_infos)]
        with aws_quota_path.open("w") as f:
            f.write(json.dumps(region_infos, indent=2))

    def clear_region_config(self):
        with aws_config_path.open("w") as f:
            f.write("")

    @staticmethod
    def get_region_config():
        if not aws_config_path.exists():
            return []
        with aws_config_path.open() as f:
            region_list = []
            for region in f.read().split("\n"):
                region_list.append(region)
            return region_list

    @property
    def access_key(self):
        if self._access_key is None:
            self._access_key, self._secret_key = self.infer_credentials()
        return self._access_key

    @property
    def secret_key(self):
        if self._secret_key is None:
            self._access_key, self._secret_key = self.infer_credentials()
        return self._secret_key

    def enabled(self):
        return self.config.aws_enabled

    @imports.inject("boto3", pip_extra="aws")
    def infer_credentials(boto3, self):
        # todo load temporary credentials from STS
        cached_credential = getattr(self.__cached_credentials, "boto3_credential", None)
        if cached_credential is None:
            session = boto3.Session()
            credentials = session.get_credentials()
            if credentials:
                credentials = credentials.get_frozen_credentials()
                cached_credential = (credentials.access_key, credentials.secret_key)
            setattr(self.__cached_credentials, "boto3_credential", cached_credential)
        return cached_credential if cached_credential else (None, None)

    @imports.inject("boto3", pip_extra="aws")
    def get_boto3_session(boto3, self, aws_region: Optional[str] = None):
        if self.config_mode == "manual":
            return boto3.Session(aws_access_key_id=self.access_key, aws_secret_access_key=self.secret_key, region_name=aws_region)
        else:
            return boto3.Session(region_name=aws_region)

    def get_boto3_resource(self, service_name, aws_region=None):
        return self.get_boto3_session().resource(service_name, region_name=aws_region)

    def get_boto3_client(self, service_name, aws_region=None):
        if aws_region is None:
            return self.get_boto3_session().client(service_name)
        else:
            return self.get_boto3_session().client(service_name, region_name=aws_region)

    def get_azs_in_region(self, region):
        ec2 = self.get_boto3_client("ec2", region)
        azs = []
        for az in ec2.describe_availability_zones()["AvailabilityZones"]:
            azs.append(az["ZoneName"])
        return azs
