import threading
from typing import Optional

from skyplane import aws_config_path
from skyplane.compute.cloud_auth_provider import CloudAuthenticationProvider
from skyplane.config import SkyplaneConfig
from skyplane.utils import imports


class AWSAuthenticationProvider(CloudAuthenticationProvider):
    __cached_credentials = threading.local()

    def __init__(self, config=None):
        """Loads AWS authentication details. If no access key is provided, it will try to load credentials using boto3"""
        if not config:
            self.config_mode = "iam_inferred"
            self._access_key = None
            self._secret_key = None
        elif config.aws_access_key and config.aws_secret_key:
            self.config_mode = "manual"
            self._access_key = config.aws_access_key
            self._secret_key = config.aws_secret_key

    @imports.inject("boto3", pip_extra="aws")
    def save_region_config(boto3, self, config: SkyplaneConfig):
        if config.aws_enabled == False:
            self.clear_region_config()
            return
        with aws_config_path.open("w") as f:
            region_list = []
            describe_regions = boto3.client("ec2", region_name="us-east-1").describe_regions()
            for region in describe_regions["Regions"]:
                if region["OptInStatus"] == "opt-in-not-required" or region["OptInStatus"] == "opted-in":
                    region_text = region["Endpoint"]
                    region_name = region_text[region_text.find(".") + 1 : region_text.find(".amazon")]
                    region_list.append(region_name)
            f.write("\n".join(region_list))

    def clear_region_config(self):
        with aws_config_path.open("w") as f:
            f.write("")

    @staticmethod
    def get_region_config():
        try:
            f = open(aws_config_path, "r")
        except FileNotFoundError:
            return []
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
        try:
            self.get_boto3_client("ec2", "us-west-2").describe_regions()
            return True
        except Exception as e:
            print("Error inferring whether aws is enabled:", str(e))
            return False

    @imports.inject("boto3", pip_extra="aws")
    def infer_credentials(boto3, self):
        # todo load temporary credentials from STS
        cached_credential = getattr(self.__cached_credentials, "boto3_credential", None)
        if cached_credential == None:
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
