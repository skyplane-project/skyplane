import threading
from typing import Optional

import boto3

from skylark.config import SkylarkConfig
from skylark import config_path


class AWSAuthentication:
    __cached_credentials = threading.local()

    def __init__(self, access_key: Optional[str] = None, secret_key: Optional[str] = None):
        """Loads AWS authentication details. If no access key is provided, it will try to load credentials using boto3"""
        self.config = SkylarkConfig.load_config(config_path)
        if access_key and secret_key:
            self.config_mode = "manual"
            self._access_key = access_key
            self._secret_key = secret_key
        else:
            self.config_mode = "iam_inferred"
            self._access_key = None
            self._secret_key = None

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

    def infer_credentials(self):
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

    def get_boto3_session(self, aws_region: str):
        if self.config_mode == "manual":
            return boto3.Session(
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key,
                region_name=aws_region,
            )
        else:
            return boto3.Session(region_name=aws_region)

    def get_boto3_resource(self, service_name, aws_region=None):
        return self.get_boto3_session(aws_region).resource(service_name, region_name=aws_region)

    def get_boto3_client(self, service_name, aws_region=None):
        if aws_region is None:
            return self.get_boto3_session(aws_region).client(service_name)
        else:
            return self.get_boto3_session(aws_region).client(service_name, region_name=aws_region)
