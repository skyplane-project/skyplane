from typing import Optional

import boto3


class AWSAuthentication:
    def __init__(self, access_key: Optional[str] = None, secret_key: Optional[str] = None):
        """Loads AWS authentication details. If no access key is provided, it will try to load credentials using boto3"""
        if access_key and secret_key:
            self.config_mode = "manual"
            self.access_key = access_key
            self.secret_key = secret_key
        else:
            infer_access_key, infer_secret_key = self.infer_credentials()
            if infer_access_key and infer_secret_key:
                self.config_mode = "iam_inferred"
                self.access_key = infer_access_key
                self.secret_key = infer_secret_key
            else:
                self.config_mode = "disabled"
                self.access_key = None
                self.secret_key = None

    def enabled(self):
        return self.config_mode != "disabled"

    def infer_credentials(self):
        # todo load temporary credentials from STS
        session = boto3.Session()
        credentials = session.get_credentials()
        credentials = credentials.get_frozen_credentials()
        return credentials.access_key, credentials.secret_key

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
