from typing import Type, Optional

from skyplane.compute.aws.aws_auth import AWSAuthentication
from skyplane.compute.azure.azure_auth import AzureAuthentication
from skyplane.compute.gcp.gcp_auth import GCPAuthentication


class AuthenticationConfig:
    def make_auth_provider(self):
        raise NotImplementedError


class AWSAuthenticationConfig(AuthenticationConfig):
    def __init__(self, access_key_id: Optional[str] = None, secret_access_key: Optional[str] = None):
        self.aws_access_key = access_key_id
        self.aws_secret_key = secret_access_key
        self.aws_enabled = True

    def make_auth_provider(self) -> AWSAuthentication:
        return AWSAuthentication(config=self)  # type: ignore


class AzureAuthenticationConfig(AuthenticationConfig):
    def __init__(self, subscription_id: str, resource_group: str, umi_id: str, umi_name: str, umi_client_id: str):
        self.azure_subscription_id = subscription_id
        self.azure_resource_group = resource_group
        self.azure_umi_id = umi_id
        self.azure_umi_name = umi_name
        self.azure_umi_client_id = umi_client_id
        self.azure_enabled = True

    def make_auth_provider(self) -> AzureAuthentication:
        return AzureAuthentication(config=self)  # type: ignore


class GCPAuthenticationConfig(AuthenticationConfig):
    def __init__(self, project_id: str):
        self.gcp_project_id = project_id
        self.gcp_enabled = True

    def make_auth_provider(self) -> GCPAuthentication:
        return GCPAuthentication(config=self)  # type: ignore
