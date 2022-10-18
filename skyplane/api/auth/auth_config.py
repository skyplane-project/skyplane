from typing import Type, Optional

from skyplane.compute.aws.aws_auth_provider import AWSAuthenticationProvider
from skyplane.compute.azure.azure_auth_provider import AzureAuthenticationProvider
from skyplane.compute.gcp.gcp_auth_provider import GCPAuthenticationProvider
from skyplane.compute.cloud_auth_provider import CloudAuthenticationProvider


class AuthenticationConfig:
    def make_auth_provider(self) -> Type[CloudAuthenticationProvider]:
        raise NotImplementedError


class AWSAuthenticationConfig(AuthenticationConfig):
    def __init__(self, access_key_id: Optional[str], secret_access_key: Optional[str]):
        self.aws_access_key = access_key_id
        self.aws_secret_key = secret_access_key
        self.aws_enabled = True

    def make_auth_provider(self) -> AWSAuthenticationProvider:
        return AWSAuthenticationProvider(config=self)


class GCPAuthenticationConfig(AuthenticationConfig):
    def __init__(self, project_id: str):
        self.gcp_project_id = project_id
        self.gcp_enabled = True

    def make_auth_provider(self) -> GCPAuthenticationProvider:
        return GCPAuthenticationProvider(config=self)


class AzureAuthenticationConfig(AuthenticationConfig):
    def __init__(self, client_id: str, subscription_id: str, umi_id: str, umi_name: str, resource_group: str):
        self.azure_client_id = client_id
        self.azure_subscription_id = subscription_id
        self.azure_principal_id = umi_id
        self.azure_umi_name = umi_name
        self.azure_resource_group = resource_group
        self.azure_enabled = True

    def make_auth_provider(self) -> AzureAuthenticationProvider:
        return AzureAuthenticationProvider(config=self)
