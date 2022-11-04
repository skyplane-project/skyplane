from dataclasses import dataclass

from typing import Optional

from skyplane.compute import AWSAuthentication
from skyplane.compute import AzureAuthentication
from skyplane.compute import GCPAuthentication


class AuthenticationConfig:
    def make_auth_provider(self):
        raise NotImplementedError


@dataclass
class AWSConfig(AuthenticationConfig):
    aws_access_key: Optional[str] = None
    aws_secret_key: Optional[str] = None
    aws_enabled: bool = True

    def make_auth_provider(self) -> AWSAuthentication:
        return AWSAuthentication(config=self)  # type: ignore


@dataclass
class AzureConfig(AuthenticationConfig):
    azure_subscription_id: str
    azure_resource_group: str
    azure_umi_id: str
    azure_umi_name: str
    azure_umi_client_id: str
    azure_enabled: bool = True

    def make_auth_provider(self) -> AzureAuthentication:
        return AzureAuthentication(config=self)  # type: ignore


@dataclass
class GCPConfig(AuthenticationConfig):
    gcp_project_id: str
    gcp_enabled: bool = True

    def make_auth_provider(self) -> GCPAuthentication:
        return GCPAuthentication(config=self)  # type: ignore
