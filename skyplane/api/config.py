from dataclasses import dataclass

from typing import Optional, List

from skyplane import compute

from skyplane.config_paths import aws_quota_path, gcp_quota_path, azure_standardDv5_quota_path
from pathlib import Path


class AuthenticationConfig:
    def make_auth_provider(self):
        raise NotImplementedError


@dataclass
class AWSConfig(AuthenticationConfig):
    aws_access_key: Optional[str] = None
    aws_secret_key: Optional[str] = None
    aws_enabled: bool = True

    def make_auth_provider(self) -> compute.AWSAuthentication:
        return compute.AWSAuthentication(config=self)  # type: ignore


@dataclass
class AzureConfig(AuthenticationConfig):
    azure_subscription_id: str
    azure_resource_group: str
    azure_umi_id: str
    azure_umi_name: str
    azure_umi_client_id: str
    azure_enabled: bool = True

    def make_auth_provider(self) -> compute.AzureAuthentication:
        return compute.AzureAuthentication(config=self)  # type: ignore


@dataclass
class GCPConfig(AuthenticationConfig):
    gcp_project_id: str
    gcp_enabled: bool = True

    def make_auth_provider(self) -> compute.GCPAuthentication:
        return compute.GCPAuthentication(config=self)  # type: ignore


@dataclass
class IBMCloudConfig(AuthenticationConfig):
    ibmcloud_access_id: Optional[str] = None
    ibmcloud_secret_key: Optional[str] = None
    ibmcloud_iam_key: Optional[str] = None
    ibmcloud_iam_endpoint: Optional[str] = None
    ibmcloud_useragent: Optional[str] = None
    ibmcloud_resource_group_id: Optional[str] = None
    ibmcloud_enabled: bool = False

    def make_auth_provider(self) -> compute.IBMCloudAuthentication:
        # pytype: disable=attribute-error
        return compute.IBMCloudAuthentication(config=self)  # type: ignore
        # pytype: enable=attribute-error


@dataclass(frozen=True)
class TransferConfig:
    autoterminate_minutes: int = 15
    requester_pays: bool = False

    # randomly generate data
    gen_random_data: bool = False
    gen_random_data_chunk_size_mb: Optional[float] = None
    gen_random_data_num_chunks: Optional[int] = None

    # gateway settings
    use_bbr: bool = True
    use_compression: bool = True
    use_e2ee: bool = True
    use_socket_tls: bool = False

    # provisioning config
    aws_use_spot_instances: bool = False
    azure_use_spot_instances: bool = False
    gcp_use_spot_instances: bool = False
    ibmcloud_use_spot_instances: bool = False

    aws_instance_class: str = "m5.8xlarge"
    azure_instance_class: str = "Standard_D2_v5"
    gcp_instance_class: str = "n2-standard-16"
    ibmcloud_instance_class: str = "bx2-2x8"
    gcp_use_premium_network: bool = True

    aws_vcpu_file: Path = aws_quota_path
    gcp_vcpu_file: Path = gcp_quota_path
    azure_vcpu_file: Path = azure_standardDv5_quota_path
    # TODO: add ibmcloud when the quota info is available

    # multipart config
    multipart_enabled: bool = True
    multipart_threshold_mb: int = 128
    multipart_chunk_size_mb: int = 64
    multipart_max_chunks: int = 10000
