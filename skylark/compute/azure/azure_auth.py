import os
import subprocess
import threading
from typing import Optional
from azure.identity import DefaultAzureCredential
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.network import NetworkManagementClient
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.authorization import AuthorizationManagementClient
from azure.mgmt.storage import StorageManagementClient
from azure.storage.blob import BlobServiceClient, ContainerClient

from skylark import cloud_config
from skylark.compute.utils import query_which_cloud
from skylark.config import SkylarkConfig
from skylark import config_path


class AzureAuthentication:
    __cached_credentials = threading.local()

    def __init__(self, config: Optional[SkylarkConfig] = None, subscription_id: str = cloud_config.azure_subscription_id):
        if not config == None:
            self.config = config
        else:
            self.config = SkylarkConfig.load_config(config_path)
        self.subscription_id = subscription_id
        self.credential = self.get_credential(subscription_id)

    def get_credential(self, subscription_id: str):
        cached_credential = getattr(self.__cached_credentials, f"credential_{subscription_id}", None)
        if cached_credential is None:
            cached_credential = DefaultAzureCredential(
                exclude_managed_identity_credential=query_which_cloud() != "azure",  # exclude MSI if not Azure
                exclude_powershell_credential=True,
                exclude_visual_studio_code_credential=True,
            )
            setattr(self.__cached_credentials, f"credential_{subscription_id}", cached_credential)
        return cached_credential

    def enabled(self) -> bool:
        return self.config.azure_enabled and self.subscription_id is not None

    @staticmethod
    def infer_subscription_id() -> Optional[str]:
        if "AZURE_SUBSCRIPTION_ID" in os.environ:
            return os.environ["AZURE_SUBSCRIPTION_ID"]
        else:
            try:
                return subprocess.check_output(["az", "account", "show", "--query", "id"]).decode("utf-8").replace('"', "").strip()
            except subprocess.CalledProcessError:
                return None

    def get_token(self, resource: str):
        return self.credential.get_token(resource)

    def get_compute_client(self):
        return ComputeManagementClient(self.credential, self.subscription_id)

    def get_resource_client(self):
        return ResourceManagementClient(self.credential, self.subscription_id)

    def get_network_client(self):
        return NetworkManagementClient(self.credential, self.subscription_id)

    def get_authorization_client(self):
        return AuthorizationManagementClient(self.credential, self.subscription_id)

    def get_storage_management_client(self):
        return StorageManagementClient(self.credential, self.subscription_id)

    def get_container_client(self, account_url: str, container_name: str):
        return ContainerClient(account_url, container_name, credential=self.credential)

    def get_blob_service_client(self, account_url: str):
        return BlobServiceClient(account_url=account_url, credential=self.credential)
