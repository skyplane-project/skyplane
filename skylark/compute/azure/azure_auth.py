import os
import subprocess
from typing import Optional
from azure.identity import DefaultAzureCredential
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.network import NetworkManagementClient
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.authorization import AuthorizationManagementClient
from azure.storage.blob import BlobServiceClient

from skylark import cloud_config
from skylark.compute.utils import query_which_cloud


class AzureAuthentication:
    def __init__(self, subscription_id: str = cloud_config.azure_subscription_id):
        self.subscription_id = subscription_id
        self.credential = DefaultAzureCredential(
            exclude_managed_identity_credential=query_which_cloud() != "azure",  # exclude MSI if not Azure
            exclude_powershell_credential=True,
            exclude_visual_studio_code_credential=True,
        )

    def enabled(self) -> bool:
        return self.subscription_id is not None

    @staticmethod
    def infer_subscription_id() -> Optional[str]:
        if "AZURE_SUBSCRIPTION_ID" in os.environ:
            return os.environ["AZURE_SUBSCRIPTION_ID"]
        else:
            try:
                return subprocess.check_output(["az", "account", "show", "--query", "id"]).decode("utf-8").strip()
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

    def get_storage_client(self, account_url: str):
        return BlobServiceClient(account_url=account_url, credential=self.credential)
