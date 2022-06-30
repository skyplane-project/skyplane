import json
import os
import subprocess
from typing import Dict, List, Optional

from azure.identity import DefaultAzureCredential, ManagedIdentityCredential
from azure.mgmt.authorization import AuthorizationManagementClient
from azure.mgmt.storage import StorageManagementClient
from azure.storage.blob import BlobServiceClient, ContainerClient

from skyplane import azure_config_path
from skyplane import azure_sku_path
from skyplane import config_path
from skyplane import is_gateway_env
from skyplane.compute.const_cmds import query_which_cloud
from skyplane.config import SkyplaneConfig
from skyplane.utils.fn import do_parallel

# optional imports due to large package size
try:
    from azure.mgmt.network import NetworkManagementClient
except ImportError:
    NetworkManagementClient = None

try:
    from azure.mgmt.compute import ComputeManagementClient
except ImportError:
    ComputeManagementClient = None

try:
    from azure.mgmt.resource import ResourceManagementClient
except ImportError:
    ResourceManagementClient = None

try:
    from azure.mgmt.resource.subscriptions import SubscriptionClient
except ImportError:
    SubscriptionClient = None


class AzureAuthentication:
    def __init__(self, config: Optional[SkyplaneConfig] = None):
        self.config = config if config is not None else SkyplaneConfig.load_config(config_path)
        self._credential = None

    @property
    def credential(self) -> DefaultAzureCredential:
        if self._credential is None:
            self._credential = self.get_azure_credential()
        return self._credential

    @property
    def subscription_id(self) -> Optional[str]:
        return self.config.azure_subscription_id

    @staticmethod
    def get_azure_credential() -> DefaultAzureCredential:
        if is_gateway_env:
            return ManagedIdentityCredential()
        return DefaultAzureCredential(
            exclude_managed_identity_credential=(query_which_cloud() != "azure"),
            exclude_powershell_credential=True,
            exclude_visual_studio_code_credential=True,
        )

    def save_region_config(self, config: SkyplaneConfig):
        if config.azure_enabled == False:
            self.clear_region_config()
            return
        region_list = []
        for location in self.get_subscription_client().subscriptions.list_locations(subscription_id=self.subscription_id):
            region_list.append(location.name)
        with azure_config_path.open("w") as f:
            f.write("\n".join(region_list))
        print(f"    Azure region config file saved to {azure_config_path}")

        client = self.get_compute_client()

        def get_skus(region):
            valid_skus = []
            for sku in client.resource_skus.list(filter="location eq '{}'".format(region)):
                if len(sku.restrictions) == 0 and sku.name.startswith("Standard_D32"):
                    valid_skus.append(sku.name)
            return set(valid_skus)

        result = do_parallel(
            get_skus, region_list, spinner=True, spinner_persist=False, desc="Query available VM SKUs from each enabled Azure region", n=8
        )
        region_sku = dict()
        for region, skus in result:
            region_sku[region] = list()
            region_sku[region].extend(skus)

        with azure_sku_path.open("w") as f:
            json.dump(region_sku, f)

        print(f"    Azure SKU availability cached in {azure_sku_path}")

    @staticmethod
    def get_region_config() -> List[str]:
        try:
            f = open(azure_config_path, "r")
        except FileNotFoundError:
            return []
        region_list = []
        for region in f.read().split("\n"):
            if not region.endswith("stage") and not region.endswith("euap"):
                region_list.append(region)
        return region_list

    @staticmethod
    def get_sku_mapping() -> Dict[str, List[str]]:
        try:
            f = open(azure_sku_path, "r")
        except FileNotFoundError:
            print("     Azure SKU data has not been chaced! Run skyplane init to remedy this!")
            return dict()

        return json.load(f)

    def clear_region_config(self):
        with azure_config_path.open("w") as f:
            f.write("")

    def enabled(self) -> bool:
        return self.config.azure_enabled and self.credential is not None

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
        assert ComputeManagementClient is not None, "ComputeManagementClient is not installed"
        return ComputeManagementClient(self.credential, self.subscription_id)

    def get_resource_client(self):
        assert ResourceManagementClient is not None, "ResourceManagementClient is not installed"
        return ResourceManagementClient(self.credential, self.subscription_id)

    def get_subscription_client(self):
        assert SubscriptionClient is not None, "SubscriptionClient is not installed"
        return SubscriptionClient(self.credential)

    def get_network_client(self):
        assert NetworkManagementClient is not None, "NetworkManagementClient is not installed"
        return NetworkManagementClient(self.credential, self.subscription_id)

    def get_authorization_client(self):
        # set API version to avoid UnsupportedApiVersionForRoleDefinitionHasDataActions error
        return AuthorizationManagementClient(self.credential, self.subscription_id, api_version="2018-01-01-preview")

    def get_storage_management_client(self):
        return StorageManagementClient(self.credential, self.subscription_id)

    def get_container_client(self, account_url: str, container_name: str):
        return ContainerClient(account_url, container_name, credential=self.credential)

    def get_blob_service_client(self, account_url: str):
        return BlobServiceClient(account_url=account_url, credential=self.credential)
