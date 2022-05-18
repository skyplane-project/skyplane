import os
import subprocess
from typing import Dict, List, Optional

from azure.identity import DefaultAzureCredential
from azure.mgmt.authorization import AuthorizationManagementClient
from azure.mgmt.storage import StorageManagementClient
from azure.storage.blob import BlobServiceClient, ContainerClient

from skylark import config_path
from skylark.compute.utils import query_which_cloud
from skylark.config import SkylarkConfig
from skylark import config_path
from skylark import azure_config_path
from skylark import azure_sku_path
from skylark.utils.utils import do_parallel
from skylark.utils import logger
import subprocess
import json

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
    def __init__(self, config: Optional[SkylarkConfig] = None):
        self.config = config if config is not None else SkylarkConfig.load_config(config_path)
        self._credential = None

    @property
    def credential(self) -> DefaultAzureCredential:
        if self._credential is None:
            self._credential = self.get_credential()
        return self._credential

    @property
    def subscription_id(self) -> Optional[str]:
        return self.config.azure_subscription_id

    @staticmethod
    def get_azure_credential() -> DefaultAzureCredential:
        exclude_msi = query_which_cloud() != "azure"  # exclude MSI if not Azure
        logger.fs.warning(f"Getting Azure credential, exclude_msi={exclude_msi}")
        return DefaultAzureCredential(
            exclude_managed_identity_credential=exclude_msi,
            exclude_powershell_credential=True,
            exclude_visual_studio_code_credential=True,
        )

    def save_region_config(self, config: SkylarkConfig):
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

        print("    Querying for SKU availbility in regions")
        result = do_parallel(get_skus, region_list, spinner=True, spinner_persist=False, desc="Query SKUs")

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
            print(
                "     No Azure config detected! Consquently, the Azure region list is empty. Run skylark init --reinit-azure to remedy this."
            )
            return []
        region_list = []
        for region in f.read().split("\n"):
            region_list.append(region)
        return region_list

    @staticmethod
    def get_sku_mapping() -> Dict[str, List[str]]:
        try:
            f = open(azure_sku_path, "r")
        except FileNotFoundError:
            print("     Azure SKU data has not been chaced! Run skylark init to remedy this!")
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
