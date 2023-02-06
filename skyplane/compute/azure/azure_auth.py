import json
import logging
import os
import subprocess

from typing import Dict, List, Optional

from skyplane.compute.const_cmds import query_which_cloud
from skyplane.config import SkyplaneConfig
from skyplane.config_paths import config_path, azure_config_path, azure_sku_path
from skyplane.utils import imports
from skyplane.utils.definitions import is_gateway_env
from skyplane.utils.fn import do_parallel, wait_for


class AzureAuthentication:
    def __init__(self, config: Optional[SkyplaneConfig] = None):
        self.config = config if config is not None else SkyplaneConfig.load_config(config_path)
        self._credential = None

    @property
    @imports.inject(
        "azure.identity.DefaultAzureCredential",
        "azure.identity.ManagedIdentityCredential",
        pip_extra="azure",
    )
    def credential(DefaultAzureCredential, ManagedIdentityCredential, self):
        if self._credential is None:
            if is_gateway_env:
                print("Configured managed identity credential.")
                return ManagedIdentityCredential(client_id=self.config.azure_umi_client_id)
            else:
                if query_which_cloud() != "azure":
                    return DefaultAzureCredential(
                        exclude_environment_credential=True,
                        exclude_managed_identity_credential=True,
                        exclude_powershell_credential=True,
                        exclude_visual_studio_code_credential=True,
                    )
                else:
                    return DefaultAzureCredential(
                        managed_identity_client_id=self.config.azure_client_id
                        if isinstance(self.config, SkyplaneConfig)
                        else self.config.azure_umi_client_id,
                        exclude_powershell_credential=True,
                        exclude_visual_studio_code_credential=True,
                    )
        return self._credential

    @property
    def subscription_id(self) -> Optional[str]:
        return self.config.azure_subscription_id

    def save_region_config(self):
        if self.config.azure_enabled == False:
            self.clear_region_config()
            return
        region_list = []
        for location in self.get_subscription_client().subscriptions.list_locations(subscription_id=self.subscription_id):
            region_list.append(location.name)
        with azure_config_path.open("w") as f:
            f.write("\n".join(region_list))
        client = self.get_compute_client()

        def get_skus(region):
            valid_skus = []
            for sku in client.resource_skus.list(filter="location eq '{}'".format(region)):
                if len(sku.restrictions) == 0:
                    valid_skus.append(sku.name)
            return set(valid_skus)

        result = do_parallel(
            get_skus,
            region_list,
            spinner=False,
            spinner_persist=False,
            desc="Query available VM SKUs from each enabled Azure region (est. time: ~1 minute)",
            n=8,
        )
        region_sku = dict()
        for region, skus in result:
            region_sku[region] = list()
            region_sku[region].extend(skus)
        with azure_sku_path.open("w") as f:
            json.dump(region_sku, f)

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

    @imports.inject("azure.mgmt.compute.ComputeManagementClient", pip_extra="azure")
    def get_compute_client(ComputeManagementClient, self):
        return ComputeManagementClient(self.credential, self.subscription_id)

    @imports.inject("azure.mgmt.resource.ResourceManagementClient", pip_extra="azure")
    def get_resource_client(ResourceManagementClient, self):
        return ResourceManagementClient(self.credential, self.subscription_id)

    @imports.inject("azure.mgmt.subscription.SubscriptionClient", pip_extra="azure")
    def get_subscription_client(SubscriptionClient, self):
        return SubscriptionClient(self.credential)

    @imports.inject("azure.mgmt.network.NetworkManagementClient", pip_extra="azure")
    def get_network_client(NetworkManagementClient, self):
        return NetworkManagementClient(self.credential, self.subscription_id)

    @imports.inject("azure.mgmt.authorization.AuthorizationManagementClient", pip_extra="azure")
    def get_authorization_client(AuthorizationManagementClient, self):
        # set API version to avoid UnsupportedApiVersionForRoleDefinitionHasDataActions error
        return AuthorizationManagementClient(self.credential, self.subscription_id, api_version="2018-01-01-preview")

    @imports.inject("azure.mgmt.storage.StorageManagementClient", pip_extra="azure")
    def get_storage_management_client(StorageManagementClient, self):
        return StorageManagementClient(self.credential, self.subscription_id)

    @imports.inject("azure.storage.blob.ContainerClient", pip_extra="azure")
    def get_container_client(ContainerClient, self, account_url: str, container_name: str):
        return ContainerClient(account_url, container_name, credential=self.credential)

    @imports.inject("azure.storage.blob.BlobServiceClient", pip_extra="azure")
    def get_blob_service_client(BlobServiceClient, self, account_url: str):
        return BlobServiceClient(account_url=account_url, credential=self.credential)
