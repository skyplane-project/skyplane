from functools import lru_cache

from skyplane import exceptions, compute
from skyplane.utils import logger, imports
from skyplane.config_paths import cloud_config


class AzureStorageAccountInterface:
    """Class to manage state for an Azure storage account. Note that storage account names are globally unique."""

    def __init__(self, account_name: str):
        self.auth = compute.AzureAuthentication()
        self.account_name = account_name

    @lru_cache(maxsize=1)
    def storage_account_obj(self):
        sm_client = self.auth.get_storage_management_client()
        storage_accounts = sm_client.storage_accounts.list()
        for storage_account in storage_accounts:
            if storage_account.name == self.account_name:
                return storage_account
        raise exceptions.MissingBucketException(f"Storage account {self.account_name} not found")

    @property
    def azure_region(self):
        """Return the Azure region of the storage account. If the user doesn't have access to the storage account location, return 'unknown'."""
        try:
            return self.storage_account_obj().location
        except Exception as e:  # user does not have "Storage Account Contributor" role on the storage account
            logger.exception(e)
            return cloud_config.get_flag("default_azure_region")

    @property
    def storage_management_client(self):
        return self.auth.get_storage_management_client()

    def query_storage_account(self, storage_account_name):
        for account in self.storage_management_client.storage_accounts.list():
            if account.name == storage_account_name:
                return account
        raise ValueError(
            f"Storage account {storage_account_name} not found (found {[account.name for account in self.storage_management_client.storage_accounts.list()]})"
        )

    def storage_account_exists_in_account(self):
        """Note that we are not able to check if a storage account exists outside your account, so this is a best-effort check of your own account."""
        try:
            self.storage_account_obj()
            return True
        except exceptions.MissingBucketException:
            return False

    @imports.inject("azure.core.exceptions", pip_extra="azure")
    def create_storage_account(exceptions, self, azure_region, resource_group, tier="Premium_LRS"):
        try:
            operation = self.storage_management_client.storage_accounts.begin_create(
                resource_group, self.account_name, {"sku": {"name": tier}, "kind": "BlockBlobStorage", "location": azure_region}
            )
            operation.result()
        except exceptions.ResourceExistsError as e:
            logger.warning(f"Unable to create storage account as it already exists: {e}")
        self.storage_account_obj.cache_clear()
