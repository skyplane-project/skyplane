import uuid
from functools import lru_cache

from azure.core.exceptions import ResourceExistsError
from azure.mgmt.authorization.v2015_07_01.models import RoleAssignmentCreateParameters, RoleAssignmentProperties

from skyplane import exceptions
from skyplane.compute.azure.azure_auth import AzureAuthentication
from skyplane.compute.azure.azure_server import AzureServer
from skyplane.utils import logger


class AzureStorageAccountInterface:
    """Class to manage state for an Azure storage account. Note that storage account names are globally unique."""

    def __init__(self, account_name: str):
        self.auth = AzureAuthentication()
        self.account_name = account_name

    @lru_cache
    def storage_account_obj(self):
        sm_client = self.auth.get_storage_management_client()
        storage_accounts = sm_client.storage_accounts.list()
        for storage_account in storage_accounts:
            if storage_account.name == self.account_name:
                return storage_account
        raise exceptions.MissingBucketException(f"Storage account {self.account_name} not found")

    @property
    def azure_region(self):
        return self.storage_account_obj().location

    @property
    def azure_resource_group(self):
        return self.storage_account_obj().resource_group_name

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

    def storage_account_exists(self):
        for account in self.storage_management_client.storage_accounts.list():
            if account.name == self.account_name:
                return True
        return False

    def create_storage_account(self, azure_region, resource_group, tier="Premium_LRS"):
        try:
            operation = self.storage_management_client.storage_accounts.begin_create(
                resource_group,
                self.account_name,
                {"sku": {"name": tier}, "kind": "BlockBlobStorage", "location": azure_region},
            )
            operation.result()
        except ResourceExistsError as e:
            logger.warning(f"Unable to create storage account as it already exists: {e}")
        self.storage_account_obj.cache_clear()

    def grant_storage_account_access(self, role_name: str, principal_id: str):
        # lookup role
        auth_client = self.auth.get_authorization_client()
        scope = f"/subscriptions/{self.auth.subscription_id}/resourceGroups/{self.azure_resource_group}/providers/Microsoft.Storage/storageAccounts/{self.account_name}"
        roles = list(auth_client.role_definitions.list(scope, filter="roleName eq '{}'".format(role_name)))
        assert len(roles) == 1

        # query for existing role assignment
        matches = []
        for assignment in auth_client.role_assignments.list_for_scope(scope, filter="principalId eq '{}'".format(principal_id)):
            if assignment.role_definition_id == roles[0].id:
                matches.append(assignment)
        if len(matches) == 0:
            logger.debug(f"Granting access to {principal_id} for role {role_name} on storage account {self.account_name}")
            params = RoleAssignmentCreateParameters(
                properties=RoleAssignmentProperties(role_definition_id=roles[0].id, principal_id=principal_id)
            )
            auth_client.role_assignments.create(scope, uuid.uuid4(), params)
