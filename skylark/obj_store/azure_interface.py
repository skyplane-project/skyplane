import os
import subprocess
from typing import Iterator, List
import uuid
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
from skylark.compute.azure.azure_auth import AzureAuthentication
from skylark.compute.azure.azure_server import AzureServer
from skylark.utils import logger
from skylark.obj_store.object_store_interface import NoSuchObjectException, ObjectStoreInterface, ObjectStoreObject
from azure.mgmt.authorization.models import RoleAssignmentCreateParameters, RoleAssignmentProperties
from azure.identity import AzureCliCredential


class AzureObject(ObjectStoreObject):
    def full_path(self):
        account_name, container_name = self.bucket.split("/")
        return os.path.join(f"https://{account_name}.blob.core.windows.net", container_name, self.key)


class AzureInterface(ObjectStoreInterface):
    def __init__(self, azure_region, account_name, container_name, use_tls=True):
        # TODO (#210): should be configured via argument
        self.account_name = f"skylark{azure_region.replace(' ', '').lower()}"
        self.container_name = container_name

        # Create a blob service client
        self.auth = AzureAuthentication()
        self.account_url = f"https://{self.account_name}.blob.core.windows.net"

        # infer azure region from storage account
        if azure_region is None or azure_region == "infer":
            self.azure_region = self.get_region_from_storage_account(self.account_name)
        else:
            self.azure_region = azure_region

        # parallel upload/downloads
        self.max_concurrency = 1

    @property
    def blob_service_client(self):
        return self.auth.get_blob_service_client(self.account_url)

    @property
    def container_client(self):
        return self.auth.get_container_client(self.account_url, self.container_name)

    @property
    def storage_management_client(self):
        return self.auth.get_storage_management_client()

    def region_tag(self):
        return "azure:" + self.azure_region

    def get_region_from_storage_account(self, storage_account_name):
        storage_account = self.storage_management_client.storage_accounts.get_properties(
            AzureServer.resource_group_name, storage_account_name
        )
        return storage_account.location

    def storage_account_exists(self):
        try:
            self.storage_management_client.storage_accounts.get_properties(AzureServer.resource_group_name, self.account_name)
            return True
        except ResourceNotFoundError:
            return False

    def container_exists(self):
        try:
            self.container_client.get_container_properties()
            return True
        except ResourceNotFoundError:
            return False

    def create_storage_account(self, tier="Premium_LRS"):
        try:
            operation = self.storage_management_client.storage_accounts.begin_create(
                AzureServer.resource_group_name,
                self.account_name,
                {"sku": {"name": tier}, "kind": "BlockBlobStorage", "location": self.azure_region},
            )
            operation.result()
        except ResourceExistsError as e:
            logger.warning(f"Unable to create storage account as it already exists: {e}")

    def infer_cli_principal_id(self):
        self.auth.credential.get_token("https://graph.windows.net")  # must request token to attempt to load credential
        if isinstance(self.auth.credential._successful_credential, AzureCliCredential):
            out = subprocess.check_output(["az", "ad", "signed-in-user", "show", "--query", "objectId", "-o", "tsv"])
            return out.decode("utf-8").strip()
        else:
            return None

    def grant_storage_account_access(self, role_name: str, principal_id: str):
        # lookup role
        auth_client = self.auth.get_authorization_client()
        scope = f"/subscriptions/{self.auth.subscription_id}/resourceGroups/{AzureServer.resource_group_name}/providers/Microsoft.Storage/storageAccounts/{self.account_name}"
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

    def create_container(self):
        try:
            self.container_client.create_container()
        except ResourceExistsError:
            logger.warning(f"Unable to create container {self.container_name} as it already exists")

    def create_bucket(self, premium_tier=True):
        tier = "Premium_LRS" if premium_tier else "Standard_LRS"
        if not self.storage_account_exists():
            logger.debug(f"Creating storage account {self.account_name}")
            self.create_storage_account(tier=tier)
        principal_id = self.infer_cli_principal_id()
        if principal_id:
            self.grant_storage_account_access("Storage Blob Data Contributor", principal_id)
        if not self.container_exists():
            logger.debug(f"Creating container {self.container_name}")
            self.create_container()

    def delete_container(self):
        try:
            self.container_client.delete_container()
        except ResourceNotFoundError:
            logger.warning("Unable to delete container as it doesn't exists")

    def delete_bucket(self):
        return self.delete_container()

    def list_objects(self, prefix="") -> Iterator[AzureObject]:
        blobs = self.container_client.list_blobs()
        for blob in blobs:
            yield AzureObject("azure", f"{self.account_name}/{blob.container}", blob.name, blob.size, blob.last_modified)

    def delete_objects(self, keys: List[str]):
        for key in keys:
            blob_client = self.blob_service_client.get_blob_client(container=self.container_name, blob=key)
            blob_client.delete_blob()

    def get_obj_metadata(self, obj_name):  # Not Tested
        blob_client = self.blob_service_client.get_blob_client(container=self.container_name, blob=obj_name)
        try:
            return blob_client.get_blob_properties()
        except ResourceNotFoundError as e:
            raise NoSuchObjectException(f"Object {obj_name} does not exist, or you do not have permission to access it") from e

    def get_obj_size(self, obj_name):
        return self.get_obj_metadata(obj_name).size

    def exists(self, obj_name):
        blob_client = self.blob_service_client.get_blob_client(container=self.container_name, blob=obj_name)
        try:
            blob_client.get_blob_properties()
            return True
        except ResourceNotFoundError:
            return False

    def download_object(self, src_object_name, dst_file_path, offset_bytes=None, size_bytes=None):
        src_object_name, dst_file_path = str(src_object_name), str(dst_file_path)
        src_object_name = src_object_name if src_object_name[0] != "/" else src_object_name
        blob_client = self.blob_service_client.get_blob_client(container=self.container_name, blob=src_object_name)
        if not os.path.exists(dst_file_path):
            open(dst_file_path, "a").close()
        with open(dst_file_path, "rb+") as download_file:
            download_file.write(blob_client.download_blob(max_concurrency=self.max_concurrency).readall())

    def upload_object(self, src_file_path, dst_object_name, part_number=None, upload_id=None):
        src_file_path, dst_object_name = str(src_file_path), str(dst_object_name)
        dst_object_name = dst_object_name if dst_object_name[0] != "/" else dst_object_name
        os.path.getsize(src_file_path)
        blob_client = self.blob_service_client.get_blob_client(container=self.container_name, blob=dst_object_name)
        with open(src_file_path, "rb") as data:
            blob_client.upload_blob(data=data, overwrite=True, max_concurrency=self.max_concurrency)
