import os
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Iterator, List
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
from skylark.compute.azure.azure_auth import AzureAuthentication
from skylark.compute.azure.azure_server import AzureServer
from skylark.utils import logger
from skylark.obj_store.object_store_interface import NoSuchObjectException, ObjectStoreInterface, ObjectStoreObject


class AzureObject(ObjectStoreObject):
    def full_path(self):
        account_name, container_name = self.bucket.split("/")
        return os.path.join(f"https://{account_name}.blob.core.windows.net", container_name, self.key)


class AzureInterface(ObjectStoreInterface):
    def __init__(self, azure_region, container_name):
        self.azure_region = azure_region
        self.account_name = f"skylark{azure_region.replace(' ', '').lower()}"
        self.container_name = container_name
        # Create a blob service client
        self.auth = AzureAuthentication()
        self.account_url = f"https://{self.account_name}.blob.core.windows.net"
        self.storage_management_client = self.auth.get_storage_management_client()
        self.container_client = self.auth.get_container_client(self.account_url, self.container_name)
        self.blob_service_client = self.auth.get_blob_service_client(self.account_url)

        # parallel upload/downloads
        self.pool = ThreadPoolExecutor(max_workers=256)  # TODO: This might need some tuning
        self.max_concurrency = 1

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
        except ResourceExistsError:
            logger.warning("Unable to create storage account as it already exists")

    def create_container(self):
        try:
            self.container_client.create_container()
        except ResourceExistsError:
            logger.warning("Unable to create container as it already exists")

    def create_bucket(self, premium_tier=True):
        tier = "Premium_LRS" if premium_tier else "Standard_LRS"
        if not self.storage_account_exists():
            self.create_storage_account(tier=tier)
        if not self.container_exists():
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

    def download_object(self, src_object_name, dst_file_path) -> Future:
        src_object_name, dst_file_path = str(src_object_name), str(dst_file_path)
        src_object_name = src_object_name if src_object_name[0] != "/" else src_object_name

        def _download_object_helper(offset, **kwargs):
            blob_client = self.blob_service_client.get_blob_client(container=self.container_name, blob=src_object_name)
            # write file
            if not os.path.exists(dst_file_path):
                open(dst_file_path, "a").close()
            with open(dst_file_path, "rb+") as download_file:
                download_file.write(blob_client.download_blob(max_concurrency=self.max_concurrency).readall())

        return self.pool.submit(_download_object_helper, 0)

    def upload_object(self, src_file_path, dst_object_name, content_type="infer") -> Future:
        src_file_path, dst_object_name = str(src_file_path), str(dst_object_name)
        dst_object_name = dst_object_name if dst_object_name[0] != "/" else dst_object_name
        os.path.getsize(src_file_path)

        def _upload_object_helper():
            blob_client = self.blob_service_client.get_blob_client(container=self.container_name, blob=dst_object_name)
            with open(src_file_path, "rb") as data:
                # max_concurrency useless for small files
                blob_client.upload_blob(data=data, overwrite=True, max_concurrency=self.max_concurrency)
            return True

        return self.pool.submit(_upload_object_helper)
