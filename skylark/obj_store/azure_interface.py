import os
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Iterator, List
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
from azure.identity import DefaultAzureCredential, ClientSecretCredential
from azure.storage.blob import BlobServiceClient
from skylark.config import load_config
from skylark.utils import logger
from skylark.obj_store.azure_keys import azure_storage_credentials
from skylark.obj_store.object_store_interface import NoSuchObjectException, ObjectStoreInterface, ObjectStoreObject


class AzureObject(ObjectStoreObject):
    def full_path(self):
        raise NotImplementedError()


class AzureInterface(ObjectStoreInterface):
    def __init__(self, azure_region, container_name):
        # TODO: the azure region should get corresponding os.getenv()
        self.azure_region = azure_region
        self.container_name = container_name
        self.bucket_name = self.container_name  # For compatibility
        self.pending_downloads, self.completed_downloads = 0, 0
        self.pending_uploads, self.completed_uploads = 0, 0
        # Authenticate
        config = load_config()
        self.subscription_id = config["azure_subscription_id"]
        self.credential = ClientSecretCredential(
            tenant_id=config["azure_tenant_id"],
            client_id=config["azure_client_id"],
            client_secret=config["azure_client_secret"],
        )
        # Create a blob service client
        self._connect_str = azure_storage_credentials[self.azure_region]["connection_string"]
        self.account_url = "https://{}.blob.core.windows.net".format("skylark" + self.azure_region)
        self.blob_service_client = BlobServiceClient(account_url=self.account_url, credential=self.credential)

        self.pool = ThreadPoolExecutor(max_workers=256)  # TODO: This might need some tuning
        self.max_concurrency = 1
        self.container_client = None
        if not self.container_exists():
            self.create_container()
            logger.info(f"==> Creating Azure container {self.container_name}")

    def _on_done_download(self, **kwargs):
        self.completed_downloads += 1
        self.pending_downloads -= 1

    def _on_done_upload(self, **kwargs):
        self.completed_uploads += 1
        self.pending_uploads -= 1

    def container_exists(self):  # More like "is container empty?"
        # Get a client to interact with a specific container - though it may not yet exist
        if self.container_client is None:
            self.container_client = self.blob_service_client.get_container_client(self.container_name)
        try:
            for blob in self.container_client.list_blobs():
                return True
        except ResourceNotFoundError:
            return False

    def create_container(self):
        try:
            self.container_client = self.blob_service_client.create_container(self.container_name)
            self.properties = self.container_client.get_container_properties()
        except ResourceExistsError:
            logger.warning("==> Container might already exist, in which case blobs are re-written")
            # logger.warning("==> Alternatively use a diff bucket name with `--bucket-prefix`")
            return

    def create_bucket(self):
        return self.create_container()

    def delete_container(self):
        if self.container_client is None:
            self.container_client = self.blob_service_client.get_container_client(self.container_name)
        try:
            self.container_client.delete_container()
        except ResourceNotFoundError:
            logger.warning("Container doesn't exists. Unable to delete")

    def delete_bucket(self):
        return self.delete_container()

    def list_objects(self, prefix="") -> Iterator[AzureObject]:
        if self.container_client is None:
            self.container_client = self.blob_service_client.get_container_client(self.container_name)
        blobs = self.container_client.list_blobs()
        for blob in blobs:
            yield AzureObject("azure", blob.container, blob.name, blob.size, blob.last_modified)

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
