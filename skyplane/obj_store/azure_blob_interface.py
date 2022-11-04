import base64
import hashlib
import os
from functools import lru_cache

from typing import Iterator, List, Optional, Tuple

from skyplane import exceptions, compute
from skyplane.exceptions import NoSuchObjectException
from skyplane.obj_store.azure_storage_account_interface import AzureStorageAccountInterface
from skyplane.obj_store.object_store_interface import ObjectStoreInterface, ObjectStoreObject
from skyplane.utils import logger, imports


class AzureBlobObject(ObjectStoreObject):
    def full_path(self):
        account_name, container_name = self.bucket.split("/")
        return os.path.join(f"https://{account_name}.blob.core.windows.net", container_name, self.key)


class AzureBlobInterface(ObjectStoreInterface):
    def __init__(self, account_name: str, container_name: str, max_concurrency=1):
        self.auth = compute.AzureAuthentication()
        self.storage_account_interface = AzureStorageAccountInterface(account_name)
        self.account_name = account_name
        self.container_name = container_name
        self.max_concurrency = max_concurrency  # parallel upload/downloads, seems to cause issues if too high

    def path(self):
        return f"https://{self.account_name}.blob.core.windows.net/{self.container_name}"

    def region_tag(self):
        return f"azure:{self.storage_account_interface.azure_region}"

    def bucket(self) -> str:
        return f"{self.account_name}/{self.container_name}"

    @property
    def blob_service_client(self):
        return self.auth.get_blob_service_client(f"https://{self.account_name}.blob.core.windows.net")

    @property
    def container_client(self):
        return self.auth.get_container_client(f"https://{self.account_name}.blob.core.windows.net", self.container_name)

    @imports.inject("azure.core.exceptions", pip_extra="azure")
    def bucket_exists(exceptions, self):
        try:
            self.container_client.get_container_properties()
            return True
        except exceptions.ResourceNotFoundError:
            return False

    def exists(self, obj_name):
        return self.blob_service_client.get_blob_client(container=self.container_name, blob=obj_name).exists()

    @imports.inject("azure.core.exceptions", pip_extra="azure")
    def create_container(exceptions, self):
        try:
            self.container_client.create_container()
        except exceptions.ResourceExistsError:
            logger.warning(f"Unable to create container {self.container_name} as it already exists")

    def create_bucket(self, azure_region, resource_group=compute.AzureServer.resource_group_name, premium_tier=True):
        tier = "Premium_LRS" if premium_tier else "Standard_LRS"
        if not self.storage_account_interface.storage_account_exists_in_account():
            logger.debug(f"Creating storage account {self.account_name}")
            self.storage_account_interface.create_storage_account(azure_region, resource_group, tier)
        if not self.bucket_exists():
            logger.debug(f"Creating container {self.container_name}")
            self.create_container()

    @imports.inject("azure.core.exceptions", pip_extra="azure")
    def delete_container(exceptions, self):
        try:
            self.container_client.delete_container()
        except exceptions.ResourceNotFoundError:
            logger.warning("Unable to delete container as it doesn't exists")

    def delete_bucket(self):
        return self.delete_container()

    @imports.inject("azure.core.exceptions", pip_extra="azure")
    def list_objects(exceptions, self, prefix="") -> Iterator[AzureBlobObject]:
        blobs = self.container_client.list_blobs(name_starts_with=prefix)
        try:
            for blob in blobs:
                yield AzureBlobObject(
                    "azure",
                    f"{self.account_name}/{blob.container}",
                    blob.name,
                    blob.size,
                    blob.last_modified,
                    mime_type=getattr(blob.content_settings, "content_type", None),
                )
        except exceptions.HttpResponseError as e:
            if "AuthorizationPermissionMismatch" in str(e):
                logger.error(
                    f"Unable to list objects in container {self.container_name} as you don't have permission to access it. You need the 'Storage Blob Data Contributor' and 'Storage Account Contributor' roles: {e}"
                )
                raise e from None

    def delete_objects(self, keys: List[str]):
        for key in keys:
            blob_client = self.blob_service_client.get_blob_client(container=self.container_name, blob=key)
            blob_client.delete_blob()

    @lru_cache(maxsize=1024)
    @imports.inject("azure.core.exceptions", pip_extra="azure")
    def get_obj_metadata(exceptions, self, obj_name):
        blob_client = self.blob_service_client.get_blob_client(container=self.container_name, blob=obj_name)
        try:
            return blob_client.get_blob_properties()
        except exceptions.ResourceNotFoundError as e:
            raise NoSuchObjectException(f"Object {obj_name} does not exist, or you do not have permission to access it") from e

    def get_obj_size(self, obj_name):
        return self.get_obj_metadata(obj_name).size

    def get_obj_last_modified(self, obj_name):
        return self.get_obj_metadata(obj_name).last_modified

    def get_obj_mime_type(self, obj_name):
        return self.get_obj_metadata(obj_name).content_settings.content_type

    def download_object(
        self, src_object_name, dst_file_path, offset_bytes=None, size_bytes=None, write_at_offset=False, generate_md5=False
    ) -> Tuple[Optional[str], Optional[bytes]]:
        src_object_name, dst_file_path = str(src_object_name), str(dst_file_path)
        downloader = self.container_client.download_blob(
            src_object_name, offset=offset_bytes, length=size_bytes, max_concurrency=self.max_concurrency
        )

        if not os.path.exists(dst_file_path):
            open(dst_file_path, "a").close()
        if generate_md5:
            m = hashlib.md5()
        with open(dst_file_path, "wb+" if write_at_offset else "wb") as f:
            f.seek(offset_bytes if write_at_offset else 0)
            for b in downloader.chunks():
                if generate_md5:
                    m.update(b)
                f.write(b)
        md5 = m.digest() if generate_md5 else None
        mime_type = self.get_obj_metadata(src_object_name).content_settings.content_type
        return mime_type, md5

    @imports.inject("azure.storage.blob", pip_extra="azure")
    def upload_object(azure_blob, self, src_file_path, dst_object_name, part_number=None, upload_id=None, check_md5=None, mime_type=None):
        if part_number is not None or upload_id is not None:
            # todo implement multipart upload
            raise NotImplementedError("Multipart upload is not implemented for Azure")
        src_file_path, dst_object_name = str(src_file_path), str(dst_object_name)
        with open(src_file_path, "rb") as f:
            print(f"Uploading {src_file_path} to {dst_object_name}")
            blob_client = self.container_client.upload_blob(
                name=dst_object_name,
                data=f,
                length=os.path.getsize(src_file_path),
                max_concurrency=self.max_concurrency,
                overwrite=True,
                content_settings=azure_blob.ContentSettings(content_type=mime_type),
            )
        if check_md5:
            b64_md5sum = base64.b64encode(check_md5).decode("utf-8") if check_md5 else None
            blob_md5 = blob_client.get_blob_properties().properties.content_settings.content_md5
            if b64_md5sum != blob_md5:
                raise exceptions.ChecksumMismatchException(
                    f"Checksum mismatch for object {dst_object_name} in Azure container {self.container_name}, "
                    + f"expected {b64_md5sum}, got {blob_md5}"
                )
