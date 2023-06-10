import base64
from collections import defaultdict
import hashlib
import os
from functools import lru_cache

from typing import Iterator, List, Optional, Tuple

from skyplane import exceptions, compute
from skyplane.exceptions import NoSuchObjectException
from skyplane.obj_store.azure_storage_account_interface import AzureStorageAccountInterface
from skyplane.obj_store.object_store_interface import ObjectStoreInterface, ObjectStoreObject
from skyplane.utils import logger, imports

from azure.storage.blob import BlobServiceClient, BlobType


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

        self.block_ids_mapping = defaultdict(list)  # Keep tracks of the block_ids to stage for a blob multipart upload

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
                    blob.name,
                    provider="azure",
                    bucket=f"{self.account_name}/{blob.container}",
                    size=blob.size,
                    last_modified=blob.last_modified,
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
        if size_bytes is not None and offset_bytes is None:
            offset_bytes = 0
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
        src_file_path, dst_object_name = str(src_file_path), str(dst_object_name)
        blob_client = self.container_client.get_blob_client(dst_object_name)
        print(f"Uploading {src_file_path} to {dst_object_name}")

        try:
            # multipart upload
            if part_number is not None and upload_id is not None:
                with open(src_file_path, "rb") as f:
                    block_id = AzureBlobInterface._id_to_base64_encoding(part_number)
                    blob_client.stage_block(block_id=block_id, data=f, length=os.path.getsize(src_file_path))  # stage the block
                    self.block_ids_mapping[upload_id].append(block_id)

            # single upload
            with open(src_file_path, "rb") as f:
                blob_client.upload_blob(
                    name=dst_object_name,
                    data=f,
                    length=os.path.getsize(src_file_path),
                    max_concurrency=self.max_concurrency,
                    overwrite=True,
                    content_settings=azure_blob.ContentSettings(content_type=mime_type),
                )

            # check MD5 if required
            if check_md5:
                b64_md5sum = base64.b64encode(check_md5).decode("utf-8") if check_md5 else None
                blob_md5 = blob_client.get_blob_properties().properties.content_settings.content_md5
                if b64_md5sum != blob_md5:
                    raise exceptions.ChecksumMismatchException(
                        f"Checksum mismatch for object {dst_object_name} in Azure container {self.container_name}, "
                        + f"expected {b64_md5sum}, got {blob_md5}"
                    )
        except Exception as e:
            raise ValueError(f"Failed to upload {dst_object_name} to bucket {self.bucket_name} upload id {upload_id}: {e}")

    def initiate_multipart_upload(self, dst_object_name: str, mime_type: Optional[str] = None) -> str:
        """Azure does not have an equivalent function to return an upload ID like s3 and gcs do.
        Blocks in Azure are uploaded and associated with an ID, and can then be committed in a single operation to create the blob.
        We will just return the dst_object_name (blob name) as the "upload_id" to keep the return type consistent for the multipart thread.
        When the blocks/parts are uploaded/staged in the gateway, they are associated with the dst_object_name in the
        self.block_ids_mapping dictionary and will be commited together upon completion

        :param dst_object_name: name of the destination object, also our psuedo-uploadID
        :type dst_object_name: str
        :param mime_type: unused in this function but is kept for consistency with the other interfaces (default: None)
        :type mime_type: str
        """

        assert len(dst_object_name) > 0, f"Destination object name must be non-empty: '{dst_object_name}'"

        return dst_object_name

    def complete_multipart_upload(self, dst_object_name: str, upload_id: str) -> None:
        """After all blocks of a blob are uploaded/staged with their unique block_id and when the self.block_id_mappings
        is populated with these block_ids, in order to complete the multipart upload, we commit them together.

        :param dst_object_name: name of the destination object, also is used to index into our block mappings
        :type dst_object_name: str
        :param upload_id: upload_id to index into our block id mappings, should be the same as the dst_object_name in Azure
        :Type upload_id: str
        """

        assert upload_id == dst_object_name, "In Azure, upload_id should be the same as the blob name."

        # Fetch the list of block_ids
        # This list is populated while you are uploading blocks in the _run_multipart_chunk_thread method.
        block_list = self._get_block_list_for_upload(upload_id)

        # Check if block_list is empty
        if block_list == []:
            raise exceptions.SkyplaneException(
                f"There must be a non-empty block list to complete the multipart upload for {dst_object_name}"
            )

        blob_client = self.blob_service_client.get_blob_client(container=self.container_name, blob=dst_object_name)
        try:
            # The below operation will create the blob from the uploaded blocks.
            blob_client.commit_block_list(block_list)
        except Exception as e:
            raise exceptions.SkyplaneException(f"Failed to complete multipart upload for {dst_object_name}: {str(e)}")

    @staticmethod
    def _id_to_base64_encoding(id: int) -> str:
        """Azure expects all blockIDs to be Base64 strings. This function serves to convert the part numbers to
        base64-encoded strings. Used within upload_object

        :param id: part number of the block, determined while splitting the date into chunks before the transfer
        :type id: int
        """
        block_id = format(id, "06")  # pad with zeros to get consistent length
        block_id = block_id.encode("utf-8")
        block_id = base64.b64encode(block_id).decode("utf-8")
        return block_id

    def _get_block_list_for_upload(self, upload_id: str):
        """Gets the block id mappings for a particular destination object

        :param upload_id: upload_ids in Azure are the destionation object names
        :type upload_id: str
        """
        return self.block_ids_mapping.get(upload_id, [])


"""

More things to consider about this implementation:

Upload ID handling: Azure doesn't really have a concept equivalent to AWS's upload IDs. 
Instead, blobs are created immediately and blocks are associated with a blob via block IDs. 
My workaround of using the blob name as the upload ID should 
work as long as blob names are unique across all concurrent multi-part uploads. If not, 
this might experience issues with block ID mapping.

Block IDs: It's worth noting that Azure requires block IDs to be of the same length. 
I've appropriately handled this by formatting the IDs to be of length 6. If the part numbers 
exceed this length (i.e., I have more than 999999 parts), this might run into issues.

"""
