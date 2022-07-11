import base64
import hashlib
import os
import subprocess
import time
import uuid
from functools import lru_cache, partial
from typing import Iterator, List, Optional

from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError, HttpResponseError
from azure.identity import AzureCliCredential
from azure.mgmt.authorization.models import RoleAssignmentCreateParameters, RoleAssignmentProperties

from skyplane import exceptions, is_gateway_env
from skyplane.compute.azure.azure_auth import AzureAuthentication
from skyplane.compute.azure.azure_server import AzureServer
from skyplane.obj_store.object_store_interface import NoSuchObjectException, ObjectStoreInterface, ObjectStoreObject
from skyplane.utils import logger
from skyplane.utils.timer import Timer


class AzureObject(ObjectStoreObject):
    def full_path(self):
        account_name, container_name = self.bucket.split("/")
        return os.path.join(f"https://{account_name}.blob.core.windows.net", container_name, self.key)


class AzureInterface(ObjectStoreInterface):
    def __init__(self, account_name: str, container_name: str, region: str = "infer", max_concurrency=1):
        self.auth = AzureAuthentication()
        self.account_name = account_name
        self.container_name = container_name
        self.account_url = f"https://{self.account_name}.blob.core.windows.net"
        self.max_concurrency = max_concurrency  # parallel upload/downloads, seems to cause issues if too high
        self.azure_region = self.query_storage_account(self.account_name).location if region == "infer" else region

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

    def container_exists(self):
        try:
            self.container_client.get_container_properties()
            return True
        except ResourceNotFoundError:
            return False

    def bucket_exists(self):
        return self.storage_account_exists() and self.container_exists()

    def exists(self, obj_name):
        return self.blob_service_client.get_blob_client(container=self.container_name, blob=obj_name).exists()

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
        blobs = self.container_client.list_blobs(name_starts_with=prefix)
        for blob in blobs:
            yield AzureObject("azure", f"{self.account_name}/{blob.container}", blob.name, blob.size, blob.last_modified)

    def delete_objects(self, keys: List[str]):
        for key in keys:
            blob_client = self.blob_service_client.get_blob_client(container=self.container_name, blob=key)
            blob_client.delete_blob()

    @lru_cache(maxsize=1024)
    def get_obj_metadata(self, obj_name):
        blob_client = self.blob_service_client.get_blob_client(container=self.container_name, blob=obj_name)
        try:
            return blob_client.get_blob_properties()
        except ResourceNotFoundError as e:
            raise NoSuchObjectException(f"Object {obj_name} does not exist, or you do not have permission to access it") from e

    def get_obj_size(self, obj_name):
        return self.get_obj_metadata(obj_name).size

    def get_obj_last_modified(self, obj_name):
        return self.get_obj_metadata(obj_name).last_modified

    @staticmethod
    def _run_azure_op_with_retry(fn, interval=0.5, timeout=180):
        try:
            return fn()
        except HttpResponseError as e:
            # catch permissions errors if in a gateway environment and auto-retry after 5 seconds as it takes time for Azure to propogate role assignments
            if "This request is not authorized to perform this operation using this permission." in str(e) and is_gateway_env:
                logger.fs.warning("Unable to download object as you do not have permission to access it")
                # permission hasn't propagated yet, retry up to 180s
                with Timer("Wait for role assignment to propagate") as timer:
                    while timer.elapsed < timeout:
                        time.sleep(interval)
                        try:
                            logger.fs.error(f"Retrying object operation after {timer.elapsed:.2}s")
                            return fn()
                        except HttpResponseError as e:
                            if (
                                "This request is not authorized to perform this operation using this permission." in str(e)
                                and is_gateway_env
                            ):
                                logger.fs.error(f"Azure waiting for roles to propogate, retry failed after {timer.elapsed:.2}s")
                                continue
            raise

    def download_object(
        self,
        src_object_name,
        dst_file_path,
        offset_bytes=None,
        size_bytes=None,
        write_at_offset=False,
        generate_md5=False,
    ) -> Optional[bytes]:
        src_object_name, dst_file_path = str(src_object_name), str(dst_file_path)
        downloader = self._run_azure_op_with_retry(
            partial(
                self.container_client.download_blob,
                src_object_name,
                offset=offset_bytes,
                length=size_bytes,
                max_concurrency=self.max_concurrency,
            )
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

        return m.digest() if generate_md5 else None

    def upload_object(self, src_file_path, dst_object_name, part_number=None, upload_id=None, check_md5=None):
        if part_number is not None or upload_id is not None:
            # todo implement multipart upload
            raise NotImplementedError("Multipart upload is not implemented for Azure")
        src_file_path, dst_object_name = str(src_file_path), str(dst_object_name)
        with open(src_file_path, "rb") as f:
            blob_client = self._run_azure_op_with_retry(
                partial(
                    self.container_client.upload_blob,
                    name=dst_object_name,
                    data=f,
                    length=os.path.getsize(src_file_path),
                    max_concurrency=self.max_concurrency,
                    overwrite=True,
                )
            )
        if check_md5:
            b64_md5sum = base64.b64encode(check_md5).decode("utf-8") if check_md5 else None
            blob_md5 = blob_client.get_blob_properties().properties.content_settings.content_md5
            if b64_md5sum != blob_md5:
                raise exceptions.ChecksumMismatchException(
                    f"Checksum mismatch for object {dst_object_name} in bucket {self.bucket_name}, "
                    + f"expected {b64_md5sum}, got {blob_md5}"
                )
