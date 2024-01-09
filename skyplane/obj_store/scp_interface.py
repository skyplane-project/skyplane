# (C) Copyright Samsung SDS. 2023

#

# Licensed under the Apache License, Version 2.0 (the "License");

# you may not use this file except in compliance with the License.

# You may obtain a copy of the License at

#

#     http://www.apache.org/licenses/LICENSE-2.0

#

# Unless required by applicable law or agreed to in writing, software

# distributed under the License is distributed on an "AS IS" BASIS,

# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

# See the License for the specific language governing permissions and

# limitations under the License.
import base64
import hashlib
import os
import time
import boto3

from functools import lru_cache, wraps
from typing import Iterator, Any, List, Optional, Tuple

from skyplane import exceptions, compute
from skyplane.exceptions import NoSuchObjectException
from skyplane.obj_store.object_store_interface import ObjectStoreInterface, ObjectStoreObject
from skyplane.config_paths import cloud_config
from skyplane.utils import logger, imports
from skyplane.compute.scp.scp_network import SCPNetwork
import skyplane.compute.scp.scp_utils as sc


def _retry(method, max_tries=60, backoff_s=1):
    @wraps(method)
    def method_with_retries(self, *args, **kwargs):
        try_count = 0
        while try_count < max_tries:
            try:
                return method(self, *args, **kwargs)
            except Exception as e:
                try_count += 1
                # print(e)
                # with open(f"/skyplane/scp_interface_{method.__name__}_{try_count}_error.txt", "w") as f:
                #     f.write(str(e))
                logger.fs.debug(f"retries: {method.__name__} - {e}, try_count : {try_count}")
                if try_count < max_tries:
                    time.sleep(backoff_s)
                else:
                    raise e

    return method_with_retries


class SCPObject(ObjectStoreObject):
    def full_path(self):
        return f"scp://{self.bucket}/{self.key}"


class SCPInterface(ObjectStoreInterface):
    def __init__(self, bucket_name: str):
        # OpenAPI Auth
        self.auth = compute.SCPAuthentication()
        self.scp_client = sc.SCPClient()

        self.bucket_name = bucket_name
        self.obsBucketId = self._get_bucket_id()
        self.obs_access_key = ""
        self.obs_secret_key = ""
        self.obs_ednpoint = ""
        self.obs_region = ""

        self.requester_pays = False
        self._cached_s3_clients = {}
        if self.obsBucketId is not None:
            self._set_s3_credentials()
            self._s3_client()

    @_retry
    def _set_s3_credentials(self):
        # if self.obsBucketId is None:
        max_retries = 60
        retries = 0
        while self.obsBucketId is None and retries < max_retries:
            self.obsBucketId = self._get_bucket_id()
            retries += 1
            time.sleep(0.25)
        # scp_client = self.scp_client
        if self.obs_access_key == "":
            # uri_path = f"/object-storage/v3/buckets/{self.obsBucketId}/api-info"
            uri_path = f"/object-storage/v4/buckets/{self.obsBucketId}/access-info"
            response = self.scp_client._get(uri_path, None)
            # print(response)
            try:
                self.obs_access_key = response["objectStorageBucketAccessKey"]
                self.obs_secret_key = response["objectStorageBucketSecretKey"]
                self.obs_ednpoint = response["objectStorageBucketPublicEndpointUrl"]
                # self.obs_region = response["serviceZoneId"]
                network = SCPNetwork(self.auth)
                self.obs_region = network.get_service_zoneName(response["serviceZoneId"])
            except Exception as e:
                if "An error occurred (AccessDenied) when calling the GetBucketLocation operation" in str(e):
                    logger.warning(f"Bucket location {self.bucket_name} is not public.")
                logger.warning(f"Specified bucket {self.bucket_name} does not exist, got SCP error: {e}")
                # print("Error getting SCP region", e)
                raise exceptions.MissingBucketException(f"SCP bucket {self.bucket_name} does not exist") from e

    @_retry
    def _s3_client(self, region=None):
        region = region if region is not None else self.obs_region
        if region not in self._cached_s3_clients:
            obsSession = boto3.Session(
                aws_access_key_id=self.obs_access_key,
                aws_secret_access_key=self.obs_secret_key,
                region_name=self.obs_region
                # config=Config(connect_timeout=timeout_seconds, read_timeout=timeout_seconds)
            )
            # s3_client = obsSession.client('s3', endpoint_url=self.obs_ednpoint)
            timeout_seconds = 120
            self._cached_s3_clients[region] = obsSession.client(
                "s3",
                endpoint_url=self.obs_ednpoint,
                config=boto3.session.Config(connect_timeout=timeout_seconds, read_timeout=timeout_seconds),
            )
        return self._cached_s3_clients[region]

    @property
    def provider(self):
        return "scp"

    def path(self):
        return f"scp://{self.bucket_name}"

    def region_tag(self):
        return "scp:" + self.scp_region

    def bucket(self) -> str:
        return self.bucket_name

    @property
    @lru_cache(maxsize=1)
    def scp_region(self):
        # scp_client = self.scp_client
        # obsBucketId = self._get_bucket_id()
        if self.obsBucketId is None:
            raise exceptions.MissingBucketException(f"SCP bucket {self.bucket_name} does not exist")

        default_region = cloud_config.get_flag("scp_default_region")
        # uri_path = f"/object-storage/v3/buckets/{self.obsBucketId}/api-info"
        uri_path = f"/object-storage/v4/buckets/{self.obsBucketId}/access-info"
        try:
            response = self.scp_client._get(uri_path, None)  # No value
            network = SCPNetwork(self.auth)
            region = network.get_service_zoneName(response["serviceZoneId"])
            return region if region is not None else default_region
        except Exception as e:
            if "An error occurred (AccessDenied) when calling the GetBucketLocation operation" in str(e):
                logger.warning(f"Bucket location {self.bucket_name} is not public. Assuming region is {default_region}")
                return default_region
            logger.warning(f"Specified bucket {self.bucket_name} does not exist, got SCP error: {e}")
            # print("Error getting SCP region", e)
            raise exceptions.MissingBucketException(f"SCP bucket {self.bucket_name} does not exist") from e

    def bucket_exists(self) -> bool:
        try:
            obsBuckets = self.bucket_lists()
            # pytype: disable=unsupported-operands
            for bucket in obsBuckets:
                if bucket["objectStorageBucketName"] == self.bucket_name:
                    return True
            return False
            # pytype: enable=unsupported-operands
        except Exception as e:
            logger.warning(f"Specified bucket {self.bucket_name} does not exist, got error: {e}")
            # print("Error getting bucket: ", e)
            return False

    def bucket_lists(self) -> List[str]:
        # scp_client = self.scp_client
        # uri_path = "/object-storage/v3/buckets?size=999"
        uri_path = "/object-storage/v4/buckets?size=999"
        response = self.scp_client._get(uri_path)
        return response

    def create_object_repr(self, key: str) -> SCPObject:
        return SCPObject(provider=self.provider, bucket=self.bucket(), key=key)

    def _get_bucket_id(self) -> Optional[str]:
        # url = f"/object-storage/v3/buckets?obsBucketName={self.bucket_name}"
        url = f"/object-storage/v4/buckets?objectStorageBucketName={self.bucket_name}"
        try:
            response = self.scp_client._get(url)
            # print(response)
            if len(response) == 0:
                return None
            else:
                return response[0]["objectStorageBucketId"]
        except Exception as e:
            logger.warning(f"Specified bucket {self.bucket_name} does not exist, got error: {e}")
            # print("Error getting bucket id : ", e)
            return None

    def get_objectstorage_id(self, zone_id=None):
        # scp_client = self.scp_client
        zone_id = zone_id if zone_id is not None else None
        # uri_path = f"/object-storage/v3/object-storages?zoneId={zone_id}"
        uri_path = f"/object-storage/v4/object-storages?serviceZoneId={zone_id}"

        response = self.scp_client._get(uri_path)
        return response[0]["objectStorageId"]

    def create_bucket(self, scp_region):
        if not self.bucket_exists():
            network = SCPNetwork(self.auth)
            zone_id = network.get_service_zone_id(scp_region)
            obs_id = self.get_objectstorage_id(zone_id)

            # uri_path = "/object-storage/v3/buckets"
            uri_path = "/object-storage/v4/buckets"
            req_body = {
                "objectStorageBucketAccessControlEnabled": "false",
                "objectStorageBucketFileEncryptionEnabled": "false",
                "objectStorageBucketName": self.bucket_name,
                "objectStorageBucketVersionEnabled": "false",
                "objectStorageId": obs_id,
                "productNames": ["Object Storage"],
                "serviceZoneId": zone_id,
                "tags": [{"tagKey": "skycomputing", "tagValue": "skyplane"}],
            }
            self.scp_client._post(uri_path, req_body)
            time.sleep(3)
        else:
            logger.warning(f"Bucket {self.bucket} in region {scp_region} already exists")

    def delete_bucket(self):
        if self.bucket_exists():
            # obsBucketId = self._get_bucket_id()
            # scp_client = self.scp_client
            # uri_path = f"/object-storage/v3/buckets/{self.obsBucketId}"
            uri_path = f"/object-storage/v4/buckets/{self.obsBucketId}"

            self.scp_client._delete(uri_path)
        else:
            logger.warning(f"Bucket {self.bucket} in region {self.scp_region} is not exists")

    # def list_objects(self, prefix: str = None, recursive: bool = False) -> Iterator[SCPObject]:
    @_retry
    def list_objects(self, prefix="", obs_region=None) -> Iterator[SCPObject]:
        self._set_s3_credentials()
        paginator = self._s3_client(self.obs_region).get_paginator("list_objects_v2")
        requester_pays = {"RequestPayer": "requester"} if self.requester_pays else {}
        page_iterator = paginator.paginate(Bucket=self.bucket_name, Prefix=prefix, **requester_pays)
        for page in page_iterator:
            objs = []
            for obj in page.get("Contents", []):
                objs.append(
                    SCPObject(
                        obj["Key"],
                        provider=self.provider,
                        bucket=self.bucket(),
                        size=obj["Size"],
                        last_modified=obj["LastModified"],
                        mime_type=obj.get("ContentType"),
                    )
                )
            yield from objs
            # return objs         # For Test

    def delete_objects(self, keys: List[str]):
        # self._set_s3_credentials()
        s3_client = self._s3_client()
        while keys:
            batch, keys = keys[:1000], keys[1000:]  # take up to 1000 keys at a time
            s3_client.delete_objects(Bucket=self.bucket_name, Delete={"Objects": [{"Key": k} for k in batch]})

    @lru_cache(maxsize=1024)
    @imports.inject("botocore.exceptions", pip_extra="aws")
    def get_obj_metadata(botocore_exceptions, self, obj_name):
        self._set_s3_credentials()
        s3_client = self._s3_client()
        try:
            return s3_client.head_object(Bucket=self.bucket_name, Key=str(obj_name))
        except botocore_exceptions.ClientError as e:
            raise NoSuchObjectException(f"Object {obj_name} does not exist, or you do not have permission to access it") from e

    def get_obj_size(self, obj_name):
        return self.get_obj_metadata(obj_name)["ContentLength"]

    def get_obj_last_modified(self, obj_name):
        return self.get_obj_metadata(obj_name)["LastModified"]

    def get_obj_mime_type(self, obj_name):
        return self.get_obj_metadata(obj_name)["ContentType"]

    def exists(self, obj_name):
        try:
            self.get_obj_metadata(obj_name)
            return True
        except NoSuchObjectException:
            return False

    def download_object(
        self,
        src_object_name,
        dst_file_path,
        offset_bytes=None,
        size_bytes=None,
        write_at_offset=False,
        generate_md5=False,
        write_block_size=2**16,
    ) -> Tuple[Optional[str], Optional[bytes]]:
        src_object_name, dst_file_path = str(src_object_name), str(dst_file_path)
        # self._set_s3_credentials()
        max_retries = 10
        retries = 0
        md5 = None
        mime_type = None
        while retries < max_retries:
            s3_client = self._s3_client()
            try:
                assert len(src_object_name) > 0, f"Source object name must be non-empty: '{src_object_name}'"
                args = {"Bucket": self.bucket_name, "Key": src_object_name}
                assert not (offset_bytes and not size_bytes), f"Cannot specify {offset_bytes} without {size_bytes}"
                if offset_bytes is not None and size_bytes is not None:
                    args["Range"] = f"bytes={offset_bytes}-{offset_bytes + size_bytes - 1}"
                if self.requester_pays:
                    args["RequestPayer"] = "requester"
                response = s3_client.get_object(**args)

                # write response data
                if os.path.exists(dst_file_path):
                    os.remove(dst_file_path)
                if not os.path.exists(dst_file_path):
                    open(dst_file_path, "a").close()
                if generate_md5:
                    m = hashlib.md5()
                with open(dst_file_path, "wb+" if write_at_offset else "wb") as f:
                    f.seek(offset_bytes if write_at_offset else 0)
                    b = response["Body"].read(write_block_size)
                    while b:
                        if generate_md5:
                            m.update(b)
                        f.write(b)
                        b = response["Body"].read(write_block_size)
                response["Body"].close()
                md5 = m.digest() if generate_md5 else None
                mime_type = response["ContentType"]
            except Exception as e:
                retries += 1
                if retries == max_retries:
                    raise
                else:
                    chunk_name = os.path.basename(dst_file_path)
                    logger.warning(f"Download failed_{chunk_name}, retrying ({retries}/{max_retries})")
                    # 이 위치에 에러 파일 만들기
                    with open(f"/skyplane/download_object_{chunk_name}_{retries}_{max_retries}_error.txt", "w") as f:
                        f.write(str(e))
                    time.sleep(1)
            else:
                break
        return mime_type, md5

    @imports.inject("botocore.exceptions", pip_extra="aws")
    def upload_object(
        botocore_exceptions, self, src_file_path, dst_object_name, part_number=None, upload_id=None, check_md5=None, mime_type=None
    ):
        dst_object_name, src_file_path = str(dst_object_name), str(src_file_path)
        self._set_s3_credentials()
        s3_client = self._s3_client()
        assert len(dst_object_name) > 0, f"Destination object name must be non-empty: '{dst_object_name}'"
        b64_md5sum = base64.b64encode(check_md5).decode("utf-8") if check_md5 else None
        checksum_args = dict(ContentMD5=b64_md5sum) if b64_md5sum else dict()

        max_retries = 10
        retries = 0
        while retries < max_retries:
            try:
                with open(src_file_path, "rb") as f:
                    if upload_id:
                        # if part_number is None: # Upload ALL parts
                        #     part_size = 10 * 1024 * 1024   #100MB
                        #     pn = 1
                        #     while True:
                        #         data = f.read(part_size)
                        #         if not len(data):
                        #             break
                        #         s3_client.upload_part(
                        #             Body=data,
                        #             Key=dst_object_name,
                        #             Bucket=self.bucket_name,
                        #             PartNumber=pn,
                        #             UploadId=upload_id.strip(),  # TODO: figure out why whitespace gets added,
                        #             **checksum_args,
                        #         )
                        #         pn += 1
                        # else:
                        s3_client.upload_part(
                            Body=f,
                            Key=dst_object_name,
                            Bucket=self.bucket_name,
                            PartNumber=part_number,
                            UploadId=upload_id.strip(),  # TODO: figure out why whitespace gets added,
                            **checksum_args,
                        )
                    else:
                        mime_args = dict(ContentType=mime_type) if mime_type else dict()
                        s3_client.put_object(Body=f, Key=dst_object_name, Bucket=self.bucket_name, **checksum_args, **mime_args)
            except botocore_exceptions.ClientError as e:
                retries += 1
                if retries == max_retries:
                    # catch MD5 mismatch error and raise appropriate exception
                    if "Error" in e.response and "Code" in e.response["Error"] and e.response["Error"]["Code"] == "InvalidDigest":
                        raise exceptions.ChecksumMismatchException(f"Checksum mismatch for object {dst_object_name}") from e
                    raise
                else:
                    logger.warning(f"Upload failed, retrying ({retries}/{max_retries})")
                    # 이 위치에 에러 파일 만들기
                    with open(f"/skyplane/upload_object_{dst_object_name}_{retries}_error.txt", "w") as f:
                        f.write(str(e))
                    time.sleep(1)
            else:
                break

    def initiate_multipart_upload(self, dst_object_name: str, mime_type: Optional[str] = None) -> str:
        self._set_s3_credentials()
        client = self._s3_client()
        assert len(dst_object_name) > 0, f"Destination object name must be non-empty: '{dst_object_name}'"
        response = client.create_multipart_upload(
            Bucket=self.bucket_name, Key=dst_object_name, **(dict(ContentType=mime_type) if mime_type else dict())
        )
        if "UploadId" in response:
            return response["UploadId"]
        else:
            raise exceptions.SkyplaneException(f"Failed to initiate multipart upload for {dst_object_name}: {response}")

    def complete_multipart_upload(self, dst_object_name, upload_id, metadata: Optional[Any] = None):
        self._set_s3_credentials()
        s3_client = self._s3_client()
        all_parts = []
        while True:
            response = s3_client.list_parts(
                Bucket=self.bucket_name, Key=dst_object_name, MaxParts=100, UploadId=upload_id, PartNumberMarker=len(all_parts)
            )
            if "Parts" not in response:
                break
            else:
                if len(response["Parts"]) == 0:
                    break
                all_parts += response["Parts"]
        all_parts = sorted(all_parts, key=lambda d: d["PartNumber"])
        response = s3_client.complete_multipart_upload(
            UploadId=upload_id,
            Bucket=self.bucket_name,
            Key=dst_object_name,
            MultipartUpload={"Parts": [{"PartNumber": p["PartNumber"], "ETag": p["ETag"]} for p in all_parts]},
        )
        assert "ETag" in response, f"Failed to complete multipart upload for {dst_object_name}: {response}"

    # Lists in-progress multipart uploads.
    @imports.inject("botocore.exceptions", pip_extra="aws")
    def list_multipart_uploads(self):
        self._set_s3_credentials()
        s3_client = self._s3_client()
        response = s3_client.list_multipart_uploads(Bucket=self.bucket())
        return response["Uploads"] if response is not None else None

    # Aborts a multipart upload. After a multipart upload is aborted, no additional parts can be uploaded using that upload ID
    @imports.inject("botocore.exceptions", pip_extra="aws")
    def abort_multipart_upload(self, key, upload_id):
        self._set_s3_credentials()
        s3_client = self._s3_client()
        response = s3_client.abort_multipart_upload(Bucket=self.bucket(), Key=key, UploadId=upload_id)
        return response
