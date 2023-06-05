import base64
import hashlib
import os
import boto3
from functools import lru_cache

from typing import Iterator, List, Optional, Tuple

from skyplane import exceptions, compute
from skyplane.exceptions import NoSuchObjectException
from skyplane.obj_store.object_store_interface import ObjectStoreInterface, ObjectStoreObject
from skyplane.config_paths import cloud_config
from skyplane.utils import logger, imports
from skyplane.utils.generator import batch_generator

from skyplane.config_paths import config_path
from skyplane.config import SkyplaneConfig


class R2Object(ObjectStoreObject):
    def full_path(self):
        account_name, bucket_name = self.bucket.split("/")
        return os.path.join(f"https://{account_name}.r2.cloudflarestorage.com", bucket_name, self.key)


class R2Interface(ObjectStoreInterface):
    def __init__(self, account_id: str, bucket_name: str):
        self.config = SkyplaneConfig.load_config(config_path)
        self.endpoint_url = f"https://{account_id}.r2.cloudflarestorage.com"
        try:
            self._s3_client = boto3.client(
                "s3",
                endpoint_url=self.endpoint_url,
                aws_access_key_id=self.config.cloudflare_access_key_id,
                aws_secret_access_key=self.config.cloudflare_secret_access_key,
            )
        except Exception as e:
            raise ValueError("Error with connecting to {self.endpoint_url}: {e}")
        self.requester_pays = False
        self.bucket_name = bucket_name
        self.account_id = account_id
        self.provider = "cloudflare"

    def path(self):
        return f"{self.endpoint_url}/{self.bucket_name}"

    def region_tag(self):
        # no regions in cloudflare
        return "cloudflare:infer"

    def bucket(self) -> str:
        return f"{self.account_id}/{self.bucket_name}"

    def set_requester_bool(self, requester: bool):
        self.requester_pays = requester

    @imports.inject("botocore.exceptions", pip_extra="aws")
    def bucket_exists(botocore_exceptions, self, region=None):
        # for bucket in self._s3_client.buckets.all():
        #    if bucket.name == self.bucket_name:
        #        return True
        # return False
        try:
            requester_pays = {"RequestPayer": "requester"} if self.requester_pays else {}
            self._s3_client.list_objects_v2(Bucket=self.bucket_name, MaxKeys=1, **requester_pays)
            return True
        except botocore_exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchBucket" or e.response["Error"]["Code"] == "AccessDenied":
                return False
            raise

    def create_bucket(self, aws_region):
        if not self.bucket_exists(aws_region):
            if aws_region == "us-east-1":
                self._s3_client.create_bucket(Bucket=self.bucket_name)
            else:
                self._s3_client.create_bucket(Bucket=self.bucket_name, CreateBucketConfiguration={"LocationConstraint": aws_region})
        else:
            logger.warning(f"Bucket {self.bucket} in region {aws_region} already exists")

    def delete_bucket(self):
        # delete 1000 keys at a time
        for batch in batch_generator(self.list_objects(), 1000):
            self.delete_objects([obj.key for obj in batch])
        assert len(list(self.list_objects())) == 0, f"Bucket not empty after deleting all keys {list(self.list_objects())}"
        # delete bucket
        self._s3_client.delete_bucket(Bucket=self.bucket_name)

    def list_objects(self, prefix="", region=None) -> Iterator[R2Object]:
        # for obj in self._s3_client.Bucket(self.bucket).objects.all():
        #    yield R2Object(
        #        obj["Key"],
        #        provider="cloudflare",
        #        bucket=self.bucket(),
        #        size=obj["Size"],
        #        last_modified=obj["LastModified"],
        #        mime_type=obj.get("ContentType"),
        #    )

        paginator = self._s3_client.get_paginator("list_objects_v2")
        requester_pays = {"RequestPayer": "requester"} if self.requester_pays else {}
        page_iterator = paginator.paginate(Bucket=self.bucket_name, Prefix=prefix, **requester_pays)
        for page in page_iterator:
            objs = []
            for obj in page.get("Contents", []):
                objs.append(
                    R2Object(
                        obj["Key"],
                        provider="aws",
                        bucket=self.bucket(),
                        size=obj["Size"],
                        last_modified=obj["LastModified"],
                        mime_type=obj.get("ContentType"),
                    )
                )
            yield from objs

    def delete_objects(self, keys: List[str]):
        while keys:
            batch, keys = keys[:1000], keys[1000:]  # take up to 1000 keys at a time
            self._s3_client.delete_objects(Bucket=self.bucket_name, Delete={"Objects": [{"Key": k} for k in batch]})

    @lru_cache(maxsize=1024)
    @imports.inject("botocore.exceptions", pip_extra="aws")
    def get_obj_metadata(botocore_exceptions, self, obj_name):
        try:
            return self._s3_client.head_object(Bucket=self.bucket_name, Key=str(obj_name))
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

        assert len(src_object_name) > 0, f"Source object name must be non-empty: '{src_object_name}'"
        args = {"Bucket": self.bucket_name, "Key": src_object_name}
        assert not (offset_bytes and not size_bytes), f"Cannot specify {offset_bytes} without {size_bytes}"
        if offset_bytes is not None and size_bytes is not None:
            args["Range"] = f"bytes={offset_bytes}-{offset_bytes + size_bytes - 1}"
        if self.requester_pays:
            args["RequestPayer"] = "requester"
        response = self._s3_client.get_object(**args)

        # write response data
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
        return mime_type, md5

    @imports.inject("botocore.exceptions", pip_extra="aws")
    def upload_object(
        botocore_exceptions, self, src_file_path, dst_object_name, part_number=None, upload_id=None, check_md5=None, mime_type=None
    ):
        dst_object_name, src_file_path = str(dst_object_name), str(src_file_path)
        assert len(dst_object_name) > 0, f"Destination object name must be non-empty: '{dst_object_name}'"
        b64_md5sum = base64.b64encode(check_md5).decode("utf-8") if check_md5 else None
        checksum_args = dict(ContentMD5=b64_md5sum) if b64_md5sum else dict()

        try:
            with open(src_file_path, "rb") as f:
                if upload_id:
                    self._s3_client.upload_part(
                        Body=f,
                        Key=dst_object_name,
                        Bucket=self.bucket_name,
                        PartNumber=part_number,
                        UploadId=upload_id.strip(),  # TODO: figure out why whitespace gets added,
                        **checksum_args,
                    )
                else:
                    mime_args = dict(ContentType=mime_type) if mime_type else dict()
                    self._s3_client.put_object(Body=f, Key=dst_object_name, Bucket=self.bucket_name, **checksum_args, **mime_args)
        except botocore_exceptions.ClientError as e:
            # catch MD5 mismatch error and raise appropriate exception
            if "Error" in e.response and "Code" in e.response["Error"] and e.response["Error"]["Code"] == "InvalidDigest":
                raise exceptions.ChecksumMismatchException(f"Checksum mismatch for object {dst_object_name}") from e
            raise

    def initiate_multipart_upload(self, dst_object_name: str, mime_type: Optional[str] = None) -> str:
        assert len(dst_object_name) > 0, f"Destination object name must be non-empty: '{dst_object_name}'"
        response = self._s3_client.create_multipart_upload(
            Bucket=self.bucket_name, Key=dst_object_name, **(dict(ContentType=mime_type) if mime_type else dict())
        )
        if "UploadId" in response:
            return response["UploadId"]
        else:
            raise exceptions.SkyplaneException(f"Failed to initiate multipart upload for {dst_object_name}: {response}")

    def complete_multipart_upload(self, dst_object_name, upload_id):
        all_parts = []
        while True:
            response = self._s3_client.list_parts(
                Bucket=self.bucket_name, Key=dst_object_name, MaxParts=100, UploadId=upload_id, PartNumberMarker=len(all_parts)
            )
            if "Parts" not in response:
                break
            else:
                if len(response["Parts"]) == 0:
                    break
                all_parts += response["Parts"]
        all_parts = sorted(all_parts, key=lambda d: d["PartNumber"])
        response = self._s3_client.complete_multipart_upload(
            UploadId=upload_id,
            Bucket=self.bucket_name,
            Key=dst_object_name,
            MultipartUpload={"Parts": [{"PartNumber": p["PartNumber"], "ETag": p["ETag"]} for p in all_parts]},
        )
        assert "ETag" in response, f"Failed to complete multipart upload for {dst_object_name}: {response}"
