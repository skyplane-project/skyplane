from functools import lru_cache
import base64
import hashlib
import os
from typing import Iterator, List, Optional

import botocore.exceptions

from skyplane import exceptions
from skyplane.compute.aws.aws_auth import AWSAuthentication
from skyplane.obj_store.object_store_interface import NoSuchObjectException, ObjectStoreInterface, ObjectStoreObject
from skyplane.utils import logger


class S3Object(ObjectStoreObject):
    def full_path(self):
        return f"s3://{self.bucket}/{self.key}"


class S3Interface(ObjectStoreInterface):
    def __init__(self, bucket_name: str, aws_region: str = "infer"):
        self.auth = AWSAuthentication()
        self.bucket_name = bucket_name
        self.aws_region = self.infer_s3_region(bucket_name) if aws_region == "infer" else aws_region

    def region_tag(self):
        return "aws:" + self.aws_region

    def infer_s3_region(self, bucket_name: str):
        s3_client = self.auth.get_boto3_client("s3")
        try:
            region = s3_client.get_bucket_location(Bucket=bucket_name).get("LocationConstraint", "us-east-1")
            return region if region is not None else "us-east-1"
        except Exception as e:
            if "An error occurred (AccessDenied) when calling the GetBucketLocation operation" in str(e):
                logger.error(f"Bucket location {bucket_name} is not public. Assuming region is us-east-1.")
                return "us-east-1"
            logger.error(f"Specified bucket {bucket_name} does not exist, got AWS error: {e}")
            raise exceptions.MissingBucketException(f"S3 bucket {bucket_name} does not exist") from e

    def bucket_exists(self):
        s3_client = self.auth.get_boto3_client("s3", self.aws_region)
        return self.bucket_name in [b["Name"] for b in s3_client.list_buckets()["Buckets"]]

    def create_bucket(self, premium_tier=True):
        s3_client = self.auth.get_boto3_client("s3", self.aws_region)
        if not self.bucket_exists():
            if self.aws_region == "us-east-1":
                s3_client.create_bucket(Bucket=self.bucket_name)
            else:
                s3_client.create_bucket(Bucket=self.bucket_name, CreateBucketConfiguration={"LocationConstraint": self.aws_region})
        assert self.bucket_exists()

    def delete_bucket(self):
        s3_client = self.auth.get_boto3_client("s3", self.aws_region)
        s3_client.delete_bucket(Bucket=self.bucket_name)

    def list_objects(self, prefix="") -> Iterator[S3Object]:
        s3_client = self.auth.get_boto3_client("s3", self.aws_region)
        paginator = s3_client.get_paginator("list_objects_v2")
        page_iterator = paginator.paginate(Bucket=self.bucket_name, Prefix=prefix)
        for page in page_iterator:
            for obj in page.get("Contents", []):
                yield S3Object("s3", self.bucket_name, obj["Key"], obj["Size"], obj["LastModified"])

    def delete_objects(self, keys: List[str]):
        s3_client = self.auth.get_boto3_client("s3", self.aws_region)
        while keys:
            batch, keys = keys[:1000], keys[1000:]  # take up to 1000 keys at a time
            s3_client.delete_objects(Bucket=self.bucket_name, Delete={"Objects": [{"Key": k} for k in batch]})

    @lru_cache(maxsize=1024)
    def get_obj_metadata(self, obj_name):
        s3_client = self.auth.get_boto3_client("s3", self.aws_region)
        try:
            return s3_client.head_object(Bucket=self.bucket_name, Key=str(obj_name))
        except botocore.exceptions.ClientError as e:
            raise NoSuchObjectException(f"Object {obj_name} does not exist, or you do not have permission to access it") from e

    def get_obj_size(self, obj_name):
        return self.get_obj_metadata(obj_name)["ContentLength"]

    def get_obj_last_modified(self, obj_name):
        return self.get_obj_metadata(obj_name)["LastModified"]

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
    ) -> Optional[bytes]:
        src_object_name, dst_file_path = str(src_object_name), str(dst_file_path)
        s3_client = self.auth.get_boto3_client("s3", self.aws_region)
        assert len(src_object_name) > 0, f"Source object name must be non-empty: '{src_object_name}'"

        if size_bytes:
            byte_range = f"bytes={offset_bytes}-{offset_bytes + size_bytes - 1}"
            response = s3_client.get_object(Bucket=self.bucket_name, Key=src_object_name, Range=byte_range)
        else:
            response = s3_client.get_object(
                Bucket=self.bucket_name,
                Key=src_object_name,
            )

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
        return m.digest() if generate_md5 else None

    def upload_object(self, src_file_path, dst_object_name, part_number=None, upload_id=None, check_md5=None):
        dst_object_name, src_file_path = str(dst_object_name), str(src_file_path)
        s3_client = self.auth.get_boto3_client("s3", self.aws_region)
        assert len(dst_object_name) > 0, f"Destination object name must be non-empty: '{dst_object_name}'"
        b64_md5sum = base64.b64encode(check_md5).decode("utf-8") if check_md5 else None
        checksum_args = dict(ContentMD5=b64_md5sum) if b64_md5sum else dict()

        try:
            with open(src_file_path, "rb") as f:
                if upload_id:
                    s3_client.upload_part(
                        Body=f,
                        Key=dst_object_name,
                        Bucket=self.bucket_name,
                        PartNumber=part_number,
                        UploadId=upload_id.strip(),  # TODO: figure out why whitespace gets added,
                        **checksum_args,
                    )
                else:
                    s3_client.put_object(
                        Body=f,
                        Key=dst_object_name,
                        Bucket=self.bucket_name,
                        **checksum_args,
                    )
        except botocore.exceptions.ClientError as e:
            # catch MD5 mismatch error and raise appropriate exception
            if "Error" in e.response and "Code" in e.response["Error"] and e.response["Error"]["Code"] == "InvalidDigest":
                raise exceptions.ChecksumMismatchException(f"Checksum mismatch for object {dst_object_name}") from e
            raise

    def initiate_multipart_upload(self, dst_object_name):
        # cannot infer content type here
        assert len(dst_object_name) > 0, f"Destination object name must be non-empty: '{dst_object_name}'"
        s3_client = self.auth.get_boto3_client("s3", self.aws_region)
        response = s3_client.create_multipart_upload(
            Bucket=self.bucket_name,
            Key=dst_object_name,
            # ContentType=content_type
        )
        return response["UploadId"]

    def complete_multipart_upload(self, dst_object_name, upload_id):
        s3_client = self.auth.get_boto3_client("s3", self.aws_region)

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

        # sort by part-number
        all_parts = sorted(all_parts, key=lambda d: d["PartNumber"])
        response = s3_client.complete_multipart_upload(
            UploadId=upload_id,
            Bucket=self.bucket_name,
            Key=dst_object_name,
            MultipartUpload={"Parts": [{"PartNumber": p["PartNumber"], "ETag": p["ETag"]} for p in all_parts]},
        )
        return True
