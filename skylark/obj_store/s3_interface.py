import boto3
from boto3.s3.transfer import TransferConfig
import botocore.exceptions

import os
import mimetypes
from typing import Iterator, List

from skylark.compute.aws.aws_auth import AWSAuthentication
from skylark.obj_store.object_store_interface import NoSuchObjectException, ObjectStoreInterface, ObjectStoreObject

class S3Object(ObjectStoreObject):
    def full_path(self):
        return f"s3://{self.bucket}/{self.key}"

class S3Interface(ObjectStoreInterface):
    def __init__(self, aws_region, bucket_name, use_tls=True, part_size=None, throughput_target_gbps=10, num_threads=4):
        self.auth = AWSAuthentication()
        self.aws_region = self.infer_s3_region(bucket_name) if aws_region is None or aws_region == "infer" else aws_region
        self.bucket_name = bucket_name

    def region_tag(self):
        return "aws:" + self.aws_region

    def infer_s3_region(self, bucket_name: str):
        s3_client = self.auth.get_boto3_client("s3")
        region = s3_client.get_bucket_location(Bucket=bucket_name).get("LocationConstraint", "us-east-1")
        return region if region is not None else "us-east-1"

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

    def list_objects(self, prefix="") -> Iterator[S3Object]:
        prefix = prefix if not prefix.startswith("/") else prefix[1:]
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

    def get_obj_metadata(self, obj_name):
        s3_resource = self.auth.get_boto3_resource("s3", self.aws_region).Bucket(self.bucket_name)
        try:
            return s3_resource.Object(str(obj_name))
        except botocore.exceptions.ClientError as e:
            raise NoSuchObjectException(f"Object {obj_name} does not exist, or you do not have permission to access it") from e

    def get_obj_size(self, obj_name):
        return self.get_obj_metadata(obj_name).content_length

    def exists(self, obj_name):
        try:
            self.get_obj_metadata(obj_name)
            return True
        except NoSuchObjectException:
            return False

    def download_object(self, src_object_name, dst_file_path, byte_offset=None, byte_count=None):
        src_object_name, dst_file_path = str(src_object_name), str(dst_file_path)
        src_object_name = "/" + src_object_name if src_object_name[0] != "/" else src_object_name
        s3_client = self.auth.get_boto3_client("s3", self.aws_region)
        parameters = {
                "Bucket": self.bucket_name,
                "Key": src_object_name
        }
        if byte_offset is None or byte_count is None:
            #unoptimized
            byte_offset = 0
        else:
            parameters["Range"] = f"bytes={byte_offset}-{byte_offset + byte_count - 1}"
        response = s3_client.get_object(**parameters)
        if not os.path.exists(dst_file_path):
            open(dst_file_path, "a").close()
        with open(dst_file_path, "rb+") as f:
            f.seek(byte_offset)
            f.write(response["Body"].read())
        response["Body"].close() 
        return response["ETag"] #might want to return bytes read instead

    def initiate_multipart_upload(self, dst_object_name, content_type):
        #cannot infer content type here
        dst_object_name = "/" + dst_object_name if dst_object_name[0] != "/" else dst_object_name
        s3_client = self.auth.get_boto3_client("s3", self.aws_region)
        response = s3_client.create_multipart_upload(
            Bucket=self.bucket_name,
            Key=dst_object_name,
            ContentType=content_type
        )
        return response["UploadId"]

    def upload_object(self, src_file_path, dst_object_name, upload_id=None, byte_offset=None, byte_count=None, part_number=None):
        assert part_number is None or 1 <= part_number <= 10000, f"invalid part_number {part_number}, should be in range [1, 10000]" 
        dst_object_name, src_file_path = str(dst_object_name), str(src_file_path)
        dst_object_name = "/" + dst_object_name if dst_object_name[0] != "/" else dst_object_name
        def _upload_object_part_helper():
            s3_client = self.auth.get_boto3_client("s3", self.aws_region)
            with open(src_file_path, mode="rb+") as f:
                f.seek(byte_offset)
                response = s3_client.upload_part(
                    UploadId=upload_id,
                    Bucket=self.bucket_name,
                    Key=dst_object_name,
                    PartNumber=part_number,
                    Body=f,
                    ContentLength=byte_count
            )
            return {"ETag": response["ETag"], "PartNumber": part_number} #user should build a list of these
             
        if upload_id is None or byte_offset is None or byte_count is None or part_number is None:
            #upload entire object
            inferred_type = mimetypes.guess_type(src_file_path)[0] or "application/octet-stream"
            upload_id = self.initiate_multipart_upload(dst_object_name, inferred_type)
            byte_offset = 0
            byte_count = os.path.getsize(src_file_path)
            part_number = 1
            part_list = [_upload_object_part_helper()]
            return self.finalize_multipart_upload(dst_object_name, upload_id, part_list)
        else:
            return _upload_object_part_helper()

    def finalize_multipart_upload(self, dst_object_name, upload_id, part_list):
        dst_object_name = "/" + dst_object_name if dst_object_name[0] != "/" else dst_object_name
        part_list.sort(key=lambda d: d["PartNumber"]) #list sorting is handled here, not left to user
        s3_client = self.auth.get_boto3_client("s3", self.aws_region)
        response = s3_client.complete_multipart_upload(
                UploadId=upload_id,
                Bucket=self.bucket_name,
                Key=dst_object_name,
                MultipartUpload={"Parts": part_list}
        )
        return response["ETag"]
