import boto3
from boto3.s3.transfer import TransferConfig
import botocore.exceptions
import os
from typing import Iterator, List
from skylark.compute.aws.aws_auth import AWSAuthentication
from skylark.obj_store.object_store_interface import NoSuchObjectException, ObjectStoreInterface, ObjectStoreObject

class S3Object(ObjectStoreObject):
    def full_path(self):
        return f"s3://{self.bucket}/{self.key}"

class S3Interface(ObjectStoreInterface):
    """
    @property
    def client(self):
        return self.auth.get_boto3_client("s3", self.aws_region)

    @property
    def resource(self):
        return self.auth.get_boto3_resource("s3", self.aws_region)

    @staticmethod
    def sanitize_targets(object_name, file_path=""):
        object_name = "/" + object_name if object_name[0] != "/" else object_name
        return (str(object_name), str(file_path)) if file_path else str(object_name)
    """
    #thoughts? could reduce repetition by a little but may be superfluous
    #I think sanitize_targets would be useful in other places as well, this might not be the best place to define it

    def __init__(self, aws_region, bucket_name, use_tls=True, part_size=None, throughput_target_gbps=10, num_threads=4):
        self.auth = AWSAuthentication()
        self.aws_region = self.infer_s3_region(bucket_name) if aws_region is None or aws_region == "infer" else aws_region
        self.bucket_name = bucket_name

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
            return s3_resource.Object(str(obj_name).lstrip("/"))
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

    def download_part(self, src_object_name, dst_file_path, byte_offset, byte_count) -> str:
        src_object_name, dst_file_path = str(src_object_name), str(dst_file_path)
        src_object_name = "/" + src_object_name if src_object_name[0] != "/" else src_object_name
        s3_client = self.auth.get_boto3_client("s3", self.aws_region)
        response = s3_client.get_object(
            Bucket=self.bucket_name,
            Key=src_object_name,
            Range=f"bytes={byte_offset}-{byte_offset + byte_count - 1}"
        )
        if not os.path.exists(dst_file_path):
            open(dst_file_path, "a").close()
        with open(dst_file_path, "rb+") as f:
            f.seek(byte_offset)
            f.write(response["Body"].read())
        response["Body"].close() 
        return response["ETag"] #might want to return bytes read instead

    def download_entire_object(self, src_object_name, dst_file_path, config: TransferConfig=None) -> str:
        src_object_name, dst_file_path = str(src_object_name), str(dst_file_path)
        src_object_name = "/" + src_object_name if src_object_name[0] != "/" else src_object_name
        if not config:
            config = TransferConfig(
                #fine-tune this
                use_threads = True
            )
        s3_resource = self.auth.get_boto3_resource("s3", self.aws_region)
        response = s3_resource.download_file(dst_file_path, self.bucket_name, src_object_name, config)
        return response["ETag"] #might want to return bytes read instead

    def initiate_multipart_upload(self, dst_object_name, content_type) -> str:
        #cannot infer content type here
        dst_object_name = "/" + dst_object_name if dst_object_name[0] != "/" else dst_object_name
        s3_client = self.auth.get_boto3_client("s3", self.aws_region)
        response = s3_client.create_multipart_upload(
            Bucket=self.bucket_name,
            Key=dst_object_name,
            ContentType=content_type
        )
        return response["UploadID"]

    def upload_part(self, upload_id, src_file_path, dst_object_name, byte_offset, byte_count, part_number) -> dict:
        #all parts excluding the final one must be 5MB or larger
        assert 1 <= part_number <= 10000, f"invalid part_number {part_number}, should be in range [1, 10000]" 
        dst_object_name, src_file_path = str(dst_object_name), str(src_file_path)
        dst_object_name = "/" + dst_object_name if dst_object_name[0] != "/" else dst_object_name
        s3_client = self.auth.get_boto3_client("s3", self.aws_region)
        with open(src_file_path, mode="rb+") as f:
            f.seek(byte_offset)
            response = s3_client.upload_part(
                UploadId=part_upload_id,
                Bucket=self.bucket_name,
                Key=dst_object_name,
                PartNumber=part_number,
                Body=f,
                ContentLength=byte_count
                #checksum support could be added here
            )
        return {"ETag": response["ETag"], "PartNumber": part_number} #user should build a list of these

    def finalize_multipart_upload(self, upload_id, dst_object_name, part_list) -> str:
        dst_object_name = "/" + dst_object_name if dst_object_name[0] != "/" else dst_object_name
        part_list.sort(key=lambda d: d["PartNumber"]) #list sorting is handled here, not left to user
        s3_client = self.auth.get_boto3_client("s3", self.aws_region)
        response = s3_client.complete_multipart_upload(
                UploadID=upload_id,
                Bucket=self.bucket_name,
                Key=dst_object_name,
                MultipartUpload=part_list
        )
        return response["ETag"]

    def upload_entire_object(self, src_file_path, dst_object_name, config: TransferConfig=None) -> str:
        dst_object_name, src_file_path = str(dst_object_name), str(src_file_path)
        dst_object_name = "/" + dst_object_name if dst_object_name[0] != "/" else dst_object_name
        if not config:
            config = TransferConfig(
                #fine-tune this
                use_threads = True
            )
        s3_resource = self.auth.get_boto3_resource("s3", self.aws_region)
        response = s3_resource.upload_file(src_file_path, self.bucket_name, dst_object_name, config)
        return response["ETag"]
