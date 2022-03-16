import mimetypes
import os
from typing import Iterator, List

from concurrent.futures import Future
import botocore.exceptions
from awscrt.auth import AwsCredentialsProvider
from awscrt.http import HttpHeaders, HttpRequest
from awscrt.io import ClientBootstrap, DefaultHostResolver, EventLoopGroup
from awscrt.s3 import S3Client, S3RequestTlsMode, S3RequestType

from skylark.compute.aws.aws_server import AWSServer
from skylark.config import load_config
from skylark.obj_store.object_store_interface import NoSuchObjectException, ObjectStoreInterface, ObjectStoreObject


class S3Object(ObjectStoreObject):
    def full_path(self):
        return f"s3://{self.bucket}/{self.key}"


class S3Interface(ObjectStoreInterface):
    def __init__(self, aws_region, bucket_name, use_tls=True, part_size=None, throughput_target_gbps=100):
        self.aws_region = self.infer_s3_region(bucket_name) if aws_region is None or aws_region == "infer" else aws_region
        self.bucket_name = bucket_name
        num_threads = 4
        event_loop_group = EventLoopGroup(num_threads=num_threads, cpu_group=None)
        host_resolver = DefaultHostResolver(event_loop_group)
        bootstrap = ClientBootstrap(event_loop_group, host_resolver)
        # Authenticate
        config = load_config()
        aws_access_key_id = config["aws_access_key_id"]
        aws_secret_access_key = config["aws_secret_access_key"]
        if aws_access_key_id and aws_secret_access_key:
            credential_provider = AwsCredentialsProvider.new_static(aws_access_key_id, aws_secret_access_key)
        else:  # use default
            credential_provider = AwsCredentialsProvider.new_default_chain(bootstrap)

        self._s3_client = S3Client(
            bootstrap=bootstrap,
            region=self.aws_region,
            credential_provider=credential_provider,
            throughput_target_gbps=throughput_target_gbps,
            part_size=part_size,
            tls_mode=S3RequestTlsMode.ENABLED if use_tls else S3RequestTlsMode.DISABLED,
        )

    @staticmethod
    def infer_s3_region(bucket_name: str):
        s3_client = AWSServer.get_boto3_client("s3")
        region = s3_client.get_bucket_location(Bucket=bucket_name).get("LocationConstraint", "us-east-1")
        return region if region is not None else "us-east-1"

    def bucket_exists(self):
        s3_client = AWSServer.get_boto3_client("s3", self.aws_region)
        return self.bucket_name in [b["Name"] for b in s3_client.list_buckets()["Buckets"]]

    def create_bucket(self):
        s3_client = AWSServer.get_boto3_client("s3", self.aws_region)
        if not self.bucket_exists():
            if self.aws_region == "us-east-1":
                s3_client.create_bucket(Bucket=self.bucket_name)
            else:
                s3_client.create_bucket(Bucket=self.bucket_name, CreateBucketConfiguration={"LocationConstraint": self.aws_region})
        assert self.bucket_exists()

    def list_objects(self, prefix="") -> Iterator[S3Object]:
        prefix = prefix if not prefix.startswith("/") else prefix[1:]
        s3_client = AWSServer.get_boto3_client("s3", self.aws_region)
        paginator = s3_client.get_paginator("list_objects_v2")
        page_iterator = paginator.paginate(Bucket=self.bucket_name, Prefix=prefix)
        for page in page_iterator:
            for obj in page.get("Contents", []):
                yield S3Object("s3", self.bucket_name, obj["Key"], obj["Size"], obj["LastModified"])

    def delete_objects(self, keys: List[str]):
        s3_client = AWSServer.get_boto3_client("s3", self.aws_region)
        while keys:
            batch, keys = keys[:1000], keys[1000:]  # take up to 1000 keys at a time
            s3_client.delete_objects(Bucket=self.bucket_name, Delete={"Objects": [{"Key": k} for k in batch]})

    def get_obj_metadata(self, obj_name):
        s3_resource = AWSServer.get_boto3_resource("s3", self.aws_region).Bucket(self.bucket_name)
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

    # todo: implement range request for download
    def download_object(self, src_object_name, dst_file_path) -> Future:
        src_object_name, dst_file_path = str(src_object_name), str(dst_file_path)
        src_object_name = "/" + src_object_name if src_object_name[0] != "/" else src_object_name
        download_headers = HttpHeaders([("host", self.bucket_name + ".s3." + self.aws_region + ".amazonaws.com")])
        request = HttpRequest("GET", src_object_name, download_headers)

        def _on_body_download(offset, chunk, **kwargs):
            if not os.path.exists(dst_file_path):
                open(dst_file_path, "a").close()
            with open(dst_file_path, "rb+") as f:
                f.seek(offset)
                f.write(chunk)

        return self._s3_client.make_request(
            recv_filepath=dst_file_path,
            request=request,
            type=S3RequestType.GET_OBJECT,
        ).finished_future

    def upload_object(self, src_file_path, dst_object_name, content_type="infer") -> Future:
        src_file_path, dst_object_name = str(src_file_path), str(dst_object_name)
        dst_object_name = "/" + dst_object_name if dst_object_name[0] != "/" else dst_object_name
        content_len = os.path.getsize(src_file_path)
        if content_type == "infer":
            content_type = mimetypes.guess_type(src_file_path)[0] or "application/octet-stream"
        upload_headers = HttpHeaders()
        upload_headers.add("host", self.bucket_name + ".s3." + self.aws_region + ".amazonaws.com")
        upload_headers.add("Content-Type", content_type)
        upload_headers.add("Content-Length", str(content_len))
        request = HttpRequest("PUT", dst_object_name, upload_headers)
        return self._s3_client.make_request(
            send_filepath=src_file_path, request=request, type=S3RequestType.PUT_OBJECT
        ).finished_future
