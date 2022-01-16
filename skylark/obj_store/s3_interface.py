import mimetypes
import os
from concurrent.futures import Future
from typing import Iterator, List

import botocore.exceptions
from awscrt.auth import AwsCredentialsProvider
from awscrt.http import HttpHeaders, HttpRequest
from awscrt.io import ClientBootstrap, DefaultHostResolver, EventLoopGroup
from awscrt.s3 import S3Client, S3RequestTlsMode, S3RequestType

from skylark.compute.aws.aws_server import AWSServer
from skylark.obj_store.object_store_interface import NoSuchObjectException, ObjectStoreInterface, ObjectStoreObject



class S3Object(ObjectStoreObject):
    def full_path(self):
        return f"s3://{self.bucket}/{self.key}"


class S3Interface(ObjectStoreInterface):
    def __init__(self, aws_region, bucket_name, use_tls=True):
       
        # TODO: remove (debugging)
        # install tls thing? 
        import awscrt
        awscrt.io.init_logging(awscrt.io.LogLevel.Error, 'stderr')

        self.aws_region = self.infer_s3_region(bucket_name) if aws_region is None or aws_region == "infer" else aws_region
        self.bucket_name = bucket_name
        self.pending_downloads, self.completed_downloads = 0, 0
        self.pending_uploads, self.completed_uploads = 0, 0
        event_loop_group = EventLoopGroup(num_threads=os.cpu_count(), cpu_group=None)
        host_resolver = DefaultHostResolver(event_loop_group)
        bootstrap = ClientBootstrap(event_loop_group, host_resolver)

        # get aws auth info for docker envs
        aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID', None)
        aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY', None)
        if aws_access_key_id and aws_secret_access_key:
            credential_provider = AwsCredentialsProvider.new_static(aws_access_key_id, aws_secret_access_key)
        else: # use default
            credential_provider = AwsCredentialsProvider.new_default_chain(bootstrap)

        self._s3_client = S3Client(
            bootstrap=bootstrap,
            region=self.aws_region,
            credential_provider=credential_provider,
            throughput_target_gbps=100,
            part_size=None,
            tls_mode=S3RequestTlsMode.ENABLED if use_tls else S3RequestTlsMode.DISABLED,
        )

    def _on_done_download(self, **kwargs):
        self.completed_downloads += 1
        self.pending_downloads -= 1

    def _on_done_upload(self, **kwargs):
        self.completed_uploads += 1
        self.pending_uploads -= 1

    def infer_s3_region(self, bucket_name: str):
        s3_client = AWSServer.get_boto3_client("s3")
        region = s3_client.get_bucket_location(Bucket=bucket_name).get("LocationConstraint", "us-east-1")
        return region if region is not None else "us-east-1"

    def bucket_exists(self):
        s3_client = AWSServer.get_boto3_client("s3", self.aws_region)
        print("bucket name", self.bucket_name)
        print([b["Name"] for b in s3_client.list_buckets()["Buckets"]])
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
        s3_client.delete_objects(Bucket=self.bucket_name, Delete={"Objects": [{"Key": k} for k in keys]})

    def get_obj_metadata(self, obj_name):
        s3_client = AWSServer.get_boto3_client("s3", self.aws_region)
        try:
            return s3_client.head_object(Bucket=self.bucket_name, Key=str(obj_name).lstrip("/"))
        except botocore.exceptions.ClientError as e:
            raise NoSuchObjectException(f"Object {obj_name} does not exist, or you do not have permission to access it") from e

    def get_obj_size(self, obj_name):
        return self.get_obj_metadata(obj_name)["ContentLength"]

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
            on_done=self._on_done_download,
            on_body=_on_body_download,
        ).finished_future

    def upload_object(self, src_file_path, dst_object_name, content_type="infer") -> Future:
        print("uploading object", src_file_path, dst_object_name)
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
            send_filepath=src_file_path,
            request=request,
            type=S3RequestType.PUT_OBJECT,
            on_done=self._on_done_upload,
        ).finished_future
