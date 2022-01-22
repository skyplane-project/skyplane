import mimetypes
import os
from concurrent.futures import Future
from typing import Iterator, List

#import botocore.exceptions
#from awscrt.auth import AwsCredentialsProvider
#from awscrt.http import HttpHeaders, HttpRequest
#from awscrt.io import ClientBootstrap, DefaultHostResolver, EventLoopGroup
#from awscrt.s3 import S3Client, S3RequestTlsMode, S3RequestType

from google.cloud import storage

from skylark.compute.aws.aws_server import AWSServer
from skylark.obj_store.object_store_interface import NoSuchObjectException, ObjectStoreInterface, ObjectStoreObject


class S3Object(ObjectStoreObject):
    def full_path(self):
        return f"s3://{self.bucket}/{self.key}"


class GCSInterface(ObjectStoreInterface):
    def __init__(self, gcp_region, bucket_name, use_tls=True, part_size=None, throughput_target_gbps=None):
        # TODO: infer region?
        self.gcp_region = gcp_region

        self.bucket_name = bucket_name
        self.pending_downloads, self.completed_downloads = 0, 0
        self.pending_uploads, self.completed_uploads = 0, 0

        self.gcs_part_size = part_size
        self.gcs_throughput_target_gbps = throughput_target_gbps

        # TODO - figure out how paralllelism handled
        self._gcs_client = storage.Client()

    def _on_done_download(self, **kwargs):
        self.completed_downloads += 1
        self.pending_downloads -= 1

    def _on_done_upload(self, **kwargs):
        self.completed_uploads += 1
        self.pending_uploads -= 1

    #def infer_s3_region(self, bucket_name: str):
    #    s3_client = AWSServer.get_boto3_client("s3")
    #    region = s3_client.get_bucket_location(Bucket=bucket_name).get("LocationConstraint", "us-east-1")
    #    return region if region is not None else "us-east-1"

    def bucket_exists(self):
        try:
            bucket = self._gcs_client.get_bucket(self.bucket_name)
            return True
        except Exception as e: 
            print(e)
            return False

    def create_bucket(self):
        if not self.bucket_exists():
            bucket = self._gcs_client.bucket(self.bucket_name)
            bucket.storage_class = "COLDLINE" # TODO: which storage class?
            print(self.gcp_region)
            new_bucket = self._gcs_client.create_bucket(bucket, location=self.gcp_region)
        assert self.bucket_exists()

    #def list_objects(self, prefix="") -> Iterator[S3Object]:
    #    prefix = prefix if not prefix.startswith("/") else prefix[1:]
    #    s3_client = AWSServer.get_boto3_client("s3", self.aws_region)
    #    paginator = s3_client.get_paginator("list_objects_v2")
    #    page_iterator = paginator.paginate(Bucket=self.bucket_name, Prefix=prefix)
    #    for page in page_iterator:
    #        for obj in page.get("Contents", []):
    #            yield S3Object("s3", self.bucket_name, obj["Key"], obj["Size"], obj["LastModified"])

    #def delete_objects(self, keys: List[str]):
    #    s3_client = AWSServer.get_boto3_client("s3", self.aws_region)
    #    while keys:
    #        batch, keys = keys[:1000], keys[1000:]  # take up to 1000 keys at a time
    #        s3_client.delete_objects(Bucket=self.bucket_name, Delete={"Objects": [{"Key": k} for k in batch]})

    #def get_obj_metadata(self, obj_name):
    #    s3_resource = AWSServer.get_boto3_resource("s3", self.aws_region).Bucket(self.bucket_name)
    #    try:
    #        return s3_resource.Object(str(obj_name).lstrip("/"))
    #    except botocore.exceptions.ClientError as e:
    #        raise NoSuchObjectException(f"Object {obj_name} does not exist, or you do not have permission to access it") from e

    #def get_obj_size(self, obj_name):
    #    return self.get_obj_metadata(obj_name).content_length

    #def exists(self, obj_name):
    #    try:
    #        self.get_obj_metadata(obj_name)
    #        return True
    #    except NoSuchObjectException:
    #        return False

    ## todo: implement range request for download
    #def download_object(self, src_object_name, dst_file_path) -> Future:
    #    src_object_name, dst_file_path = str(src_object_name), str(dst_file_path)
    #    src_object_name = "/" + src_object_name if src_object_name[0] != "/" else src_object_name
    #    download_headers = HttpHeaders([("host", self.bucket_name + ".s3." + self.aws_region + ".amazonaws.com")])
    #    request = HttpRequest("GET", src_object_name, download_headers)

    #    def _on_body_download(offset, chunk, **kwargs):
    #        if not os.path.exists(dst_file_path):
    #            open(dst_file_path, "a").close()
    #        with open(dst_file_path, "rb+") as f:
    #            f.seek(offset)
    #            f.write(chunk)

    #    return self._s3_client.make_request(
    #        recv_filepath=dst_file_path,
    #        request=request,
    #        type=S3RequestType.GET_OBJECT,
    #        on_done=self._on_done_download,
    #        on_body=_on_body_download,
    #    ).finished_future

    #def upload_object(self, src_file_path, dst_object_name, content_type="infer") -> Future:
    #    print("uploading object", src_file_path, dst_object_name)
    #    src_file_path, dst_object_name = str(src_file_path), str(dst_object_name)
    #    dst_object_name = "/" + dst_object_name if dst_object_name[0] != "/" else dst_object_name
    #    content_len = os.path.getsize(src_file_path)
    #    if content_type == "infer":
    #        content_type = mimetypes.guess_type(src_file_path)[0] or "application/octet-stream"
    #    upload_headers = HttpHeaders()
    #    upload_headers.add("host", self.bucket_name + ".s3." + self.aws_region + ".amazonaws.com")
    #    upload_headers.add("Content-Type", content_type)
    #    upload_headers.add("Content-Length", str(content_len))
    #    request = HttpRequest("PUT", dst_object_name, upload_headers)
    #    return self._s3_client.make_request(
    #        send_filepath=src_file_path,
    #        request=request,
    #        type=S3RequestType.PUT_OBJECT,
    #        on_done=self._on_done_upload,
    #    ).finished_future
