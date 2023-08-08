import os
import boto3

from typing import Iterator

from skyplane.obj_store.s3_interface import S3Object, S3Interface
from skyplane.utils import logger, imports

from skyplane.config_paths import config_path
from skyplane.config import SkyplaneConfig


class R2Object(S3Object):
    def full_path(self):
        account_name, bucket_name = self.bucket.split("/")
        return os.path.join(f"https://{account_name}.r2.cloudflarestorage.com", bucket_name, self.key)


class R2Interface(S3Interface):
    def __init__(self, account_id: str, bucket_name: str):
        self.config = SkyplaneConfig.load_config(config_path)
        self.endpoint_url = f"https://{account_id}.r2.cloudflarestorage.com"
        try:
            self.s3_client = boto3.client(
                "s3",
                endpoint_url=self.endpoint_url,
                aws_access_key_id=self.config.cloudflare_access_key_id,
                aws_secret_access_key=self.config.cloudflare_secret_access_key,
                region_name="auto",  # explicity set region, otherwise may be read from AWS boto3 env
            )
        except Exception:
            raise ValueError("Error with connecting to {self.endpoint_url}: {e}")
        self.requester_pays = False
        self.bucket_name = bucket_name
        self.account_id = account_id

    @property
    def provider(self):
        return "cloudflare"

    def path(self):
        return f"{self.endpoint_url}/{self.bucket_name}"

    def region_tag(self):
        # no regions in cloudflare
        return "cloudflare:infer"

    def bucket(self) -> str:
        return f"{self.account_id}/{self.bucket_name}"

    def _s3_client(self):
        return self.s3_client

    @imports.inject("botocore.exceptions", pip_extra="aws")
    def bucket_exists(botocore_exceptions, self, region=None):
        try:
            requester_pays = {"RequestPayer": "requester"} if self.requester_pays else {}
            self._s3_client().list_objects_v2(Bucket=self.bucket_name, MaxKeys=1, **requester_pays)
            return True
        except botocore_exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchBucket" or e.response["Error"]["Code"] == "AccessDenied":
                return False
            raise

    def create_bucket(self):
        if not self.bucket_exists():
            self._s3_client().create_bucket(Bucket=self.bucket_name)
        else:
            logger.warning(f"Bucket {self.bucket} already exists")

    def list_objects(self, prefix="", region=None) -> Iterator[R2Object]:
        paginator = self._s3_client().get_paginator("list_objects_v2")
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
