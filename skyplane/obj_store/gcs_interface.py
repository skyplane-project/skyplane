import os
from typing import Iterator, List
from skyplane import exceptions

from skyplane.utils import logger
from skyplane.compute.gcp.gcp_auth import GCPAuthentication
from skyplane.obj_store.object_store_interface import NoSuchObjectException, ObjectStoreInterface, ObjectStoreObject


class GCSObject(ObjectStoreObject):
    def full_path(self):
        return os.path.join(f"gs://{self.bucket}", self.key)


class GCSInterface(ObjectStoreInterface):
    def __init__(self, bucket_name, gcp_region="infer", create_bucket=False):
        self.bucket_name = bucket_name
        self.auth = GCPAuthentication()
        self._gcs_client = self.auth.get_storage_client()
        try:
            self.gcp_region = self.infer_gcp_region(bucket_name) if gcp_region is None or gcp_region == "infer" else gcp_region
            if not self.bucket_exists():
                raise exceptions.MissingBucketException()
        except exceptions.MissingBucketException:
            if create_bucket:
                assert gcp_region is not None and gcp_region != "infer", "Must specify AWS region when creating bucket"
                self.gcp_region = gcp_region
                self.create_bucket()
                logger.info(f"Created GCS bucket {self.bucket_name} in region {self.gcp_region}")
            else:
                raise

    def region_tag(self):
        return "gcp:" + self.gcp_region

    def map_region_to_zone(self, region) -> str:
        """Resolves bucket locations to a valid zone."""
        parsed_region = region.lower().split("-")
        if len(parsed_region) == 3:
            return region
        elif len(parsed_region) == 2 or len(parsed_region) == 1:
            # query the API to get the list of zones in the region and return the first one
            compute = self.auth.get_gcp_client()
            zones = compute.zones().list(project=self.auth.project_id).execute()
            for zone in zones["items"]:
                if zone["name"].startswith(region):
                    return zone["name"]
        raise ValueError(f"No GCP zone found for region {region}")

    def infer_gcp_region(self, bucket_name: str):
        bucket = self._gcs_client.lookup_bucket(bucket_name)
        if bucket is None:
            raise exceptions.MissingBucketException(f"GCS bucket {bucket_name} does not exist")
        return self.map_region_to_zone(bucket.location.lower())

    def bucket_exists(self):
        try:
            self._gcs_client.get_bucket(self.bucket_name)
            return True
        except Exception:
            return False

    def create_bucket(self, premium_tier=True):
        if not self.bucket_exists():
            bucket = self._gcs_client.bucket(self.bucket_name)
            bucket.storage_class = "STANDARD"
            region_without_zone = "-".join(self.gcp_region.split("-")[:2])
            self._gcs_client.create_bucket(bucket, location=region_without_zone)
        assert self.bucket_exists()

    def delete_bucket(self):
        self._gcs_client.get_bucket(self.bucket_name).delete()

    def list_objects(self, prefix="") -> Iterator[GCSObject]:
        blobs = self._gcs_client.list_blobs(self.bucket_name, prefix=prefix)
        for blob in blobs:
            yield GCSObject("gcs", self.bucket_name, blob.name, blob.size, blob.updated)

    def delete_objects(self, keys: List[str]):
        for key in keys:
            self._gcs_client.bucket(self.bucket_name).blob(key).delete()
            assert not self.exists(key)

    def get_obj_metadata(self, obj_name):
        bucket = self._gcs_client.bucket(self.bucket_name)
        blob = bucket.get_blob(obj_name)
        if blob is None:
            raise NoSuchObjectException(
                f"Object {obj_name} does not exist in bucket {self.bucket_name}, or you do not have permission to access it"
            )
        return blob

    def get_obj_size(self, obj_name):
        return self.get_obj_metadata(obj_name).size

    def exists(self, obj_name):
        try:
            self.get_obj_metadata(obj_name)
            return True
        except NoSuchObjectException:
            return False

    # todo: implement range request for download
    def download_object(self, src_object_name, dst_file_path, offset_bytes=None, size_bytes=None):
        src_object_name, dst_file_path = str(src_object_name), str(dst_file_path)
        src_object_name = src_object_name if src_object_name[0] != "/" else src_object_name

        offset = 0
        bucket = self._gcs_client.bucket(self.bucket_name)
        blob = bucket.blob(src_object_name)
        chunk = blob.download_as_string()
        if not os.path.exists(dst_file_path):
            open(dst_file_path, "a").close()
        with open(dst_file_path, "rb+") as f:
            f.seek(offset)
            f.write(chunk)

    def upload_object(self, src_file_path, dst_object_name, part_number=None, upload_id=None):
        src_file_path, dst_object_name = str(src_file_path), str(dst_object_name)
        dst_object_name = dst_object_name if dst_object_name[0] != "/" else dst_object_name
        os.path.getsize(src_file_path)
        bucket = self._gcs_client.bucket(self.bucket_name)
        blob = bucket.blob(dst_object_name)
        blob.upload_from_filename(src_file_path)
