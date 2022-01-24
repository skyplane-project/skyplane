import os
from concurrent.futures import Future
from typing import Iterator, List

from google.cloud import storage

from skylark.obj_store.object_store_interface import NoSuchObjectException, ObjectStoreInterface, ObjectStoreObject


class GCSObject(ObjectStoreObject):
    def full_path(self):
        return f"gs://{self.bucket}/{self.key}"


class GCSInterface(ObjectStoreInterface):
    def __init__(self, gcp_region, bucket_name):
        # TODO: infer region?
        self.gcp_region = gcp_region

        self.bucket_name = bucket_name
        self.pending_downloads, self.completed_downloads = 0, 0
        self.pending_uploads, self.completed_uploads = 0, 0

        # TODO - figure out how paralllelism handled
        self._gcs_client = storage.Client()

    def _on_done_download(self, **kwargs):
        self.completed_downloads += 1
        self.pending_downloads -= 1

    def _on_done_upload(self, **kwargs):
        self.completed_uploads += 1
        self.pending_uploads -= 1

    def infer_gcs_region(self, bucket_name: str):
        raise NotImplementedError()

    def bucket_exists(self):
        try:
            bucket = self._gcs_client.get_bucket(self.bucket_name)
            return True
        except Exception as e:
            print(e)
            return False

    def create_bucket(self, storage_class: str = "STANDARD"):
        if not self.bucket_exists():
            bucket = self._gcs_client.bucket(self.bucket_name)
            bucket.storage_class = storage_class
            new_bucket = self._gcs_client.create_bucket(bucket, location=self.gcp_region)
        assert self.bucket_exists()

    def list_objects(self, prefix="") -> Iterator[GCSObject]:
        raise NotImplementedError()

    def delete_objects(self, keys: List[str]):
        raise NotImplementedError()

    def get_obj_metadata(self, obj_name):
        bucket = self._gcs_client.bucket(self.bucket_name)
        blob = bucket.get_blob(obj_name)
        if blob is None:
            raise NoSuchObjectException(f"Object {obj_name} does not exist, or you do not have permission to access it")
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
    def download_object(self, src_object_name, dst_file_path) -> Future:
        src_object_name, dst_file_path = str(src_object_name), str(dst_file_path)
        src_object_name = src_object_name if src_object_name[0] != "/" else src_object_name

        bucket = self._gcs_client.bucket(self.bucket_name)
        blob = bucket.blob(src_object_name)
        chunk = blob.download_as_string()

        def _on_body_download(offset, chunk, **kwargs):
            if not os.path.exists(dst_file_path):
                open(dst_file_path, "a").close()
            with open(dst_file_path, "rb+") as f:
                f.seek(offset)
                f.write(chunk)

        # TODO: create future?
        _on_body_download(0, chunk)

    def upload_object(self, src_file_path, dst_object_name, content_type="infer") -> Future:
        print("uploading object", src_file_path, dst_object_name)
        src_file_path, dst_object_name = str(src_file_path), str(dst_object_name)
        dst_object_name = dst_object_name if dst_object_name[0] != "/" else dst_object_name
        bucket = self._gcs_client.bucket(self.bucket_name)
        blob = bucket.blob(dst_object_name)

        # TODO: create future?
        blob.upload_from_filename(src_file_path)
