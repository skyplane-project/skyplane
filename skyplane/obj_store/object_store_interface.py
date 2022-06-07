from dataclasses import dataclass


@dataclass
class ObjectStoreObject:
    """Defines object in object store."""

    provider: str
    bucket: str
    key: str
    size: int
    last_modified: str

    def full_path(self):
        raise NotImplementedError()


class ObjectStoreInterface:
    def bucket_exists(self):
        raise NotImplementedError()

    def create_bucket(self):
        raise NotImplementedError()

    def delete_bucket(self):
        raise NotImplementedError()

    def list_objects(self, prefix=""):
        raise NotImplementedError()

    def get_obj_size(self, obj_name):
        raise NotImplementedError()

    def download_object(self, src_object_name, dst_file_path, offset_bytes=None, size_bytes=None):
        raise NotImplementedError()

    def upload_object(self, src_file_path, dst_object_name, part_number=None, upload_id=None):
        raise NotImplementedError()

    def initiate_multipart_upload(self, dst_object_name):
        return ValueError("Multipart uploads not supported")

    def complete_multipart_upload(self, dst_object_name, upload_id, parts):
        return ValueError("Multipart uploads not supported")

    @staticmethod
    def create(region_tag: str, bucket: str):
        if region_tag.startswith("aws"):
            from skyplane.obj_store.s3_interface import S3Interface

            _, region = region_tag.split(":", 1)
            return S3Interface(region, bucket)
        elif region_tag.startswith("gcp"):
            from skyplane.obj_store.gcs_interface import GCSInterface

            _, region = region_tag.split(":", 1)
            return GCSInterface(region, bucket)
        elif region_tag.startswith("azure"):
            from skyplane.obj_store.azure_interface import AzureInterface

            storage_account, container = bucket.split("/", 1)  # <storage_account>/<container>
            _, region = region_tag.split(":", 1)
            return AzureInterface(storage_account, container, region=region)
        else:
            raise ValueError(f"Invalid region_tag {region_tag} - could not create interface")


class NoSuchObjectException(Exception):
    pass
