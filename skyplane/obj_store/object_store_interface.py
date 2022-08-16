from dataclasses import dataclass
from typing import Iterator, List, Optional, Type


@dataclass
class ObjectStoreObject:
    """Defines object in object store."""

    provider: str
    bucket: str
    key: str
    size: Optional[int] = None
    last_modified: Optional[str] = None

    @property
    def exists(self):
        return self.size is not None and self.last_modified is not None

    def full_path(self):
        raise NotImplementedError()


class ObjectStoreInterface:
    def path(self) -> str:
        raise NotImplementedError()

    def region_tag(self) -> str:
        raise NotImplementedError()

    def create_bucket(self, region_tag: str):
        raise NotImplementedError()

    def delete_bucket(self):
        raise NotImplementedError()

    def bucket_exists(self) -> bool:
        raise NotImplementedError()

    def exists(self, obj_name: str) -> bool:
        raise NotImplementedError()

    def list_objects(self, prefix="") -> Iterator[Type[ObjectStoreObject]]:
        raise NotImplementedError()

    def get_obj_size(self, obj_name) -> int:
        raise NotImplementedError()

    def get_obj_last_modified(self, obj_name):
        raise NotImplementedError()

    def download_object(
        self, src_object_name, dst_file_path, offset_bytes=None, size_bytes=None, write_at_offset=False, generate_md5: bool = False
    ) -> Optional[bytes]:
        """
        Downloads an object from the bucket to a local file.

        :param src_object_name: The object key in the source bucket.
        :param dst_file_path: The path to the file to write the object to
        :param offset_bytes: The offset in bytes from the start of the object to begin the download.
        If None, the download starts from the beginning of the object.
        :param size_bytes: The number of bytes to download. If None, the download will download the entire object.
        :param write_at_offset: If True, the file will be written at the offset specified by
        offset_bytes. If False, the file will be overwritten.
        :param generate_md5: If True, the MD5 hash of downloaded data will be returned.
        """
        raise NotImplementedError()

    def upload_object(self, src_file_path, dst_object_name, part_number=None, upload_id=None, check_md5: Optional[bytes] = None):
        """
        Uploads a file to the specified object

        :param src_file_path: The path to the file you want to upload,
        :param dst_object_name: The destination key of the object to be uploaded.
        :param part_number: For multipart uploads, the part number to upload.
        :param upload_id: For multipart uploads, the upload ID for the whole file to upload to.
        :param check_md5: The MD5 checksum of the file. If this is provided, the server will check the
        MD5 checksum of the file and raise ObjectStoreChecksumMismatchException if it doesn't match.
        """
        raise NotImplementedError()

    def delete_objects(self, keys: List[str]):
        raise NotImplementedError()

    def initiate_multipart_upload(self, dst_object_name):
        return ValueError("Multipart uploads not supported")

    def complete_multipart_upload(self, dst_object_name, upload_id):
        return ValueError("Multipart uploads not supported")

    @staticmethod
    def create(region_tag: str, bucket: str):
        if region_tag.startswith("aws"):
            from skyplane.obj_store.s3_interface import S3Interface

            return S3Interface(bucket)
        elif region_tag.startswith("gcp"):
            from skyplane.obj_store.gcs_interface import GCSInterface

            return GCSInterface(bucket)
        elif region_tag.startswith("azure"):
            from skyplane.obj_store.azure_blob_interface import AzureBlobInterface

            storage_account, container = bucket.split("/", 1)  # <storage_account>/<container>
            return AzureBlobInterface(storage_account, container)
        else:
            raise ValueError(f"Invalid region_tag {region_tag} - could not create interface")
