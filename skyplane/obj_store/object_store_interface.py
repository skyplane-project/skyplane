from dataclasses import dataclass

from typing import Any, Iterator, List, Optional, Tuple

from skyplane.obj_store.storage_interface import StorageInterface
from skyplane.utils import logger


@dataclass
class ObjectStoreObject:
    """Defines object in object store."""

    key: str
    provider: Optional[str] = None
    bucket: Optional[str] = None
    size: Optional[int] = None
    last_modified: Optional[str] = None
    mime_type: Optional[str] = None

    @property
    def exists(self):
        return self.size is not None and self.last_modified is not None

    def full_path(self):
        raise NotImplementedError()


class ObjectStoreInterface(StorageInterface):
    def set_requester_bool(self, requester: bool):
        return

    def get_obj_size(self, obj_name) -> int:
        raise NotImplementedError()

    def get_obj_last_modified(self, obj_name):
        raise NotImplementedError()

    def get_obj_mime_type(self, obj_name):
        raise NotImplementedError()

    def download_object(
        self, src_object_name, dst_file_path, offset_bytes=None, size_bytes=None, write_at_offset=False, generate_md5: bool = False
    ) -> Tuple[Optional[str], Optional[bytes]]:
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

    def upload_object(
        self,
        src_file_path,
        dst_object_name,
        part_number=None,
        upload_id=None,
        check_md5: Optional[bytes] = None,
        mime_type: Optional[str] = None,
    ):
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

    def initiate_multipart_upload(self, dst_object_name: str, mime_type: Optional[str] = None) -> str:
        raise ValueError("Multipart uploads not supported")

    def complete_multipart_upload(self, dst_object_name: str, upload_id: str, metadata: Optional[Any] = None) -> None:
        raise ValueError("Multipart uploads not supported")
