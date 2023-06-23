from functools import lru_cache
import os
import sys
from dataclasses import dataclass
from typing import Any, Iterator, List, Optional
from skyplane.exceptions import NoSuchObjectException
from skyplane.obj_store.object_store_interface import ObjectStoreInterface, ObjectStoreObject
import mimetypes


@dataclass
class POSIXFile(ObjectStoreObject):
    """Defines file on local node on a POSIX compliant FS."""

    def full_path(self):
        """For the POSIX complaint file, the key is the full path to the file."""
        return os.path.realpath(self.key)


class POSIXInterface(ObjectStoreInterface):
    """Defines a file system interface for POSIX compliant FS."""

    def __init__(self, path=""):
        self.dir_path = path

    def path(self) -> str:
        """Returns the path to the file system."""
        return self.dir_path

    def list_objects(self, prefix="") -> Iterator[POSIXFile]:
        """Lists all objects in the file system."""
        if os.path.isfile(self.dir_path):
            yield POSIXFile(
                provider="posix",
                bucket=self.dir_path,
                key=self.dir_path,
                size=os.path.getsize(self.dir_path),
                last_modified=f"{os.path.getmtime(self.dir_path)}",
            )
        else:
            for root, dirs, files in os.walk(self.dir_path):
                for file in files:
                    full_path = os.path.join(root, file)
                    yield POSIXFile(
                        provider="posix",
                        bucket=self.dir_path,
                        key=full_path,
                        size=os.path.getsize(full_path),
                        last_modified=f"{os.path.getmtime(full_path)}",
                    )

    def exists(self, obj_name: str):
        """Checks if the object exists."""
        return os.path.exists(obj_name)

    def region_tag(self) -> str:
        return f"local:{self.path()}"

    def bucket(self) -> str:
        return self.dir_path

    def create_bucket(self, region_tag: str):
        return None

    def delete_bucket(self):
        return None

    def bucket_exists(self) -> bool:
        """We always have a bucket, the file system."""
        return True

    def get_obj_size(self, obj_name) -> int:
        """Returns the size of the object."""
        if not self.exists(obj_name):
            raise NoSuchObjectException(obj_name)
        return os.path.getsize(obj_name)

    def get_obj_last_modified(self, obj_name):
        """Returns the last modified time of the object."""
        if not self.exists(obj_name):
            raise NoSuchObjectException(obj_name)
        return os.path.getmtime(obj_name)

    def get_obj_mime_type(self, obj_name):
        return mimetypes.guess_type(obj_name)[0]

    def delete_objects(self, keys: List[str]):
        for key in keys:
            try:
                os.remove(key)
            except OSError:
                print(f"{key} is a directory, not a file. Skipping.", file=sys.stderr)
                continue
        return True

    def download_object(
        self, src_object_name, dst_file_path, offset_bytes=None, size_bytes=None, write_at_offset=False, generate_md5: bool = False
    ):
        """Downloads the object to the destination file path."""
        if not self.exists(src_object_name):
            raise NoSuchObjectException(src_object_name)
        if offset_bytes is not None and size_bytes is not None:
            with open(src_object_name, "rb") as src_file:
                src_file.seek(offset_bytes)
                with open(dst_file_path, "wb") as dst_file:
                    dst_file.write(src_file.read(size_bytes))
        else:
            with open(src_object_name, "rb") as src_file:
                with open(dst_file_path, "wb") as dst_file:
                    dst_file.write(src_file.read())
        return self.get_obj_mime_type(src_object_name), None

    def upload_object(
        self,
        src_file_path,
        dst_object_name,
        part_number=None,
        upload_id=None,
        check_md5: Optional[bytes] = None,
        mime_type: Optional[str] = None,
    ):
        """Uploads the object to the destination file path."""
        with open(src_file_path, "rb") as src_file:
            with open(dst_object_name, "wb") as dst_file:
                dst_file.write(src_file.read())

    def read_file(self, file_name, offset=0, length=sys.maxsize):
        """Reads the file from the file system."""
        with open(file_name, "rb") as file:
            file.seek(offset)
            return file.read(length)

    def write_file(self, file_name, data, offset=0):
        """Writes the data to the file."""
        with open(file_name, "wb") as file:
            file.seek(offset)
            file.write(data)

    def initiate_multipart_upload(self, dst_object_name: str, mime_type: Optional[str] = None) -> str:
        raise NotImplementedError(f"Multipart upload is not supported for the POSIX file system.")

    def complete_multipart_upload(self, dst_object_name: str, upload_id: str, metadata: Optional[Any] = None) -> None:
        raise NotImplementedError(f"Multipart upload is not supported for the POSIX file system.")

    @lru_cache(maxsize=1024)
    def get_object_metadata(self, obj_name: str):
        """Returns the metadata for the object."""
        if not self.exists(obj_name):
            raise NoSuchObjectException(obj_name)
        return {
            "size": os.path.getsize(obj_name),
            "last_modified": os.path.getmtime(obj_name),
            "mime_type": self.get_obj_mime_type(obj_name),
        }
