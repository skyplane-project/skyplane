from dataclasses import dataclass
from typing import Iterator, List, Optional
from skyplane.obj_store.storage_interface import StorageInterface
import os


@dataclass
class LocalFile:
    """Defines file on local node."""

    path: str
    size: Optional[int] = None
    last_modified: Optional[str] = None
    file_format: Optional[str] = None

    @property
    def exists(self):
        raise NotImplementedError()

    def write_permissions(self):
        raise NotImplementedError()

    def real_path(self):
        raise NotImplementedError()


class FileSystemInterface(StorageInterface):

    def region_tag(self) -> str:
        return "local"

    def path(self) -> str:
        return self.path

    def list_files(self, prefix="") -> Iterator[LocalFile]:
        raise os.listdir(prefix)

    def get_file_size(self, file_name) -> int:
        return os.path.get_size(file_name)

    def get_file_last_modified(self, file_name):
        return os.path.getmtime(file_name)

    def cache_file_locally(self, src_file_path, dst_file_path):
        # Incases where the data may be on a remote filesystem, we want to cache it locally
        raise NotImplementedError()

    def clear_cache(self):
        raise NotImplementedError()

    def delete_files(self, paths: List[str]):
        for path in paths:
            os.remove(path)

    def initiate_multipart_upload(self, dst_object_name: str) -> str:
        raise ValueError("Multipart uploads not supported")

    def complete_multipart_upload(self, dst_object_name: str, upload_id: str) -> None:
        raise ValueError("Multipart uploads not supported")

    @staticmethod
    def create(fs: str, path: str, port: Optional[int] = None):
        if fs.startswith("hdfs"):
            from skyplane.obj_store.hdfs_interface import HDFSInterface

            return HDFSInterface(path, port)
        else:
            from skyplane.obj_store.posix_file_interface import POSIXInterface

            return POSIXInterface(path)
