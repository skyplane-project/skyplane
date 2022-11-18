import os
from dataclasses import dataclass
from typing import Iterator, List, Optional, Tuple
from skyplane.obj_store.object_store_interface import ObjectStoreObject


@dataclass
class LocalFile:
    """Defines file on local node."""

    path: str
    size: Optional[int] = None
    last_modified: Optional[str] = None
    file_format: Optional[str] = None

    @property
    def exists(self):
        return os.access(self.path, os.F_OK)
    
    def write_permissions(self):
        return os.access(self.path, os.W_OK)

    def real_path(self):
        return os.path.realpath(self.path)


class FileSystemInterface:
    def path(self) -> str:
        raise NotImplementedError()

    def list_files(self, prefix="") -> Iterator[ObjectStoreObject]:
        raise NotImplementedError()

    def get_file_size(self, file_name) -> int:
        raise NotImplementedError()

    def get_file_last_modified(self, file_name):
        raise NotImplementedError()

    def cache_file_locally(self, src_file_path, dst_file_path):
        # Incases where the data may be on a remote filesystem, we want to cache it locally
        raise NotImplementedError()

    def clear_cache(self):
        raise NotImplementedError()

    def delete_files(self, paths: List[str]):
        raise NotImplementedError()

    def initiate_multipart_upload(self, dst_object_name: str) -> str:
        raise ValueError("Multipart uploads not supported")

    def complete_multipart_upload(self, dst_object_name: str, upload_id: str) -> None:
        raise ValueError("Multipart uploads not supported")

