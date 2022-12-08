import os
import sys
from dataclasses import dataclass
from skyplane.obj_store.file_system_interface import FileSystemInterface, LocalFile
from typing import Iterator, List, Optional, Tuple


@dataclass
class POSIXFile(LocalFile):
    """Defines file on local node on a POSIX compliant FS."""

    def exists(self):
        return os.path.exists(self.path)

    def write_permissions(self):
        return os.access(self.path, os.W_OK)

    def real_path(self):
        return os.path.realpath(self.path)


class POSIXInterface(FileSystemInterface):
    """Defines a file system interface for POSIX compliant FS."""

    def list_files(self, prefix="") -> Iterator[POSIXFile]:
        for root, dirs, files in os.walk(prefix):
            for file in files:
                file_path = os.path.join(root, file)
                yield POSIXFile(path=file_path, size=os.path.getsize(file_path), last_modified=os.path.getmtime(file_path))

    def get_file_size(self, file_name) -> int:
        return os.path.getsize(file_name)

    def get_file_last_modified(self, file_name):
        return os.path.getmtime(file_name)

    def delete_files(self, paths: List[str]):
        """Deletes files from the file system. Returns if directory"""
        for path in paths:
            os.remove(path)

    def read_file(self, file_name, offset=0, length=sys.maxsize):
        with open(file_name, "rb") as f:
            f.seek(offset)
            return f.read(length)

    def write_file(self, file_name, data, offset=0):
        with open(file_name, "wb") as f:
            f.seek(offset)
            f.write(data)
