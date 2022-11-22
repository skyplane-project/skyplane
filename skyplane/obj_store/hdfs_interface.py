import os
from pyarrow import fs
from dataclasses import dataclass
from skyplane.obj_store.file_system_interface import FileSystemInterface, LocalFile
from typing import Iterator, List, Optional, Tuple


@dataclass
class HDFSFile(LocalFile):
    path: str
    size: Optional[int] = None
    last_modified: Optional[str] = None
    file_format: Optional[str] = None


class HDFSInterface(FileSystemInterface):
    def __init__(self, path, port=8020):
        self.path = path
        self.port = port
        self.hdfs = fs.HadoopFileSystem(host=self.path, port=self.port, extra_conf={"dfs.permissions.enabled": "false"})

    def path(self) -> str:
        return self.path

    def list_files(self, prefix="") -> Iterator[HDFSFile]:
        fileSelector = fs.FileSelector(prefix=prefix, recursive=True)
        response = self.hdfs.get_file_info(fileSelector)
        for file in response:
            yield HDFSFile(path=file.path, size=file.size, last_modified=file.mtime)

    def get_file_size(self, file_name) -> int:
        fileSelector = fs.FileSelector(prefix=file_name, recursive=False)
        return self.hdfs.get_file_info(fileSelector)[0].size

    def get_file_last_modified(self, file_name):
        fileSelector = fs.FileSelector(prefix=file_name, recursive=False)
        return self.hdfs.get_file_info(fileSelector)[0].mtime

    def delete_files(self, paths: List[str]):
        for path in paths:
            self.hdfs.delete_file(path)

    def read_file(self, file_name):
        with self.hdfs.open_input_stream(file_name) as f:
            return print(f.readall())

    def write_file(self, file_name, data):
        with self.hdfs.open_output_stream(file_name) as f:
            f.write(data)
