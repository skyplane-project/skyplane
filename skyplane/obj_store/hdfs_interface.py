from functools import lru_cache
import sys
from pyarrow import fs
from dataclasses import dataclass
from typing import Iterator, List, Optional
from skyplane.exceptions import NoSuchObjectException
from skyplane.obj_store.object_store_interface import ObjectStoreInterface, ObjectStoreObject
import mimetypes


@dataclass
class HDFSFile(ObjectStoreObject):
    def full_path(self):
        return f"hdfs://{self.key}"


class HDFSInterface(ObjectStoreInterface):
    def __init__(self, host, path="", port=8020):
        self.host = host
        self.port = port
        self.hdfs_path = path
        # self.hdfs = fs.HadoopFileSystem(
        #     host=f"{self.host}/{self.hdfs_path}", port=self.port, user="hadoop", extra_conf={"dfs.permissions.enabled": "false"}
        # )
        # print(f"Connecting to HDFS at {self.host}:{self.port} with path {self.hdfs_path}")

    def path(self) -> str:
        return self.hdfs_path

    def list_objects(self, prefix="/skyplane5") -> Iterator[HDFSFile]:
        _hdfs_connector = fs.HadoopFileSystem(
            host=f"ec2-54-234-174-31.compute-1.amazonaws.com/", port=self.port, user="hadoop", extra_conf={"dfs.permissions.enabled": "false"}
        )
        print(f"Connecting to HDFS at {self.host}:{self.port} with path {self.hdfs_path}")
        fileselector = fs.FileSelector("/skyplane5", recursive=True, allow_not_found=True)
        print(f"File selector created successfully, {fileselector.base_dir}")
        response = _hdfs_connector("/skyplane5")
        print(f"Response: {response}")
        if hasattr(response, "__len__") and (not isinstance(response, str)):
            for file in response:
                yield HDFSFile(provider="hdfs", bucket=self.host, key=file.path, size=file.size, last_modified=file.mtime)
        else:
            yield HDFSFile(provider="hdfs", bucket=self.host, key=response.path, size=response.size, last_modified=response.mtime)

    def exists(self, obj_name: str):
        try:
            self.get_obj_metadata(obj_name)
            return True
        except NoSuchObjectException:
            return False

    def region_tag(self) -> str:
        return "hdfs:us-east-1"

    def bucket(self) -> str:
        return self.hdfs_path

    def create_bucket(self, region_tag: str):
        self.hdfs.create_dir("/skyplane5")

    def delete_bucket(self):
        self.hdfs.delete_dir("/skyplane5")

    def bucket_exists(self) -> bool:
        return True

    def get_obj_size(self, obj_name) -> int:
        return self.get_obj_metadata(obj_name).size

    def get_obj_last_modified(self, obj_name):
        return self.get_obj_metadata(obj_name).mtime

    def get_obj_mime_type(self, obj_name):
        return mimetypes.guess_type(obj_name)[0]

    def delete_objects(self, keys: List[str]):
        for key in keys:
            self.hdfs.delete_file(f"/skyplane5/{key}")
        return True

    def download_object(
        self, src_object_name, dst_file_path, offset_bytes=None, size_bytes=None, write_at_offset=False, generate_md5: bool = False
    ):
        with self.hdfs.open_input_stream(f"/skyplane5/{src_object_name}") as f1:
            with open(dst_file_path, "wb+" if write_at_offset else "wb") as f2:
                b = f1.read(nbytes=size_bytes)
                while b:
                    f2.write(b)
                    b = f1.read(nbytes=size_bytes)
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
        with open(src_file_path, "rb") as f1:
            with self.hdfs.open_output_stream(f"/skyplane5/{dst_object_name}") as f2:
                b = f1.read()
                f2.write(b)

    def read_file(self, file_name, offset=0, length=sys.maxsize):
        with self.hdfs.open_input_stream(file_name) as f:
            return print(f.readall())

    def write_file(self, file_name, data, offset=0):
        with self.hdfs.open_output_stream(file_name) as f:
            f.write(data)
    
    def initiate_multipart_upload(self, dst_object_name: str, mime_type: Optional[str] = None) -> str:
        raise NotImplementedError(f"Multipart upload is not supported for the POSIX file system.")
    
    def complete_multipart_upload(self, dst_object_name: str, upload_id: str) -> None:
        raise NotImplementedError(f"Multipart upload is not supported for the POSIX file system.")

    @lru_cache(maxsize=1024)
    def get_obj_metadata(self, obj_name) -> fs.FileInfo:
        response = self.hdfs.get_file_info(obj_name)
        if response.type is fs.FileType.NotFound:
            raise NoSuchObjectException(f"Object {obj_name} not found")
        else:
            return response
