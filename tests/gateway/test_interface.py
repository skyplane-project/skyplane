from typing import Any, Iterator, Tuple, Dict, Optional, List
from skyplane.obj_store.object_store_interface import ObjectStoreInterface, ObjectStoreObject


class TestObject(ObjectStoreObject):
    def full_path(self):
        return f"test://{self.bucket}/{self.key}"


class TestInterface(ObjectStoreInterface):
    """Test interface simulates an object store interface"""

    def __init__(self, region_tag, bucket_name):
        self.bucket_name = bucket_name
        self._region_tag = region_tag
        self.provider = "test"

    def path(self):
        return f"{self.provider}://{self.bucket_name}"

    def bucket(self):
        return self.bucket_name

    def bucket_exists(self) -> bool:
        return True

    def region_tag(self):
        return self._region_tag

    def list_objects(self, prefix="") -> Iterator[Any]:
        for key in ["obj1", "obj2", "obj3"]:
            obj = self.create_object_repr(key)
            obj.size = 100
            yield obj

    def get_obj_size(self, obj_name) -> int:
        return 100

    def get_obj_last_modified(self, obj_name):
        return "2020-01-01"

    def get_obj_mime_type(self, obj_name):
        return None

    def download_object(
        self, src_object_name, dst_file_path, offset_bytes=None, size_bytes=None, write_at_offset=False, generate_md5: bool = False
    ) -> Tuple[Optional[str], Optional[bytes]]:
        return

    def upload_object(
        self,
        src_file_path,
        dst_object_name,
        part_number=None,
        upload_id=None,
        check_md5: Optional[bytes] = None,
        mime_type: Optional[str] = None,
    ):
        return

    def delete_objects(self, keys: List[str]):
        return

    def initiate_multipart_upload(self, dst_object_name: str, mime_type: Optional[str] = None) -> str:
        return

    def complete_multipart_upload(self, dst_object_name: str, upload_id: str) -> None:
        return

    def create_object_repr(self, key: str) -> TestObject:
        return TestObject(provider="test", bucket=self.bucket_name, key=key)
