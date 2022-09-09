import hashlib
import time
import os
import tempfile
import uuid
from skyplane import MB

from skyplane.obj_store.object_store_interface import ObjectStoreInterface
from skyplane.utils.timer import Timer


def interface_test_framework(region, bucket, multipart: bool, test_delete_bucket: bool = False, file_size_mb: int = 1):
    interface = ObjectStoreInterface.create(region, bucket)
    interface.create_bucket(region.split(":")[1])
    time.sleep(5)
    assert interface.bucket_exists(), f"Bucket {bucket} does not exist"
    assert list(interface.list_objects()) == [], f"Bucket {bucket} is not empty"

    # generate file and upload
    obj_name = f"test_{uuid.uuid4()}.txt"
    with tempfile.NamedTemporaryFile() as tmp:
        fpath = tmp.name
        with open(fpath, "rb+") as f:
            f.write(os.urandom(int(file_size_mb * MB)))
            f.seek(0)
            file_md5 = hashlib.md5(f.read()).hexdigest()

        with Timer() as t:
            if multipart:
                upload_id = interface.initiate_multipart_upload(obj_name)
                interface.upload_object(fpath, obj_name, 1, upload_id)
                interface.complete_multipart_upload(obj_name, upload_id)
            else:
                interface.upload_object(fpath, obj_name)

        time.sleep(1)
        assert interface.exists(obj_name), f"{region.split(':')[0]}://{bucket}/{obj_name} does not exist"
        assert not interface.exists("random_nonexistent_file"), "Object should not exist"
        iface_size = interface.get_obj_size(obj_name)
        local_size = os.path.getsize(fpath)
        assert iface_size == local_size, f"Object size mismatch: {iface_size} != {local_size}"

    # download object
    with tempfile.NamedTemporaryFile() as tmp:
        fpath = tmp.name
        if os.path.exists(fpath):
            os.remove(fpath)

        with Timer() as t:
            if multipart:
                interface.download_object(obj_name, fpath, offset_bytes=0, size_bytes=file_size_mb * MB)
            else:
                interface.download_object(obj_name, fpath)
        iface_size = interface.get_obj_size(obj_name)
        local_size = os.path.getsize(fpath)
        assert iface_size == local_size, f"Object size mismatch: {iface_size} != {local_size}"

        # check md5
        with open(fpath, "rb") as f:
            dl_file_md5 = hashlib.md5(f.read()).hexdigest()

    assert dl_file_md5 == file_md5, "MD5 does not match"

    interface.delete_objects([obj_name])
    if test_delete_bucket:
        interface.delete_bucket()
        time.sleep(2)
        interface = ObjectStoreInterface.create(region, bucket)
        assert not interface.bucket_exists(), "Bucket should not exist"

    return True
