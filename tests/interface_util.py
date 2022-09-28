import hashlib
import time
import os
import tempfile
import uuid
from skyplane import MB

from skyplane.obj_store.object_store_interface import ObjectStoreInterface
from skyplane.utils.fn import wait_for


def interface_test_framework(region, bucket, multipart: bool, test_delete_bucket: bool = False, file_size_mb: int = 1):
    interface = ObjectStoreInterface.create(region, bucket)
    interface.create_bucket(region.split(":")[1])
    time.sleep(5)

    # generate file and upload
    obj_name = f"test_{uuid.uuid4()}.txt"
    with tempfile.NamedTemporaryFile() as tmp:
        fpath = tmp.name
        with open(fpath, "rb+") as f:
            f.write(os.urandom(int(file_size_mb * MB)))
            f.seek(0)
            file_md5 = hashlib.md5(f.read()).hexdigest()

        if multipart:
            upload_id = interface.initiate_multipart_uploads([obj_name])[0]
            time.sleep(5)
            interface.upload_object(fpath, obj_name, 1, upload_id)
            time.sleep(5)
            interface.complete_multipart_upload(obj_name, upload_id)
            time.sleep(5)
        else:
            interface.upload_object(fpath, obj_name)
            time.sleep(5)
        assert not interface.exists("random_nonexistent_file"), "Object should not exist"

    # download object
    with tempfile.NamedTemporaryFile() as tmp:
        fpath = tmp.name
        if os.path.exists(fpath):
            os.remove(fpath)

        if multipart:
            interface.download_object(obj_name, fpath, 0, file_size_mb * MB)
            time.sleep(5)
        else:
            interface.download_object(obj_name, fpath)
            time.sleep(5)
        local_size = os.path.getsize(fpath)
        assert file_size_mb * MB == local_size, f"Object size mismatch: {file_size_mb * MB} != {local_size}"

        # check md5
        with open(fpath, "rb") as f:
            dl_file_md5 = hashlib.md5(f.read()).hexdigest()
    assert dl_file_md5 == file_md5, "MD5 does not match"

    # check one object is in the bucket
    objs = list(interface.list_objects())
    assert len(objs) == 1, f"{len(objs)} objects in bucket, expected 1"
    assert objs[0].key == obj_name, f"{objs[0].key} != {obj_name}"
    assert objs[0].size == file_size_mb * MB, f"{objs[0].size} != {file_size_mb * MB}"

    interface.delete_objects([obj_name])
    if test_delete_bucket:
        interface.delete_bucket()
    return True
