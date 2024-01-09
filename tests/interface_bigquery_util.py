import hashlib
import time
import os
import sys
import tempfile
import uuid
from skyplane.utils.definitions import MB

from skyplane.obj_store.object_store_interface import ObjectStoreInterface
from skyplane.utils.fn import wait_for

def bigquery_test_framework(region, bucket, content, multipart: bool, test_delete_bucket: bool = False):
    interface = ObjectStoreInterface.create(region, bucket)
    interface.create_bucket(region.split(":")[1])
    time.sleep(5)
    obj_name = f"test_{uuid.uuid4()}"
    file_size_mb = sys.getsizeof(content)
    #upload object
    with tempfile.NamedTemporaryFile() as tmp:
        fpath = tmp.name
        with open(fpath, "rb+") as f:
            f.write(bytes(content, 'utf-8'))
            f.seek(0)
            file_md5 = hashlib.md5(f.read()).hexdigest()
        if multipart:
            upload_id = interface.initiate_multipart_upload(obj_name, mime_type="multipart/related")
            print(upload_id)
            time.sleep(5)
            # interface.upload_object(fpath, obj_name, 1, upload_id)
            # time.sleep(5)
            # interface.complete_multipart_upload(obj_name, upload_id)
            # time.sleep(5)
        else:
            interface.upload_object(fpath, obj_name)
            time.sleep(5)
    assert not interface.exists("random_nonexistent_file"), "Object should not exist"

    # download object
    interface.download_object(obj_name, "/Users/briankim/desktop/asdf", 0, file_size_mb * MB)
    with tempfile.NamedTemporaryFile() as tmp:
        fpath = tmp.name
        if os.path.exists(fpath):
            os.remove(fpath)
        if multipart:
            mime_type, md5 = interface.download_object(obj_name, fpath, 0, file_size_mb * MB)
            time.sleep(5)
        else:
            mime_type, md5 = interface.download_object(obj_name, fpath)
            time.sleep(5)
        local_size = os.path.getsize(fpath)
        assert file_size_mb * MB == local_size, f"Object size mismatch: {file_size_mb} != {local_size}"
        assert md5 is None or file_md5 == md5, f"Object md5 mismatch: {file_md5} != {md5}"
        assert mime_type == "text/plain", f"Object mime type mismatch: {mime_type} != text/plain"
    return True
