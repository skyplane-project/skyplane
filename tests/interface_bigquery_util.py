import hashlib
import time
import os
import tempfile
import uuid
from skyplane.utils.definitions import MB

from skyplane.obj_store.object_store_interface import ObjectStoreInterface
from skyplane.utils.fn import wait_for

def bigquery_test_framework(region, bucket, multipart: bool, test_delete_bucket: bool = False, file_size_mb: int = 1):
    interface = ObjectStoreInterface.create(region, bucket)
    interface.create_bucket(region.split(":")[1])
    time.sleep(5)
    obj_name = f"test_{uuid.uuid4()}.csv"
    with tempfile.NamedTemporaryFile() as tmp:
        fpath = tmp.name
        with open(fpath, "rb+") as f:
            f.write(b'Title,Genre,Premiere,Runtime,IMDB Score,Language \n Discuss According Model,Horror,"February 09, 2020",107,2.6,Japanese \n People Conference Be,Comedy,"April 25, 2020",84,1.8,Chinese')
            f.seek(0)
            file_md5 = hashlib.md5(f.read()).hexdigest()
        if multipart:
            upload_id = interface.initiate_multipart_upload(obj_name, mime_type="multipart/related")
            time.sleep(5)
            # interface.upload_object(fpath, obj_name, 1, upload_id)
            # time.sleep(5)
            # interface.complete_multipart_upload(obj_name, upload_id)
            # time.sleep(5)
        else:
            interface.upload_object(fpath, obj_name.split(".")[0])
            time.sleep(5)
    assert not interface.exists("random_nonexistent_file"), "Object should not exist"
    return True
