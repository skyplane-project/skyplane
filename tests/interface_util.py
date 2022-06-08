import hashlib
import time
import os
import tempfile
from skyplane import MB

from skyplane.obj_store.object_store_interface import ObjectStoreInterface
from skyplane.utils.timer import Timer
from skyplane.utils import logger


def interface_test_framework(region, bucket, multipart: bool):
    logger.info("creating interfaces...")
    interface = ObjectStoreInterface.create(region, bucket, create_bucket=True)
    assert interface.bucket_exists()
    debug_time = lambda n, s, e: logger.info(f"{n} {s}MB in {round(e, 2)}s ({round(s / e, 2)}MB/s)")

    # generate file and upload
    obj_name = f"test_{time.time_ns()}.txt"
    file_size_mb = 1024
    with tempfile.NamedTemporaryFile() as tmp:
        fpath = tmp.name
        with open(fpath, "rb+") as f:
            logger.info("writing...")
            f.write(os.urandom(int(file_size_mb * MB)))
            f.seek(0)
            logger.info("verifying...")
            file_md5 = hashlib.md5(f.read()).hexdigest()

        logger.info("uploading...")

        with Timer() as t:
            if multipart:
                upload_id = interface.initiate_multipart_upload(obj_name)
                interface.upload_object(fpath, obj_name, 1, upload_id)
                interface.complete_multipart_upload(obj_name, upload_id)
            else:
                interface.upload_object(fpath, obj_name)
            debug_time("uploaded", file_size_mb, t.elapsed)

        assert interface.exists(obj_name)
        assert not interface.exists("random_nonexistent_file")
        assert interface.get_obj_size(obj_name) == os.path.getsize(fpath)

    # download object
    with tempfile.NamedTemporaryFile() as tmp:
        fpath = tmp.name
        if os.path.exists(fpath):
            os.remove(fpath)

        logger.info("downloading...")
        with Timer() as t:
            if multipart:
                interface.download_object(obj_name, fpath, 0, file_size_mb)
            else:
                interface.download_object(obj_name, fpath)
            debug_time("downloaded", file_size_mb, t.elapsed)

        assert interface.get_obj_size(obj_name) == os.path.getsize(fpath)

        # check md5
        with open(fpath, "rb") as f:
            logger.info("verifying...")
            dl_file_md5 = hashlib.md5(f.read()).hexdigest()

        assert dl_file_md5 == file_md5

    interface.delete_objects([obj_name])
    assert not interface.exists(obj_name)
    if not is_bucket_preexisting:
        interface.delete_bucket()
        assert not interface.bucket_exists()

    return True
