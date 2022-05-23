import hashlib
import os
import tempfile

from skyplane import MB
from skyplane.obj_store.s3_interface import S3Interface
from skyplane.utils import logger
from skyplane.utils.utils import Timer


def test_s3_interface(region="us-east-1", bucket="sky-us-east-1"):
    logger.debug("creating interfaces...")
    interface = S3Interface(region, bucket)
    assert interface.aws_region == region
    assert interface.bucket_name == bucket
    interface.create_bucket()

    debug_time = lambda n, s, e: logger.debug(f"{n} {s}MB in {round(e, 2)}s ({round(s / e, 2)}MB/s)")

    # generate file and upload
    obj_name = "test.txt"
    file_size_mb = 128
    with tempfile.NamedTemporaryFile() as tmp:
        fpath = tmp.name
        with open(fpath, "rb+") as f:
            logger.debug("writing...")
            f.write(os.urandom(int(file_size_mb * MB)))
            f.seek(0)
            logger.debug("verifying...")
            file_md5 = hashlib.md5(f.read()).hexdigest()

        logger.debug("uploading...")
        with Timer() as t:
            interface.upload_object(fpath, obj_name)
            debug_time("uploaded", file_size_mb, t.elapsed)

        assert interface.get_obj_size(obj_name) == os.path.getsize(fpath)

    # download object
    with tempfile.NamedTemporaryFile() as tmp:
        fpath = tmp.name
        if os.path.exists(fpath):
            os.remove(fpath)

        logger.debug("downloading...")
        with Timer() as t:
            interface.download_object(obj_name, fpath)
            debug_time("downloaded", file_size_mb, t.elapsed)

        assert interface.get_obj_size(obj_name) == os.path.getsize(fpath)

        # check md5
        with open(fpath, "rb") as f:
            logger.debug("verifying...")
            dl_file_md5 = hashlib.md5(f.read()).hexdigest()

        assert dl_file_md5 == file_md5
        logger.debug("done.")
