import hashlib
import os
import tempfile
from skylark import MB

from skylark.obj_store.s3_interface import S3Interface
from skylark.utils.utils import Timer
from skylark.utils import logger

def test_s3_interface(test="local"):
    if test == "local":
        uploader_region = "us-east-1"
        uploader_bucket = "sky-us-east-1"
        downloader_region = uploader_region
        downloader_bucket = uploader_bucket
    elif test == "close":  
        uploader_region = "us-east-1"
        uploader_bucket = "sky-us-east-1"
        downloader_region = "us-east-2"
        downloader_bucket = "sky-us-east-2"
    elif test == "far":
        uploader_region = "ap-northeast-1"
        uploader_bucket = "sky-ap-northeast-1"
        downloader_region = "eu-west-1"
        downloader_bucket = "sky-eu-west-1"
    else:
        raise NotImplementedError()

    logger.debug("creating interfaces...")
    uploader = S3Interface(uploader_region, uploader_bucket, True)
    downloader = S3Interface(downloader_region, downloader_bucket, True)
    assert uploader.aws_region == uploader_region
    assert uploader.bucket_name == uploader_bucket
    assert downloader.aws_region == downloader_region
    assert downloader.bucket_name == downloader_bucket
    uploader.create_bucket()
    downloader.create_bucket()

    debug_time = lambda n, s, e: logger.debug(f"{n} {s}MB in {round(e, 2)}s ({round(s / e, 2)}MB/s)")

    # generate file and upload
    obj_name = "test_small.txt"
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
            uploader.upload_object(fpath, obj_name)
            debug_time("uploaded", file_size_mb, t.elapsed) 

        assert uploader.get_obj_size(obj_name) == os.path.getsize(fpath)

    # download object
    with tempfile.NamedTemporaryFile() as tmp:
        fpath = tmp.name
        if os.path.exists(fpath):
            os.remove(fpath)

        logger.debug("downloading...")
        with Timer() as t:
            downloader.download_object(obj_name, fpath)
            debug_time("downloaded", file_size_mb, t.elapsed) 

        assert downloader.get_obj_size(obj_name) == os.path.getsize(fpath)

        # check md5
        with open(fpath, "rb") as f:
            logger.debug("verifying...")
            dl_file_md5 = hashlib.md5(f.read()).hexdigest()

        assert dl_file_md5 == file_md5
        logger.debug("done.")
