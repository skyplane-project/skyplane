import hashlib
import os
import tempfile
from skylark import MB

from skylark.obj_store.s3_interface import S3Interface
from skylark.utils.utils import Timer


def test_s3_interface():
    s3_interface = S3Interface("us-east-1", "sky-us-east-1", True)
    assert s3_interface.bucket_name == "sky-us-east-1"
    assert s3_interface.aws_region == "us-east-1"
    assert s3_interface._s3_client.use_tls is True

    # generate file and upload
    obj_name = "/test.txt"
    file_size_mb = 128
    with tempfile.NamedTemporaryFile() as tmp:
        fpath = tmp.name
        with open(fpath, "wb") as f:
            f.write(os.urandom(int(file_size_mb * MB)))
        file_md5 = hashlib.md5(open(fpath, "rb").read()).hexdigest()

        with Timer() as t:
            s3_interface.upload_object(fpath, obj_name)
        assert s3_interface.get_obj_size(obj_name) == os.path.getsize(fpath)

    # download object
    with tempfile.NamedTemporaryFile() as tmp:
        fpath = tmp.name
        if os.path.exists(fpath):
            os.remove(fpath)
        with Timer() as t:
            s3_interface.download_object(obj_name, fpath)

        # check md5
        dl_file_md5 = hashlib.md5(open(fpath, "rb").read()).hexdigest()
        assert dl_file_md5 == file_md5
