import hashlib
import os
import tempfile

from skyplane import MB
from skyplane.obj_store.gcs_interface import GCSInterface
from skyplane.utils.utils import Timer


def test_gcs_interface():
    gcs_interface = GCSInterface(f"us-east1", f"skyplane-test-us-east1")
    assert gcs_interface.bucket_name == "skyplane-test-us-east1"
    assert gcs_interface.gcp_region == "us-east1"
    gcs_interface.create_bucket()

    # generate file and upload
    obj_name = "test.txt"
    file_size_mb = 128
    with tempfile.NamedTemporaryFile() as tmp:
        fpath = tmp.name
        with open(fpath, "wb") as f:
            f.write(os.urandom(int(file_size_mb * MB)))
        file_md5 = hashlib.md5(open(fpath, "rb").read()).hexdigest()

        with Timer() as t:
            gcs_interface.upload_object(fpath, obj_name)
        assert gcs_interface.get_obj_size(obj_name) == os.path.getsize(fpath)
        assert gcs_interface.exists(obj_name)
        assert not gcs_interface.exists("random_nonexistent_file")

    # download object
    with tempfile.NamedTemporaryFile() as tmp:
        fpath = tmp.name
        if os.path.exists(fpath):
            os.remove(fpath)
        with Timer() as t:
            gcs_interface.download_object(obj_name, fpath)

        # check md5
        dl_file_md5 = hashlib.md5(open(fpath, "rb").read()).hexdigest()
        assert dl_file_md5 == file_md5
