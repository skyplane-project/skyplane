import hashlib
import os
import tempfile
from skylark import MB

from skylark.obj_store.azure_interface import AzureInterface
from skylark.utils.utils import Timer


def test_azure_interface():
    azure_interface = AzureInterface(f"us-east1", f"sky-us-east-2")
    assert azure_interface.bucket_name == "sky-us-east-2"
    assert azure_interface.azure_region == "us-east1"
    azure_interface.create_bucket()

    # generate file and upload
    obj_name = "test.txt"
    file_size_mb = 128
    with tempfile.NamedTemporaryFile() as tmp:
        fpath = tmp.name
        with open(fpath, "wb") as f:
            f.write(os.urandom(int(file_size_mb * MB)))
        file_md5 = hashlib.md5(open(fpath, "rb").read()).hexdigest()

        with Timer() as t:
            upload_future = azure_interface.upload_object(fpath, obj_name)
            upload_future.result()
        assert azure_interface.get_obj_size(obj_name) == os.path.getsize(fpath)
        assert azure_interface.exists(obj_name)
        assert not azure_interface.exists("random_nonexistent_file")

    # download object
    with tempfile.NamedTemporaryFile() as tmp:
        fpath = tmp.name
        if os.path.exists(fpath):
            os.remove(fpath)
        with Timer() as t:
            download_future = azure_interface.download_object(obj_name, fpath)
            download_future.result()

        # check md5
        dl_file_md5 = hashlib.md5(open(fpath, "rb").read()).hexdigest()
        assert dl_file_md5 == file_md5

    # Clean Up Azure
    azure_interface.delete_objects([obj_name])
    assert not azure_interface.exists(obj_name)
    azure_interface.delete_bucket()
    assert not azure_interface.container_exists()


if __name__ == "__main__":
    test_azure_interface()
