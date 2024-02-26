import uuid
from skyplane.obj_store.object_store_interface import ObjectStoreInterface
from tests.interface_util import interface_test_framework
from skyplane.utils import logger


def test_scp_singlepart():
    assert interface_test_framework("scp:KOREA-WEST-2-SCP-B001", f"test-skyplane-{uuid.uuid4()}", False, test_delete_bucket=True)


test_scp_singlepart()


def test_scp_singlepart_zero_bytes():
    assert interface_test_framework(
        "scp:KOREA-WEST-2-SCP-B001", f"test-skyplane-{uuid.uuid4()}", False, test_delete_bucket=True, file_size_mb=0
    )


test_scp_singlepart_zero_bytes()


def test_scp_multipart():
    assert interface_test_framework("scp:KOREA-WEST-2-SCP-B001", f"test-skyplane-{uuid.uuid4()}", True, test_delete_bucket=True)


test_scp_multipart()


def test_scp_bucket_exists():
    # test a public bucket with objects
    iface = ObjectStoreInterface.create("scp:infer", "skyplane")
    assert iface.bucket_exists()

    # test a random bucket that doesn't exist
    iface = ObjectStoreInterface.create("scp:infer", f"skyplane-does-not-exist-{uuid.uuid4()}")
    assert not iface.bucket_exists()

    # test public but empty bucket
    # iface = ObjectStoreInterface.create("scp:infer", "skyplane-test-empty-public-bucket")
    # assert iface.bucket_exists()


test_scp_bucket_exists()
