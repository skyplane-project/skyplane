import uuid
from skyplane.obj_store.object_store_interface import ObjectStoreInterface
from tests.interface_util import interface_test_framework
from skyplane.utils import logger


def test_aws_singlepart():
    assert interface_test_framework("aws:us-east-1", f"test-skyplane-{uuid.uuid4()}", False, test_delete_bucket=True)


def test_aws_singlepart_zero_bytes():
    assert interface_test_framework("aws:us-east-1", f"test-skyplane-{uuid.uuid4()}", False, test_delete_bucket=True, file_size_mb=0)


def test_aws_multipart():
    assert interface_test_framework("aws:us-east-1", f"test-skyplane-{uuid.uuid4()}", True, test_delete_bucket=True)


def test_aws_bucket_exists():
    # test a public bucket with objects
    iface = ObjectStoreInterface.create("aws:infer", "skyplane")
    assert iface.bucket_exists()

    # test a random bucket that doesn't exist
    iface = ObjectStoreInterface.create("aws:infer", f"skyplane-does-not-exist-{uuid.uuid4()}")
    assert not iface.bucket_exists()

    # test a requester pays bucket
    iface = ObjectStoreInterface.create("aws:infer", "devrel-delta-datasets")
    # assert iface.bucket_exists()
    logger.warning("Requester pays bucket tests disabled!")

    # test public but empty bucket
    iface = ObjectStoreInterface.create("aws:infer", "skyplane-test-empty-public-bucket")
    assert iface.bucket_exists()
