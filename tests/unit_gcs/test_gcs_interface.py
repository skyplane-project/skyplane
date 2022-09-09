import uuid
from skyplane.obj_store.object_store_interface import ObjectStoreInterface
from tests.interface_util import interface_test_framework


def test_gcs_singlepart():
    assert interface_test_framework("gcp:us-central1-a", f"test-skyplane-{uuid.uuid4()}", False, test_delete_bucket=True)


def test_gcs_singlepart_zero_bytes():
    assert interface_test_framework("gcp:us-central1-a", f"test-skyplane-{uuid.uuid4()}", False, test_delete_bucket=True, file_size_mb=0)


def test_gcs_multipart():
    assert interface_test_framework("gcp:us-central1-a", f"test-skyplane-{uuid.uuid4()}", True, test_delete_bucket=True)
t


def test_gcs_bucket_exists():
    # test a public bucket with objects
    iface = ObjectStoreInterface.create("gcp:infer", "cloud-tpu-test-datasets")
    assert iface.bucket_exists()

    # test a random bucket that doesn't exist
    iface = ObjectStoreInterface.create("gcp:infer", f"skyplane-does-not-exist-{uuid.uuid4()}")
    assert not iface.bucket_exists()

    # test public but empty bucket
    # iface = ObjectStoreInterface.create("aws:infer", "skyplane-test-empty-public-bucket")
    # assert iface.bucket_exists()
