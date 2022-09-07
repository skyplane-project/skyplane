import uuid
from skyplane.obj_store.object_store_interface import ObjectStoreInterface
from tests.interface_util import interface_test_framework
from skyplane.utils import logger


# def test_azure_singlepart():
#     assert interface_test_framework("azure:eastus", "sky-us-east-1", False)


def test_azure_multipart():
    logger.warning("Multipart tests disabled!")
    # assert test_interface("azure:eastus", "sky-us-east-1", True)


def test_azure_bucket_exists():
    # test a public bucket with objects
    iface = ObjectStoreInterface.create("azure:infer", "azureopendatastorage/mnist")
    assert not iface.storage_account_interface.storage_account_exists_in_account()  # this is a public bucket so is not owned by the user's account
    assert iface.bucket_exists()

    # test a random bucket that doesn't exist
    iface = ObjectStoreInterface.create("azure:infer", f"skyplane-does-not-exist-{uuid.uuid4()}/{uuid.uuid4()}")
    assert not iface.storage_account_interface.storage_account_exists_in_account()
    assert not iface.bucket_exists()

    # test public but empty bucket
    # iface = ObjectStoreInterface.create("aws:infer", "skyplane-test-empty-public-bucket")
    # assert iface.bucket_exists()
