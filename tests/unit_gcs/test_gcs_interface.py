import uuid
from tests.interface_util import interface_test_framework
from skyplane.utils import logger


def test_gcs_singlepart():
    assert interface_test_framework("gcp:us-central1", f"test-skyplane-{uuid.uuid4()}", False, test_delete_bucket=True)


def test_gcs_multipart():
    logger.warning("Multipart tests disabled!")
    # assert test_interface("gcp:us-central1-a", "skyplane-test-us-east1", True)
