from tests.test_interface import interface_test_framework
from skyplane.utils import logger


def test_gcs_singlepart():
    assert interface_test_framework("gcp:us-east-1", "skylark-test-us-east1", False)


def test_gcs_multipart():
    logger.warning("Multipart tests disabled!")
    # assert test_interface("gcp: us-east-1", "skylark-test-us-east1", True)
