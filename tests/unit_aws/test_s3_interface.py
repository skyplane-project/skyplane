import uuid
from tests.interface_util import interface_test_framework
from skyplane.utils import logger


def test_aws_singlepart():
    assert interface_test_framework("aws:us-east-1", f"test-skyplane-{uuid.uuid4()}", False, test_delete_bucket=True)


def test_aws_multipart():
    logger.warning("Multipart tests disabled!")
    # assert test_interface("aws: us-east-1", "sky-us-east-1", True)
