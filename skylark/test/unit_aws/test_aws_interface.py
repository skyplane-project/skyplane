from skylark.test.test_interface import interface_test_framework

from skylark.utils import logger

def test_aws_singlepart():
    assert interface_test_framework("aws:us-east-1", "sky-us-east-1", False)

def test_aws_multipart():
    logger.warning("Multipart tests disabled!")
    #assert test_interface("aws: us-east-1", "sky-us-east-1", True)
