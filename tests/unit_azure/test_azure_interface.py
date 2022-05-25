from skylark.test.test_interface import test_interface

from skylark.utils import logger

def test_azure_singlepart():
    assert interface_test_framework("azure:eastus", "sky-us-east-1", False)

def test_azure_multipart():
    logger.warning("Multipart tests disabled!")
    #assert test_interface("azure: eastus", "sky-us-east-1", True)
