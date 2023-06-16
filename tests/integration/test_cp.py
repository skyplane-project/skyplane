import pytest
from skyplane.utils import logger
import time
from skyplane.api.client import SkyplaneClient
from skyplane.obj_store.object_store_interface import ObjectStoreInterface
import uuid
import os

test_bucket = "gs://skyplane-test-bucket" # bucket containing test data 

# test cases 
test_bucket_small_file = f"{test_bucket}/files_100000_size_4_mb"
test_bucket_large_file = f"{test_bucket}/file_1_size_416_gb"
test_bucket_empty_folder = f"{test_bucket}/empty_folder"

region_tags = [
    "aws:us-west-2",
    "azure:westus2",
    "gcp:us-west2",
    "gcp:us-east4", # TODO: make sure one is in same region as bucket
]


@pytest.mark.skip(reason="Shared function")
def setup_bucket(region_tag): 
    provider, region = region_tag.split(":")
    if provider == "azure":
        bucket_name = f"integration{region}/{str(uuid.uuid4()).replace('-', '')}"
    else:
        bucket_name = f"integration{region}-{str(uuid.uuid4())[:8]}"
    
    # create bucket
    try:
        iface = ObjectStoreInterface.create(region_tag, bucket_name)
        iface.create_bucket(region)
    except Exception as e:
        logger.fs.error(f"Failed to create bucket {bucket_name}: {e}")
        raise e

    return iface

@pytest.fixture
def bucket(region_tag): 
    iface = setup_bucket(region_tag)
    yield iface.bucket() 
    # cleanup 
    iface.delete_bucket()

@pytest.fixture()
def azure_bucket(): 
    azure_region_tag = "azure:westus2"
    iface = setup_bucket(azure_region_tag)
    while not iface.bucket_exists(): 
        logger.fs.info(f"Waiting for bucket {iface.bucket()}")
        time.sleep(1)
    yield iface.bucket() 
    # cleanup 
    iface.delete_bucket()

@pytest.fixture()
def aws_bucket(): 
    aws_region_tag = "aws:us-west-2"
    iface = setup_bucket(aws_region_tag)
    #while not iface.bucket_exists(): 
    #    print("waiting for bucket...")
    #    logger.fs.info(f"Waiting for bucket {iface.bucket()}")
    #    time.sleep(1)

    assert iface.bucket_exists(), f"Bucket {iface.bucket()} does not exist"

    yield iface.bucket()
    # cleanup
    #iface.delete_bucket()


@pytest.mark.parametrize("test_case", [test_bucket_small_file, test_bucket_large_file, test_bucket_empty_folder])
def test_cp_aws(aws_bucket, test_case):  
    
    client = SkyplaneClient()
    src_iface = ObjectStoreInterface.create("gcp:us-west2", test_bucket.split("://")[1])

    print("AWS BUCKEt", aws_bucket)

    test_case = "files_100000_size_4_mb"
    assert isinstance(aws_bucket, str), f"Bucket name is not a string {aws_bucket}"
    assert len(list(src_iface.list_objects(prefix=test_case))) > 0, f"Test case {test_bucket}/{test_case} does not exist in {test_bucket}"
    client.copy(test_case, f"s3://{aws_bucket}/{test_case}", recursive=True)


# test one sided transfers 

# test multicast 

# test same region transfers 

# test multiple VMs 



