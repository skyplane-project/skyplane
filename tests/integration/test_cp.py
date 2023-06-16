import pytest
from skyplane.api.config import TransferConfig
from skyplane.utils import logger
import time
from skyplane.api.client import SkyplaneClient
from skyplane.obj_store.object_store_interface import ObjectStoreInterface
import uuid
import os

test_bucket = "gs://skyplane-test-bucket"  # bucket containing test data
test_region_tag = "gcp:us-west2"

# test cases
test_bucket_small_file = f"{test_bucket}/files_10000_size_4_mb"
test_bucket_large_file = f"{test_bucket}/file_1_size_16_gb"
# test_bucket_empty_folder = f"{test_bucket}/empty_folder"


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


@pytest.fixture(scope="session")
def bucket(region_tag):
    iface = setup_bucket(region_tag)
    yield iface.bucket()
    # cleanup
    iface.delete_bucket()


@pytest.fixture(scope="session")
def same_region_bucket():
    iface = setup_bucket(test_region_tag)
    assert iface.bucket_exists(), f"Bucket {iface.bucket()} does not exist"
    yield iface.bucket()

    # cleanup
    iface.delete_bucket()


@pytest.fixture(scope="session")
def gcp_bucket():
    region_tag = "gcp:europe-west2"
    iface = setup_bucket(region_tag)
    assert iface.bucket_exists(), f"Bucket {iface.bucket()} does not exist"
    yield iface.bucket()

    # cleanup
    iface.delete_bucket()


@pytest.fixture(scope="session")
def azure_bucket():
    azure_region_tag = "azure:westus2"
    iface = setup_bucket(azure_region_tag)
    while not iface.bucket_exists():
        logger.fs.info(f"Waiting for bucket {iface.bucket()}")
        time.sleep(1)
    yield iface.bucket()
    # cleanup
    iface.delete_bucket()


@pytest.fixture(scope="session")
def aws_bucket():
    aws_region_tag = "aws:us-west-2"
    iface = setup_bucket(aws_region_tag)
    # while not iface.bucket_exists():
    #    print("waiting for bucket...")
    #    logger.fs.info(f"Waiting for bucket {iface.bucket()}")
    #    time.sleep(1)

    assert iface.bucket_exists(), f"Bucket {iface.bucket()} does not exist"

    yield iface.bucket()
    # cleanup
    iface.delete_bucket()


@pytest.fixture(scope="session")
def cloudflare_bucket():
    iface = setup_bucket("cloudflare:infer")
    assert iface.bucket_exists(), f"Bucket {iface.bucket()} does not exist"
    yield iface.bucket()

    # cleanup
    iface.delete_bucket()


# TODO: add more parameters for bucket types
@pytest.mark.parametrize("test_case", [test_bucket_large_file])  # , test_bucket_empty_folder])
def test_big_file(aws_bucket, gcp_bucket, test_case):
    client = SkyplaneClient()
    src_iface = ObjectStoreInterface.create("gcp:us-west2", test_bucket.split("://")[1])

    assert isinstance(aws_bucket, str), f"Bucket name is not a string {aws_bucket}"
    assert (
        len(list(src_iface.list_objects(prefix=test_case.replace(f"{test_bucket}/", "")))) > 0
    ), f"Test case {test_case} does not exist in {test_bucket}"
    client.copy(test_case, f"s3://{aws_bucket}/{test_case}")

    # assert sync has cost zero
    dst_iface = ObjectStoreInterface.create("aws:us-west-2", aws_bucket)
    dst_objects = list(dst_iface.list_objects())
    assert dst_objects == [test_case], f"Object {test_case} not copied to {aws_bucket}: only container {dst_objects}"

    # copy back
    client.copy(f"s3://{aws_bucket}/{test_case}", f"gs://{gcp_bucket}/aws/{test_case}")


@pytest.mark.parametrize("test_case", [test_bucket_small_file])  # , test_bucket_empty_folder])
def test_many_files(aws_bucket, gcp_bucket, test_case):
    client = SkyplaneClient()
    src_iface = ObjectStoreInterface.create("gcp:us-west2", test_bucket.split("://")[1])

    assert isinstance(aws_bucket, str), f"Bucket name is not a string {aws_bucket}"
    assert (
        len(list(src_iface.list_objects(prefix=test_case.replace(f"{test_bucket}/", "")))) > 0
    ), f"Test case {test_case} does not exist in {test_bucket}"
    client.copy(test_case, f"s3://{aws_bucket}/{test_case}", recursive=True)

    # copy back
    client.copy(f"s3://{aws_bucket}/{test_case}", f"gs://{gcp_bucket}/aws/{test_case}", recursive=True)


# test one sided transfers
def test_cp_one_sided():
    pass


# test multiple VMs
def test_cp_multiple_vms(aws_bucket):
    client = SkyplaneClient()
    pipeline = client.pipeline(max_instances=2)
    pipeline.queue_copy(test_bucket_large_file, f"s3://{aws_bucket}/")
    pipeline.start(debug=True, progress=True)


# test multicast
# TODO: add azure
def test_cp_multicast(aws_bucket, gcp_bucket, same_region_bucket):
    client = SkyplaneClient()
    src_iface = ObjectStoreInterface.create("gcp:us-west2", test_bucket.split("://")[1])
    assert (
        len(list(src_iface.list_objects(prefix=test_bucket_large_file.replace(f"{test_bucket}/", "")))) > 0
    ), f"Test case {test_bucket_large_file} does not exist in {test_bucket}"
    client.copy(test_bucket_large_file, [f"s3://{aws_bucket}/", f"gs://{gcp_bucket}/", f"gs://{same_region_bucket}/"])
