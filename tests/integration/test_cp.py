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
test_bucket_medium_file = f"{test_bucket}/files_100_size_64_mb"
# test_bucket_empty_folder = f"{test_bucket}/empty_folder"


@pytest.mark.skip(reason="Shared function")
def setup_bucket(region_tag):
    provider, region = region_tag.split(":")
    if provider == "azure" or provider == "cloudflare":
        bucket_name = f"{str(uuid.uuid4())[:8]}/{str(uuid.uuid4()).replace('-', '')}"
    else:
        bucket_name = f"integration{region}-{str(uuid.uuid4())[:8]}"

    # create bucket
    try:
        iface = ObjectStoreInterface.create(region_tag, bucket_name)
        if provider == "cloudflare":
            iface.create_bucket()
        else:
            iface.create_bucket(region)
    except Exception as e:
        logger.fs.error(f"Failed to create bucket {bucket_name}: {e}")
        raise e

    return iface


@pytest.fixture(scope="session")
def bucket(region_tag):
    iface = setup_bucket(region_tag)
    yield iface
    # cleanup
    iface.delete_bucket()


@pytest.fixture(scope="session")
def same_region_bucket():
    iface = setup_bucket(test_region_tag)
    assert iface.bucket_exists(), f"Bucket {iface.bucket()} does not exist"
    yield iface

    # cleanup
    iface.delete_bucket()


@pytest.fixture(scope="session")
def gcp_bucket():
    region_tag = "gcp:europe-west2"
    iface = setup_bucket(region_tag)
    assert iface.bucket_exists(), f"Bucket {iface.bucket()} does not exist"
    yield iface

    # cleanup
    iface.delete_bucket()


@pytest.fixture(scope="session")
def azure_bucket():
    azure_region_tag = "azure:westus"
    iface = setup_bucket(azure_region_tag)
    while not iface.bucket_exists():
        logger.fs.info(f"Waiting for bucket {iface.bucket()}")
        time.sleep(1)
    yield iface
    # cleanup
    iface.delete_bucket()


@pytest.fixture(scope="session")
def aws_bucket():
    aws_region_tag = "aws:us-west-2"
    iface = setup_bucket(aws_region_tag)
    assert iface.bucket_exists(), f"Bucket {iface.bucket()} does not exist"

    yield iface
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
@pytest.mark.parametrize( # tests large objects
   "test_case, recursive", [(test_bucket_large_file, False), (test_bucket_medium_file, True), (test_bucket_small_file, True)]
)
#@pytest.mark.parametrize("test_case, recursive", [(test_bucket_medium_file, True)])
def test_azure(azure_bucket, gcp_bucket, test_case, recursive):
    """
    Test copying a big file to different cloud providers
    :param azure_bucket: destination interface
    :param gcp_bucket: gcp bucket to copy FROM dstiface
    :param test_case: test case from test_bucket to copy from
    """
    print("DEST", azure_bucket.path(), gcp_bucket.path())
    client = SkyplaneClient()
    src_iface = ObjectStoreInterface.create("gcp:us-west2", test_bucket.split("://")[1])
    print(azure_bucket.path())

    assert isinstance(azure_bucket.bucket(), str), f"Bucket name is not a string {azure_bucket.bucket()}"
    assert (
        len(list(src_iface.list_objects(prefix=test_case.replace(f"{test_bucket}/", "")))) > 0
    ), f"Test case {test_case} does not exist in {test_bucket}"
    print("test case", test_case)
    client.copy(test_case, f"{azure_bucket.path()}/{test_case}", recursive=recursive)

    # assert sync has cost zero
    dst_objects = list(azure_bucket.list_objects())
    assert len(dst_objects) > 0, f"Object {test_case} not copied to {azure_bucket.bucket()}: only container {dst_objects}"

    print(f"gs://{gcp_bucket}/azure/{test_case}")
    # copy back
    client.copy(f"{azure_bucket.path()}/{test_case}", f"gs://{gcp_bucket.bucket()}/azure/", recursive=recursive)


@pytest.mark.parametrize(
    "test_case, recursive", [(test_bucket_medium_file, True), (test_bucket_large_file, False), (test_bucket_small_file, True)]
)
def test_aws(aws_bucket, gcp_bucket, test_case, recursive):
    """
    Test copying a big file to different cloud providers
    :param aws_bucket: destination interface
    :param gcp_bucket: gcp bucket to copy FROM dstiface
    :param test_case: test case from test_bucket to copy from
    """
    client = SkyplaneClient()
    src_iface = ObjectStoreInterface.create("gcp:us-west2", test_bucket.split("://")[1])
    print("test case:", test_case)
    print(aws_bucket.path())

    assert isinstance(aws_bucket.bucket(), str), f"Bucket name is not a string {aws_bucket.bucket()}"
    assert (
        len(list(src_iface.list_objects(prefix=test_case.replace(f"{test_bucket}/", "")))) > 0
    ), f"Test case {test_case} does not exist in {test_bucket}"
    client.copy(test_case, f"{aws_bucket.path()}/{test_case}", recursive=recursive)

    # assert sync has cost zero
    dst_objects = list(aws_bucket.list_objects())
    assert len(dst_objects) > 0, f"Object {test_case} not copied to {aws_bucket.bucket()}: only container {dst_objects}"

    # copy back
    client.copy(f"{aws_bucket.path()}/{test_case}", f"gs://{gcp_bucket.bucket()}/aws/", recursive=recursive)


@pytest.mark.parametrize(
    "test_case, recursive", [(test_bucket_medium_file, True), (test_bucket_large_file, False), (test_bucket_small_file, True)]
)
def test_cloudflare(cloudflare_bucket, gcp_bucket, test_case, recursive):
    """
    Test copying a big file to different cloud providers
    :param cloudflare_bucket: destination interface
    :param gcp_bucket: gcp bucket to copy FROM dstiface
    :param test_case: test case from test_bucket to copy from
    """
    client = SkyplaneClient()
    src_iface = ObjectStoreInterface.create("gcp:us-west2", test_bucket.split("://")[1])

    assert isinstance(cloudflare_bucket.bucket(), str), f"Bucket name is not a string {cloudflare_bucket.bucket()}"
    assert (
        len(list(src_iface.list_objects(prefix=test_case.replace(f"{test_bucket}/", "")))) > 0
    ), f"Test case {test_case} does not exist in {test_bucket}"
    client.copy(test_case, f"{cloudflare_bucket.path()}/{test_case}", recursive=recursive)

    # assert sync has cost zero
    dst_objects = list(cloudflare_bucket.list_objects())
    assert len(dst_objects) > 0, f"Object {test_case} not copied to {cloudflare_bucket.bucket()}: only container {dst_objects}"

    # copy back
    client.copy(f"{cloudflare_bucket.path()}/{test_case}", f"gs://{gcp_bucket.bucket()}/cloudflare/", recursive=recursive)


@pytest.mark.parametrize(
    "test_case, recursive", [(test_bucket_medium_file, True), (test_bucket_large_file, False), (test_bucket_small_file, True)]
)
def test_gcp(gcp_bucket, test_case, recursive):
    """
    Test copying a big file to different cloud providers
    :param gcp_bucket: destination interface
    :param gcp_bucket: gcp bucket to copy FROM dstiface
    :param test_case: test case from test_bucket to copy from
    """
    client = SkyplaneClient()
    src_iface = ObjectStoreInterface.create("gcp:us-west2", test_bucket.split("://")[1])

    assert isinstance(gcp_bucket.bucket(), str), f"Bucket name is not a string {gcp_bucket.bucket()}"
    assert (
        len(list(src_iface.list_objects(prefix=test_case.replace(f"{test_bucket}/", "")))) > 0
    ), f"Test case {test_case} does not exist in {test_bucket}"
    client.copy(test_case, f"{gcp_bucket.path()}/{test_case}", recursive=recursive)

    # assert sync has cost zero
    dst_objects = list(gcp_bucket.list_objects())
    assert len(dst_objects) > 0, f"Object {test_case} not copied to {gcp_bucket.bucket()}: only container {dst_objects}"


@pytest.mark.timeout(60 * 20)
def test_same_region(same_region_bucket):
    client = SkyplaneClient()
    client.copy(test_bucket_large_file, f"{same_region_bucket.path()}")


@pytest.mark.timeout(60 * 20)
def test_pipeline(gcp_bucket):
    """Test pipeline's ability to run multiple copy jobs on a single dataplane"""
    client = SkyplaneClient()
    pipeline = client.pipeline()

    # queue two copy jobs
    pipeline.queue_copy(test_bucket_large_file, f"{gcp_bucket.path()}/large/")
    pipeline.queue_copy(test_bucket_medium_file, f"{gcp_bucket.path()}/medium/", recursive=True)

    # start pipeline
    try:
        pipeline.start(debug=True, progress=True)
    except Exception as e:
        print(e)
        raise e

    assert len(list(gcp_bucket.list_objects(prefix="large/"))) > 0, f"No data from {test_bucket_large_file} transferred"
    assert len(list(gcp_bucket.list_objects(prefix="medium/"))) > 0, f"No data from {test_bucket_medium_file} transferred"


# test one sided transfers
def test_cp_one_sided():
    # TODO: run on-sided tranfer between all cloud pairs
    pass


# test multiple VMs
@pytest.mark.timeout(60 * 20)
def test_cp_multiple_vms(aws_bucket):
    client = SkyplaneClient()
    pipeline = client.pipeline(max_instances=2)
    pipeline.queue_copy(test_bucket_large_file, f"s3://{aws_bucket.bucket()}/")
    pipeline.start(debug=True, progress=True)


# test multicast
# TODO: add azure
@pytest.mark.timeout(60 * 20)
def test_cp_multicast(aws_bucket, gcp_bucket, same_region_bucket):
    client = SkyplaneClient()
    src_iface = ObjectStoreInterface.create("gcp:us-west2", test_bucket.split("://")[1])
    assert (
        len(list(src_iface.list_objects(prefix=test_bucket_large_file.replace(f"{test_bucket}/", "")))) > 0
    ), f"Test case {test_bucket_large_file} does not exist in {test_bucket}"
    client.copy(
        test_bucket_large_file, [f"s3://{aws_bucket}/", f"gs://{gcp_bucket}/", f"gs://{same_region_bucket}/", f"azure://{azure_bucket}/"]
    )
