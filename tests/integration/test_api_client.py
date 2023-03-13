import pytest
from skyplane.api.client import SkyplaneClient
import uuid
import os

@pytest.mark.skip(reason="Shared function")
def test_region(region):
    client = SkyplaneClient().object_store()
    key = str(uuid.uuid4()).replace("-", "")
    src_filename = f"src_{key}"
    dst_filename = f"dst_{key}"
    provider = region.split(":")[0]
    if provider == "azure":
        # need both storage account and container
        bucket_name = str(uuid.uuid4()).replace("-", "")[:24] + "/" + str(uuid.uuid4()).replace("-", "")
    else:
        bucket_name = str(uuid.uuid4()).replace("-", "")
    file_size = 1024

    # create bucket
    bucket_path = client.create_bucket(region, bucket_name)
    assert client.bucket_exists(bucket_name, provider), f"Bucket {bucket_name} does not exist"

    # upload object
    with open(src_filename, "wb") as fout:
        fout.write(os.urandom(file_size))
    client.upload_object(src_filename, bucket_name, provider, key)
    assert client.exists(bucket_name, provider, key), f"Object {key} does not exist in bucket {bucket_name}"

    # download object
    client.download_object(bucket_name, provider, key, dst_filename)
    assert (
        open(src_filename, "rb").read() == open(dst_filename, "rb").read()
    ), f"Downloaded file {dst_filename} does not match uploaded file {src_filename}"

    # delete bucket
    client.delete_bucket(bucket_name, provider)
    assert not client.bucket_exists(bucket_name, provider), f"Bucket {bucket_name} still exists"

    # cleanup
    os.remove(src_filename)
    os.remove(dst_filename)


# TODO: implement azure
# def test_azure_interface():
#    test_region("azure:canadacentral")
#    return True


def test_aws_interface():
    test_region("aws:us-east-1")
    return True


def test_gcp_interface():
    test_region("gcp:us-central1-a")
    return True
