import pytest
from skyplane.api.client import SkyplaneClient
import uuid
import os


def test_cost_estimate():
    client = SkyplaneClient()
    obj_store = client.object_store()
    src_region = "gcp:us-central1-a"
    src_provider = "gcp"
    dst_region = "aws:us-east-1"
    dst_provider = "aws"

    # setup buckets
    key = str(uuid.uuid4()).replace("-", "")
    bucket_name = str(uuid.uuid4()).replace("-", "")
    src_filename = f"src_{key}"
    file_size = int(1e6)  # 1 MB

    # create bucket
    src_bucket = obj_store.create_bucket(src_region, bucket_name)
    dst_bucket = obj_store.create_bucket(dst_region, bucket_name)
    assert obj_store.bucket_exists(bucket_name, src_provider), f"Bucket {bucket_name} does not exist"
    assert obj_store.bucket_exists(bucket_name, dst_provider), f"Bucket {bucket_name} does not exist"

    # upload object
    with open(src_filename, "wb") as fout:
        fout.write(os.urandom(file_size))
    obj_store.upload_object(src_filename, bucket_name, src_provider, key)
    assert obj_store.exists(bucket_name, src_provider, key), f"Object {key} does not exist in bucket {bucket_name}"

    # setup transfer
    dp = client.dataplane(src_provider, src_region.split(":")[1], dst_provider, dst_region.split(":")[1], n_vms=1)
    dp.queue_copy(src_bucket + "/" + key, dst_bucket + "/" + key, recursive=False)
    cost = dp.estimate_total_cost()
    assert cost == file_size * 0.12 / 1e9, f"Cost estimate is incorrect: {cost}, should be {file_size * 0.09 / 1e9}"

    # cleanup
    obj_store.delete_bucket(bucket_name, src_provider)
    obj_store.delete_bucket(bucket_name, dst_provider)
    assert not obj_store.bucket_exists(bucket_name, src_provider), f"Bucket {bucket_name} still exists"
    assert not obj_store.bucket_exists(bucket_name, dst_provider), f"Bucket {bucket_name} still exists"

    # cleanup
    os.remove(src_filename)
