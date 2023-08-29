import uuid
import datetime
import time

from skyplane.obj_store.object_store_interface import ObjectStoreInterface
from tests.interface_bigquery_util import bigquery_test_framework

# def test_gcs_singlepart():
#     assert bigquery_test_framework("bqi:us-central1", f"skyplane-test-bq.skyplane_{uuid.uuid4().hex}", False, test_delete_bucket=True)

def test_gcs_multipart():
    assert bigquery_test_framework("bqi:us-central1", f"skyplane-test-bq.skyplane_{uuid.uuid4().hex}", True, test_delete_bucket=True)

# def test_bqi_bucket_exists():
#     # test a public bucket with objects
#     iface = ObjectStoreInterface.create("bqi:infer", "roka-srs.cloud_tpu_test_datasets")
#     iface.create_bucket("us-central1-a")
#     data = iface.get_obj_metadata("hello")
#     print(iface.get_obj_last_modified("hello"))
#     objects = iface.list_objects()
#     for obj in objects:
#         print(obj)
#     iface.download_object("hello", "/Users/briankim/desktop", generate_md5=True)
#     iface.upload_object("/Users/briankim/desktop/test.csv", "test2")
#     time.sleep(3)
#     assert iface.bucket_exists()