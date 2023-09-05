import base64
import datetime
import hashlib
import os
from functools import lru_cache
from xml.etree import ElementTree


import requests
from typing import Any, Iterator, List, Optional, Tuple

from skyplane import exceptions, compute
from skyplane.config_paths import cloud_config
from skyplane.exceptions import NoSuchObjectException
from skyplane.obj_store.object_store_interface import ObjectStoreInterface, ObjectStoreObject
from skyplane.utils import logger
from skyplane.utils.generator import batch_generator

class BQIObject(ObjectStoreObject):
    def full_path(self):
        return os.path.join(f"bq://{self.bucket}", self.key)

# in BigQuery, bucket name consists of (projectname.datasetname)
class BQIInterface(ObjectStoreInterface):
    def __init__(self, bucket_name: str):
        self.bucket_name = bucket_name
        self.auth = compute.GCPAuthentication()
        self.full_name = self.auth.project_id + "." + bucket_name
        self._bigquery_object = self.auth.get_bigquery_object()
        self._bigquery_client = self.auth.get_bigquery_client()
        self._requests_session = requests.Session()

    @property
    def provider(self):
        return "bq"

    def path(self):
        return f"bq://{self.bucket_name}"


    @property
    @lru_cache(maxsize=1)
    def gcp_region(self):
        def map_region_to_zone(region) -> str:
            """Resolves bucket locations to a valid zone."""
            parsed_region = region.lower().split("-")
            if len(parsed_region) == 3:
                return region
            elif len(parsed_region) == 2 or len(parsed_region) == 1:
                # query the API to get the list of zones in the region and return the first one
                compute = self.auth.get_gcp_client()
                zones = compute.zones().list(project=self.auth.project_id).execute()
                for zone in zones["items"]:
                    if zone["name"].startswith(region):
                        return zone["name"]
            raise ValueError(f"No GCP zone found for region {region}")

        # load dataset from BigQuery client
        bucket = None
        default_region = cloud_config.get_flag("gcp_default_region")
        try:
            bucket = self._bigquery_client.get_dataset(self.full_name)
        except Exception as e:
            # does not have storage.buckets.get access to the Google Cloud Storage bucket
            if "access to the Google Cloud Storage bucket" in str(e):
                logger.warning(
                    f"No access to the Google Cloud BigQuery Dataset '{self.full_name}', assuming bucket is in the '{default_region}' zone"
                )
                return default_region
            raise
        if bucket is None:
            raise exceptions.MissingBucketException(f"GCS BigQuery Dataset {self.bucket_name} does not exist")
        else:
            loc = bucket.location.lower()
            if default_region.startswith(loc):
                loc = default_region
            return map_region_to_zone(loc)

    def region_tag(self):
        return "bq:" + self.gcp_region

    def bucket(self) -> str:
        return self.bucket_name

    def bucket_exists(self):
        try:
            self._bigquery_client.get_dataset(self.full_name)
            return True
        except Exception as e:
            if "Not found:" in str(e) or "Request couldn't be served" in str(e):     
            # Not found when Dataset cannot be found, Reqeust couldn't be served when Project name DNE
                return False
            raise

    def exists(self, obj_name):
        try:
            self.get_obj_metadata(obj_name)
            return True
        except NoSuchObjectException:
            return False

    #Creates Dataset for BigQuery
    def create_bucket(self, gcp_region, premium_tier=True):
        if not self.bucket_exists():
            dataset = self._bigquery_object.Dataset(self.full_name)
            region_without_zone = "-".join(gcp_region.split("-")[:2])
            dataset.location = region_without_zone
            self._bigquery_client.create_dataset(dataset, timeout=30) 

    #Deletes a dataset
    def delete_bucket(self):
        self._bigquery_client.delete_dataset(self.full_name, delete_contents=True, not_found_ok=True)

    def list_objects(self, prefix="", region=None) -> Iterator[BQIObject]:
        tables = self._bigquery_client.list_tables(self.full_name)
        for table in tables:
            if (not prefix or table.table_id.startswith(prefix)):
                tableobj = self._bigquery_client.get_table(table)
                yield BQIObject(
                    tableobj.dataset_id,
                    provider="bq",
                    bucket=self.bucket_name,
                    last_modified=tableobj.modified,
                    mime_type=getattr(table, "content_type", None),
                )

    #Deletes a table inside a dataset
    def delete_objects(self, keys: List[str]):
        for key in keys:
            table_id = self.full_name + "." +key
            self._bigquery_client.delete_table(table_id, not_found_ok=True)

    # Returns reference of a table inside of a dataset
    @lru_cache(maxsize=1024)
    def get_obj_metadata(self, obj_name):
        dataset_ref = self._bigquery_client.get_dataset(self.full_name)
        table_ref = dataset_ref.table(obj_name)
        print("printing dataset_reference")
        print(dataset_ref)
        print("printing table_reference")
        print(table_ref)
        table = None
        try: 
            table = self._bigquery_client.get_table(table_ref)
        except Exception as e:
            table = None
        if table is None: 
            raise NoSuchObjectException(
                f"Object {obj_name} does not exist in bucket {self.bucket_name}, or you do not have permission to access it"
            )

        return table

    def get_obj_size(self, obj_name):
        return self.get_obj_metadata(obj_name).num_bytes

    def get_obj_last_modified(self, obj_name):
        return self.get_obj_metadata(obj_name).modified

    def send_xml_request(
        self,
        blob_name: str,
        method: str,
        headers: Optional[dict] = None,
        data=None,
        content_type="multipart/related",
        content_length=0
    ):
        dummy_data = '''
        --foo_bar_baz
        Content-Type: application/json; charset=UTF-8

        {
        "configuration": {
            "load": {
            "sourceFormat": "NEWLINE_DELIMITED_JSON",
            "schema": {
                "fields": [
                {"name": "f1", "type": "STRING"},
                {"name": "f2", "type": "INTEGER"}
                ]
            },
            "destinationTable": {
                "projectId": "projectId",
                "datasetId": "datasetId",
                "tableId": "tableId"
            }
            }
            }
        }

        --foo_bar_baz
        Content-Type: */*
        CSV, JSON, AVRO, PARQUET, or ORC data
        --foo_bar_baz--
        '''
        print(self.auth._credentials)
        print(dummy_data)
        headers = headers or {}
        headers["Host"] = 'www.googleapis.com'
        headers["Content-Type"] = 'multipart/related; boundary=foo_bar_baz'
        headers["Content-Length"] = str(len(dummy_data.encode('utf-8')))
        headers["Authorization"] = f"Bearer {self.auth._credentials.token}"
        print(headers)
        # generate BigQuery multipart upload URL
        project_name = self.full_name.split(".")[0]
        print(project_name)
        url = f"https://www.googleapis.com/upload/bigquery/v2/projects/{project_name}/jobs?uploadType=multipart"

        # prepare request
        if data:
            req = requests.Request(method, url, headers=headers, data=dummy_data)
        else:
            req = requests.Request(method, url, headers=headers, data=dummy_data)

        prepared = req.prepare()
        response = self._requests_session.send(prepared)

        if not response.ok:
            raise ValueError(f"Invalid status code {response.status_code}: {response.text}")

        return response

    def download_object(
        self, src_object_name, dst_file_path, offset_bytes=None, size_bytes=None, write_at_offset=False, generate_md5=False
    ) -> Tuple[Optional[str], Optional[bytes]]:
        src_object_name, dst_file_path = str(src_object_name), str(dst_file_path)
        src_object_name = src_object_name if src_object_name[0] != "/" else src_object_name

        table = self.get_obj_metadata(src_object_name)
        blob = self._bigquery_client.list_rows(table).to_dataframe()
        if not os.path.exists(dst_file_path):
            open(dst_file_path, "a").close()
        blob.to_csv(dst_file_path, index=False, header=True)
        mime_type = "text/csv"
        return mime_type, None 


    def upload_object(self, src_file_path, dst_object_name, part_number=None, upload_id=None, check_md5=None, mime_type=None):
        src_file_path, dst_object_name = str(src_file_path), str(dst_object_name)
        dst_object_name = dst_object_name if dst_object_name[0] != "/" else dst_object_name
        os.path.getsize(src_file_path)

        job_config = self._bigquery_object.LoadJobConfig(
            source_format = self._bigquery_object.SourceFormat.CSV,
            autodetect = True,
        )
        dataset_ref = self._bigquery_client.dataset(self.bucket_name)
        table_ref = dataset_ref.table(dst_object_name)
        table_id = self.full_name
        if part_number is None: #NOT Multipart Upload
            with open(src_file_path, "rb") as source_file: 
                try: 
                    job = self._bigquery_client.load_table_from_file(source_file, table_ref, job_config = job_config)
                    job.result()
                except Exception as e:
                    raise; 
            return


    def initiate_multipart_upload(self, dst_object_name: str, mime_type: Optional[str] = None) -> str:
        assert len(dst_object_name) > 0, f"Destination object name must be non-empty: '{dst_object_name}'"
        response = self.send_xml_request(dst_object_name, "POST", content_type=mime_type)
        return ElementTree.fromstring(response.content)[2].text

