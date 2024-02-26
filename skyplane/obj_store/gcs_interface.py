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


class GCSObject(ObjectStoreObject):
    def full_path(self):
        return os.path.join(f"gs://{self.bucket}", self.key)


class GCSInterface(ObjectStoreInterface):
    def __init__(self, bucket_name: str):
        self.bucket_name = bucket_name
        self.auth = compute.GCPAuthentication()
        self._gcs_client = self.auth.get_storage_client()
        self._requests_session = requests.Session()

    @property
    def provider(self):
        return "gcp"

    def path(self):
        return f"gs://{self.bucket_name}"

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

        # load bucket from GCS client
        bucket = None
        default_region = cloud_config.get_flag("gcp_default_region")
        try:
            bucket = self._gcs_client.lookup_bucket(self.bucket_name)
        except Exception as e:
            # does not have storage.buckets.get access to the Google Cloud Storage bucket
            if "access to the Google Cloud Storage bucket" in str(e):
                logger.warning(
                    f"No access to the Google Cloud Storage bucket '{self.bucket_name}', assuming bucket is in the '{default_region}' zone"
                )
                return default_region
            raise
        if bucket is None:
            raise exceptions.MissingBucketException(f"GCS bucket {self.bucket_name} does not exist")
        else:
            loc = bucket.location.lower()
            if default_region.startswith(loc):
                loc = default_region
            return map_region_to_zone(loc)

    def region_tag(self):
        return "gcp:" + self.gcp_region

    def bucket(self) -> str:
        return self.bucket_name

    def bucket_exists(self):
        iterator = self._gcs_client.list_blobs(self.bucket_name, page_size=1)
        try:
            next(iterator.pages, None)
            return True
        except Exception as e:
            if "The specified bucket does not exist" in str(e):
                return False
            raise

    def exists(self, obj_name):
        try:
            self.get_obj_metadata(obj_name)
            return True
        except NoSuchObjectException:
            return False

    def create_bucket(self, gcp_region, premium_tier=True):
        assert premium_tier, "Standard tier GCS buckets are not supported"
        if not self.bucket_exists():
            bucket = self._gcs_client.bucket(self.bucket_name)
            bucket.storage_class = "STANDARD"
            region_without_zone = "-".join(gcp_region.split("-")[:2])
            self._gcs_client.create_bucket(bucket, location=region_without_zone)

    def delete_bucket(self):
        for batch in batch_generator(self.list_objects(), 1000):
            self.delete_objects([obj.key for obj in batch])
        assert len(list(self.list_objects())) == 0, f"Bucket not empty after deleting all keys {list(self.list_objects())}"
        self._gcs_client.get_bucket(self.bucket_name).delete()

    def list_objects(self, prefix="", region=None) -> Iterator[GCSObject]:
        blobs = self._gcs_client.list_blobs(self.bucket_name, prefix=prefix)
        for blob in blobs:
            yield GCSObject(
                blob.name,
                provider="gcp",
                bucket=self.bucket_name,
                size=blob.size,
                last_modified=blob.updated,
                mime_type=getattr(blob, "content_type", None),
            )

    def delete_objects(self, keys: List[str]):
        for key in keys:
            self._gcs_client.bucket(self.bucket_name).blob(key).delete()

    @lru_cache(maxsize=1024)
    def get_obj_metadata(self, obj_name):
        bucket = self._gcs_client.bucket(self.bucket_name)
        blob = bucket.get_blob(obj_name)
        if blob is None:
            raise NoSuchObjectException(
                f"Object {obj_name} does not exist in bucket {self.bucket_name}, or you do not have permission to access it"
            )
        return blob

    def get_obj_size(self, obj_name):
        return self.get_obj_metadata(obj_name).size

    def get_obj_last_modified(self, obj_name):
        return self.get_obj_metadata(obj_name).updated

    def get_obj_mime_type(self, obj_name):
        return self.get_obj_metadata(obj_name).content_type

    def send_xml_request(
        self,
        blob_name: str,
        params: dict,
        method: str,
        headers: Optional[dict] = None,
        expiration=datetime.timedelta(minutes=15),
        data=None,
        content_type="application/octet-stream",
    ):
        blob = self._gcs_client.bucket(self.bucket_name).blob(blob_name)

        headers = headers or {}
        headers["Content-Type"] = content_type

        # generate signed URL
        url = blob.generate_signed_url(
            version="v4", expiration=expiration, method=method, content_type=content_type, query_parameters=params, headers=headers
        )

        # prepare request
        if data:
            req = requests.Request(method, url, headers=headers, data=data)
        else:
            req = requests.Request(method, url, headers=headers)

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
        bucket = self._gcs_client.bucket(self.bucket_name)
        blob = bucket.blob(src_object_name)

        # download object
        # TODO: download directly to file?
        if offset_bytes is None:
            chunk = blob.download_as_string()
        else:
            assert offset_bytes is not None and size_bytes is not None
            chunk = blob.download_as_string(start=offset_bytes, end=offset_bytes + size_bytes - 1)

        # write output
        if not os.path.exists(dst_file_path):
            open(dst_file_path, "a").close()
        if generate_md5:
            m = hashlib.md5()
        with open(dst_file_path, "wb+" if write_at_offset else "wb") as f:
            f.seek(offset_bytes if write_at_offset else 0)
            f.write(chunk)
            if generate_md5:
                m.update(chunk)
        md5 = m.digest() if generate_md5 else None
        mime_type = blob.content_type
        return mime_type, md5

    def upload_object(self, src_file_path, dst_object_name, part_number=None, upload_id=None, check_md5=None, mime_type=None):
        src_file_path, dst_object_name = str(src_file_path), str(dst_object_name)
        dst_object_name = dst_object_name if dst_object_name[0] != "/" else dst_object_name
        os.path.getsize(src_file_path)
        bucket = self._gcs_client.bucket(self.bucket_name)
        b64_md5sum = base64.b64encode(check_md5).decode("utf-8") if check_md5 else None

        if part_number is None:
            blob = bucket.blob(dst_object_name)
            blob.upload_from_filename(src_file_path, content_type=mime_type)
            if check_md5:
                blob_md5 = blob.md5_hash
                if b64_md5sum != blob_md5:
                    raise exceptions.ChecksumMismatchException(
                        f"Checksum mismatch for object {dst_object_name} in bucket {self.bucket_name}, "
                        + f"expected {b64_md5sum}, got {blob_md5}"
                    )
            return

        # multipart upload
        assert part_number is not None, f"Part number cannot be none for multipart upload: {part_number}, {upload_id}"
        assert upload_id is not None, f"Upload ID cannot be none for multipart upload: {part_number}, {upload_id}"

        # send XML api request
        headers = {"Content-MD5": b64_md5sum} if check_md5 else None
        try:
            response = self.send_xml_request(
                dst_object_name,
                {"uploadId": upload_id, "partNumber": part_number},
                "PUT",
                headers=headers,
                data=open(src_file_path, "rb"),
            )
        except Exception as e:
            raise ValueError(f"Failed to upload {dst_object_name} to bucket {self.bucket_name} upload id {upload_id}: {e}")

        # check response
        if "ETag" not in response.headers:
            raise exceptions.ChecksumMismatchException(
                f"Upload of object {dst_object_name} in bucket {self.bucket_name} failed, got status code {response.status_code} w/ response {response.text}"
            )

    def initiate_multipart_upload(self, dst_object_name: str, mime_type: Optional[str] = None) -> str:
        assert len(dst_object_name) > 0, f"Destination object name must be non-empty: '{dst_object_name}'"
        response = self.send_xml_request(dst_object_name, {"uploads": None}, "POST", content_type=mime_type)
        return ElementTree.fromstring(response.content)[2].text

    def complete_multipart_upload(self, dst_object_name, upload_id, metadata: Optional[Any] = None):
        # get parts
        xml_data = ElementTree.Element("CompleteMultipartUpload")
        next_part_number_marker = None

        # Parts in the list are ordered sequentially, and the XML API does not return lists longer than 1000 parts.
        while True:
            if next_part_number_marker is None:
                response = self.send_xml_request(dst_object_name, {"uploadId": upload_id}, "GET")
            else:
                response = self.send_xml_request(
                    dst_object_name, {"uploadId": upload_id, "part-number-marker": next_part_number_marker}, "GET"
                )

            # build request xml tree
            tree = ElementTree.fromstring(response.content)
            ns = {"ns": tree.tag.split("}")[0][1:]}
            for part in tree.findall("ns:Part", ns):
                part_xml = ElementTree.Element("Part")
                etag_match = part.find("ns:ETag", ns)
                assert etag_match is not None
                etag = etag_match.text
                part_num_match = part.find("ns:PartNumber", ns)
                assert part_num_match is not None
                part_num = part_num_match.text
                ElementTree.SubElement(part_xml, "PartNumber").text = part_num
                ElementTree.SubElement(part_xml, "ETag").text = etag
                xml_data.append(part_xml)

            is_truncated = tree.findall("ns:IsTruncated", ns)[0].text
            if is_truncated == "false":
                break
            else:
                next_part_number_marker = tree.findall("ns:NextPartNumberMarker", ns)[0].text

        xml_data = ElementTree.tostring(xml_data, encoding="utf-8", method="xml")
        xml_data = xml_data.replace(b"ns0:", b"")

        try:
            # complete multipart upload
            response = self.send_xml_request(
                dst_object_name, {"uploadId": upload_id}, "POST", data=xml_data, content_type="application/xml"
            )
        except Exception as e:
            # cancel upload
            response = self.send_xml_request(dst_object_name, {"uploadId": upload_id}, "DELETE")
            raise exceptions.SkyplaneException("Failed to complete multipart upload") from e
