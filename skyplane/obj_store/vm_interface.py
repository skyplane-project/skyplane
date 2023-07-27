from dataclasses import dataclass
from datetime import datetime, timezone
import json
import mimetypes
from typing import Iterator, List, Optional
from dateutil.parser import parse
import paramiko
import pytz
from skyplane.obj_store.object_store_interface import (
    ObjectStoreInterface,
    ObjectStoreObject,
)
from skyplane import exceptions


@dataclass
class VMFile(ObjectStoreObject):
    def full_path(self):
        return f"vm://{self.key}"


class VMInterface(ObjectStoreInterface):
    def __init__(
        self,
        host,
        username,
        region,
        private_key_path,
        local_path="/",
        ssh_key_password="skyplane",
    ):
        self.host = host
        self.username = username
        self.region = region
        self.private_key_path = private_key_path
        self.local_path = local_path

        # Set up SSH
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
            ssh_client.connect(
                hostname=host,
                username=username,
                pkey=paramiko.RSAKey.from_private_key_file(str(private_key_path), password=ssh_key_password),
                look_for_keys=False,
                banner_timeout=200,
            )
            self.client = ssh_client
        except paramiko.AuthenticationException as e:
            raise exceptions.BadConfigException(f"Failed to connect to Server") from e

        # TODO: check if this works for all CPs
        if self.region.startswith("aws"):
            _, stdout, _ = self.client.exec_command("curl http://169.254.169.254/latest/meta-data/instance-id")
            self.instance_id = stdout.read().decode("utf-8").strip()
        elif self.region.startswith("gcp"):
            _, stdout, _ = self.client.exec_command(
                'curl "http://metadata.google.internal/computeMetadata/v1/instance/name" -H "Metadata-Flavor: Google"'
            )
            self.instance_id = stdout.read().decode("utf-8").strip()
        elif self.region.startswith("azure"):
            _, stdout, _ = self.client.exec_command(
                'curl -H Metadata:true "http://169.254.169.254/metadata/instance/compute?api-version=2017-08-01"'
            )
            metadata = json.loads(stdout.read().decode("utf-8").strip())
            self.instance_id = metadata["name"]
        else:
            raise exceptions.BadConfigException(f"Invalid region tag: {self.region}")

    def region_tag(self) -> str:
        return self.region

    def id(self) -> str:
        return self.instance_id

    def path(self) -> str:
        return self.local_path

    def bucket(self) -> str:
        return self.local_path

    def list_objects(self, prefix="") -> Iterator[VMFile]:
        # List files in directory, recursively
        _, stdout, _ = self.client.exec_command(f"find {prefix} -type f")
        files = stdout.readlines()
        for file_path in files:
            file_path = file_path.strip()
            _, stdout, _ = self.client.exec_command(f"ls -l --time-style=full-iso {file_path}")
            file_info = stdout.readline().split()
            file_size = file_info[4]
            # Get the last modified time from the ls output
            file_datetime_str = " ".join(file_info[5:8])
            datetime_str, _ = file_datetime_str.rsplit(" ", 1)
            timestamp, nanosec = datetime_str.split(".")
            timestamp_str = f"{timestamp}.{nanosec[:6]}"

            dt_naive = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S.%f")
            dt_aware = dt_naive.replace(tzinfo=timezone.utc)

            yield VMFile(
                provider="vm",
                bucket=self.host,
                key=file_path,
                size=int(file_size),
                last_modified=dt_aware,
            )

    def exists(self, obj_name: str):
        _, stdout, _ = self.client.exec_command(f"ls {self.path}/{obj_name}")
        return stdout.readline() != ""

    def create_bucket(self, region_tag: str):
        return None

    def delete_bucket(self):
        return None

    def bucket_exists(self) -> bool:
        """We always have a bucket"""
        return True

    def download_object(self, src_object_name, dst_file_path):
        sftp = self.client.open_sftp()
        sftp.get(f"{self.path}/{src_object_name}", dst_file_path)

    def upload_object(self, src_file_path, dst_object_name):
        sftp = self.client.open_sftp()
        sftp.put(src_file_path, f"{self.path}/{dst_object_name}")

    def delete_objects(self, keys: List[str]):
        for key in keys:
            self.client.exec_command(f"rm {self.path}/{key}")

    def get_obj_size(self, obj_name) -> int:
        _, stdout, _ = self.client.exec_command(f"ls -l {self.path}/{obj_name}")
        return int(stdout.readline().split()[4])

    def get_obj_last_modified(self, obj_name):
        _, stdout, _ = self.client.exec_command(f"ls -l --time-style=full-iso {self.path}/{obj_name}")
        file_info = stdout.readline().split()
        file_datetime_str = " ".join(file_info[5:8])

        # parse datetime string and convert it to UTC
        dt_aware = parse(file_datetime_str)
        dt_utc = dt_aware.astimezone(pytz.UTC)

        return dt_utc

    def get_obj_mime_type(self, obj_name):
        return mimetypes.guess_type(obj_name)[0]

    def initiate_multipart_upload(self, dst_object_name: str, mime_type: Optional[str] = None) -> str:
        raise NotImplementedError(f"Multipart upload is not supported for the VM File System.")
