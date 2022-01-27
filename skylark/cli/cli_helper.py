import concurrent.futures
import json
import os
import re
import resource
import subprocess
from pathlib import Path
from shutil import copyfile
from typing import Dict, List, Optional

import typer
from loguru import logger
from skylark import config_file
from skylark.compute.aws.aws_cloud_provider import AWSCloudProvider
from skylark.compute.azure.azure_cloud_provider import AzureCloudProvider
from skylark.compute.gcp.gcp_cloud_provider import GCPCloudProvider
from skylark.obj_store.object_store_interface import ObjectStoreObject
from skylark.obj_store.s3_interface import S3Interface
from skylark.obj_store.gcs_interface import GCSInterface
from skylark.utils.utils import do_parallel
from tqdm import tqdm


def is_plausible_local_path(path: str):
    path = Path(path)
    if path.exists():
        return True
    if path.is_dir():
        return True
    if path.parent.exists():
        return True
    return False


def parse_path(path: str):
    if path.startswith("s3://"):
        bucket_name, key_name = path[5:].split("/", 1)
        return "s3", bucket_name, key_name
    elif path.startswith("gs://"):
        bucket_name, key_name = path[5:].split("/", 1)
        return "gs", bucket_name, key_name
    elif (path.startswith("https://") or path.startswith("http://")) and "blob.core.windows.net" in path:
        regex = re.compile(r"https?://([^/]+).blob.core.windows.net/([^/]+)/(.*)")
        match = regex.match(path)
        if match is None:
            raise ValueError(f"Invalid Azure path: {path}")
        account, container, blob_path = match.groups()
        return "azure", account, container, blob_path
    elif is_plausible_local_path(path):
        return "local", None, path
    return path


# skylark ls implementation
def ls_local(path: Path):
    if not path.exists():
        raise FileNotFoundError(path)
    if path.is_dir():
        for child in path.iterdir():
            yield child.name
    else:
        yield path.name


def ls_s3(bucket_name: str, key_name: str, use_tls: bool = True):
    s3 = S3Interface(None, bucket_name, use_tls=use_tls)
    for obj in s3.list_objects(prefix=key_name):
        yield obj.full_path()


# skylark cp implementation


def copy_local_local(src: Path, dst: Path):
    if not src.exists():
        raise FileNotFoundError(src)
    if not dst.parent.exists():
        raise FileNotFoundError(dst.parent)

    if src.is_dir():
        dst.mkdir(exist_ok=True)
        for child in src.iterdir():
            copy_local_local(child, dst / child.name)
    else:
        dst.parent.mkdir(exist_ok=True, parents=True)
        copyfile(src, dst)

# TODO: probably shoudl merge this with the s3 function (duplicate code)
def copy_local_gcs(src: Path, dst_bucket: str, dst_key: str):
    gcs = GCSInterface(None, dst_bucket)
    ops: List[concurrent.futures.Future] = []
    path_mapping: Dict[concurrent.futures.Future, Path] = {}

    def _copy(path: Path, dst_key: str, total_size=0.0):
        if path.is_dir():
            for child in path.iterdir():
                total_size += _copy(child, os.path.join(dst_key, child.name))
            return total_size
        else:
            future = gcp.upload_object(path, dst_key)
            ops.append(future)
            path_mapping[future] = path
            return path.stat().st_size

    total_bytes = _copy(src, dst_key)

    # wait for all uploads to complete, displaying a progress bar
    with tqdm(total=total_bytes, unit="B", unit_scale=True, unit_divisor=1024, desc="Uploading") as pbar:
        for op in concurrent.futures.as_completed(ops):
            op.result()
            pbar.update(path_mapping[op].stat().st_size)

# TODO: probably shoudl merge this with the s3 function (duplicate code)
def copy_gcs_local(src_bucket: str, src_key: str, dst: Path):
    gcs = GCSInterface(None, src_bucket)
    ops: List[concurrent.futures.Future] = []
    obj_mapping: Dict[concurrent.futures.Future, ObjectStoreObject] = {}

    # copy single object
    def _copy(src_obj: ObjectStoreObject, dst: Path):
        dst.parent.mkdir(exist_ok=True, parents=True)
        future = gcs.download_object(src_obj.key, dst)
        ops.append(future)
        obj_mapping[future] = src_obj
        return src_obj.size

    total_bytes = 0.0
    for obj in gcs.list_objects(prefix=src_key):
        sub_key = obj.key[len(src_key) :]
        sub_key = sub_key.lstrip("/")
        dest_path = dst / sub_key
        total_bytes += _copy(obj, dest_path)

    # wait for all downloads to complete, displaying a progress bar
    with tqdm(total=total_bytes, unit="B", unit_scale=True, unit_divisor=1024, desc="Downloading") as pbar:
        for op in concurrent.futures.as_completed(ops):
            op.result()
            pbar.update(obj_mapping[op].size)

def copy_local_s3(src: Path, dst_bucket: str, dst_key: str, use_tls: bool = True):
    s3 = S3Interface(None, dst_bucket, use_tls=use_tls)
    ops: List[concurrent.futures.Future] = []
    path_mapping: Dict[concurrent.futures.Future, Path] = {}

    def _copy(path: Path, dst_key: str, total_size=0.0):
        if path.is_dir():
            for child in path.iterdir():
                total_size += _copy(child, os.path.join(dst_key, child.name))
            return total_size
        else:
            future = s3.upload_object(path, dst_key)
            ops.append(future)
            path_mapping[future] = path
            return path.stat().st_size

    total_bytes = _copy(src, dst_key)

    # wait for all uploads to complete, displaying a progress bar
    with tqdm(total=total_bytes, unit="B", unit_scale=True, unit_divisor=1024, desc="Uploading") as pbar:
        for op in concurrent.futures.as_completed(ops):
            op.result()
            pbar.update(path_mapping[op].stat().st_size)


def copy_s3_local(src_bucket: str, src_key: str, dst: Path):
    s3 = S3Interface(None, src_bucket)
    ops: List[concurrent.futures.Future] = []
    obj_mapping: Dict[concurrent.futures.Future, ObjectStoreObject] = {}

    # copy single object
    def _copy(src_obj: ObjectStoreObject, dst: Path):
        dst.parent.mkdir(exist_ok=True, parents=True)
        future = s3.download_object(src_obj.key, dst)
        ops.append(future)
        obj_mapping[future] = src_obj
        return src_obj.size

    total_bytes = 0.0
    for obj in s3.list_objects(prefix=src_key):
        sub_key = obj.key[len(src_key) :]
        sub_key = sub_key.lstrip("/")
        dest_path = dst / sub_key
        total_bytes += _copy(obj, dest_path)

    # wait for all downloads to complete, displaying a progress bar
    with tqdm(total=total_bytes, unit="B", unit_scale=True, unit_divisor=1024, desc="Downloading") as pbar:
        for op in concurrent.futures.as_completed(ops):
            op.result()
            pbar.update(obj_mapping[op].size)


# utility functions


def check_ulimit(hard_limit=1024 * 1024 * 2, soft_limit=1024 * 1024):
    current_limit_soft, current_limit_hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    typer.secho(f"Current soft limit: {current_limit_soft}, current hard limit: {current_limit_hard}", fg="yellow")
    if current_limit_soft < soft_limit:
        typer.secho(f"Warning: ulimit is set to {current_limit_soft}, which is less than the recommended minimum of {soft_limit}", fg="red")
        if typer.confirm("sudo required; Do you want to increase the limit?"):
            os.system(f"sudo sysctl -w fs.file-max={hard_limit}")
            new_limit = subprocess.check_output(["sysctl", "-n", "fs.file-max"]).decode("utf-8").strip()
            typer.secho(f"New system limit: {new_limit}", fg="green")

            # set soft and hard limit
            subprocess.check_call(["sudo", "prlimit", "--pid", str(os.getpid()), f"--nofile={soft_limit}:{hard_limit}"])
            new_limit = resource.getrlimit(resource.RLIMIT_NOFILE)[0]
            if new_limit < soft_limit:
                typer.secho(f"Failed to increase ulimit to {soft_limit}, please set manually. Current limit is {new_limit}", fg="red")
                typer.Abort()
            else:
                typer.secho(f"Successfully increased ulimit to {new_limit}", fg="green")


def deprovision_skylark_instances(azure_subscription: Optional[str] = None, gcp_project_id: Optional[str] = None):
    instances = []

    aws = AWSCloudProvider()
    for _, instance_list in do_parallel(
        aws.get_matching_instances, aws.region_list(), progress_bar=True, leave_pbar=False, desc="Retrieve AWS instances"
    ):
        instances += instance_list

    if not azure_subscription:
        typer.secho(
            "No Microsoft Azure subscription given, so Azure instances will not be terminated", color=typer.colors.YELLOW, bold=True
        )
    else:
        azure = AzureCloudProvider(azure_subscription=azure_subscription)
        instances += azure.get_matching_instances()

    if not gcp_project_id:
        typer.secho("No GCP project ID given, so GCP instances will not be deprovisioned", color=typer.colors.YELLOW, bold=True)
    else:
        gcp = GCPCloudProvider(gcp_project=gcp_project_id)
        instances += gcp.get_matching_instances()

    if instances:
        typer.secho(f"Deprovisioning {len(instances)} instances", color=typer.colors.YELLOW, bold=True)
        do_parallel(lambda instance: instance.terminate_instance(), instances, progress_bar=True, desc="Deprovisioning")
    else:
        typer.secho("No instances to deprovision, exiting...", color=typer.colors.YELLOW, bold=True)


def load_config():
    if config_file.exists():
        try:
            with config_file.open("r") as f:
                config = json.load(f)
            if "aws_access_key_id" in config:
                os.environ["AWS_ACCESS_KEY_ID"] = config["aws_access_key_id"]
            if "aws_secret_access_key" in config:
                os.environ["AWS_SECRET_ACCESS_KEY"] = config["aws_secret_access_key"]
            if "azure_tenant_id" in config:
                os.environ["AZURE_TENANT_ID"] = config["azure_tenant_id"]
            if "azure_client_id" in config:
                os.environ["AZURE_CLIENT_ID"] = config["azure_client_id"]
            if "azure_client_secret" in config:
                os.environ["AZURE_CLIENT_SECRET"] = config["azure_client_secret"]
            if "gcp_application_credentials_file" in config:
                os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = config["gcp_application_credentials_file"]

            return config
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding config file: {e}")
            raise typer.Abort()
    return {}
