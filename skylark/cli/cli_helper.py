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
from skylark.utils import logger
from skylark.compute.aws.aws_cloud_provider import AWSCloudProvider
from skylark.compute.azure.azure_cloud_provider import AzureCloudProvider
from skylark.compute.gcp.gcp_cloud_provider import GCPCloudProvider
from skylark.obj_store.object_store_interface import ObjectStoreObject
from skylark.obj_store.s3_interface import S3Interface
from skylark.obj_store.gcs_interface import GCSInterface
from skylark.obj_store.azure_interface import AzureInterface
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
        return "azure", account, container
    elif path.startswith("azure://"):
        bucket_name = path[8:]
        region = path[8:].split("-", 2)[-1]
        return "azure", bucket_name, region
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


def copy_local_objstore(object_interface: ObjectStoreObject, src: Path, dst_bucket: str, dst_key: str):
    ops: List[concurrent.futures.Future] = []
    path_mapping: Dict[concurrent.futures.Future, Path] = {}

    def _copy(path: Path, dst_key: str, total_size=0.0):
        if path.is_dir():
            for child in path.iterdir():
                total_size += _copy(child, os.path.join(dst_key, child.name))
            return total_size
        else:
            future = object_interface.upload_object(path, dst_key)
            ops.append(future)
            path_mapping[future] = path
            return path.stat().st_size

    total_bytes = _copy(src, dst_key)

    # wait for all uploads to complete, displaying a progress bar
    with tqdm(total=total_bytes, unit="B", unit_scale=True, unit_divisor=1024, desc="Uploading") as pbar:
        for op in concurrent.futures.as_completed(ops):
            op.result()
            pbar.update(path_mapping[op].stat().st_size)


def copy_objstore_local(object_interface: ObjectStoreObject, src_bucket: str, src_key: str, dst: Path):
    ops: List[concurrent.futures.Future] = []
    obj_mapping: Dict[concurrent.futures.Future, ObjectStoreObject] = {}

    # copy single object
    def _copy(src_obj: ObjectStoreObject, dst: Path):
        dst.parent.mkdir(exist_ok=True, parents=True)
        future = object_interface.download_object(src_obj.key, dst)
        ops.append(future)
        obj_mapping[future] = src_obj
        return src_obj.size

    total_bytes = 0.0
    for obj in object_interface.list_objects(prefix=src_key):
        sub_key = obj.key[len(src_key) :]
        sub_key = sub_key.lstrip("/")
        dest_path = dst / sub_key
        total_bytes += _copy(obj, dest_path)

    # wait for all downloads to complete, displaying a progress bar
    with tqdm(total=total_bytes, unit="B", unit_scale=True, unit_divisor=1024, desc="Downloading") as pbar:
        for op in concurrent.futures.as_completed(ops):
            op.result()
            pbar.update(obj_mapping[op].size)


def copy_local_gcs(src: Path, dst_bucket: str, dst_key: str):
    gcs = GCSInterface(None, dst_bucket)
    return copy_local_objstore(gcs, src, dst_bucket, dst_key)


def copy_gcs_local(src_bucket: str, src_key: str, dst: Path):
    gcs = GCSInterface(None, src_bucket)
    return copy_objstore_local(gcs, src_bucket, src_key, dst)


def copy_local_azure(src: Path, dst_bucket: str, dst_key: str):
    # Note that dst_key is infact azure region
    azure = AzureInterface(dst_key, dst_bucket)
    return copy_local_objstore(azure, src, dst_bucket, dst_key)


def copy_azure_local(src_bucket: str, src_key: str, dst: Path):
    # Note that src_key is infact azure region
    azure = AzureInterface(src_key, src_bucket)
    return copy_objstore_local(azure, src_bucket, src_key, dst)


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


def check_ulimit(hard_limit=1024 * 1024, soft_limit=1024 * 1024):
    current_limit_soft, current_limit_hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    if current_limit_hard < hard_limit:
        typer.secho(
            f"Warning: hard file limit is set to {current_limit_hard}, which is less than the recommended minimum of {hard_limit}", fg="red"
        )
        increase_hard_limit = ["sudo", "sysctl", "-w", f"fs.file-max={hard_limit}"]
        typer.secho(f"Will run the following commands:")
        typer.secho(f"    {' '.join(increase_hard_limit)}", fg="yellow")
        if typer.confirm("sudo required; Do you want to increase the limit?", default=True):
            subprocess.check_output(increase_hard_limit)
            new_limit = resource.getrlimit(resource.RLIMIT_NOFILE)[0]
            if new_limit < soft_limit:
                typer.secho(
                    f"Failed to increase ulimit to {soft_limit}, please set manually with 'ulimit -n {soft_limit}'. Current limit is {new_limit}",
                    fg="red",
                )
                typer.Abort()
            else:
                typer.secho(f"Successfully increased ulimit to {new_limit}", fg="green")
    if current_limit_soft < soft_limit:
        increase_soft_limit = ["sudo", "prlimit", "--pid", str(os.getpid()), f"--nofile={soft_limit}:{hard_limit}"]
        logger.warning(
            f"Warning: soft file limit is set to {current_limit_soft}, increasing for process with `{' '.join(increase_soft_limit)}`"
        )
        subprocess.check_output(increase_soft_limit)


def deprovision_skylark_instances(azure_subscription: Optional[str] = None, gcp_project_id: Optional[str] = None):
    instances = []

    aws = AWSCloudProvider()
    for _, instance_list in do_parallel(
        aws.get_matching_instances, aws.region_list(), progress_bar=True, leave_pbar=False, desc="Retrieve AWS instances"
    ):
        instances += instance_list

    if True or not azure_subscription:
        typer.secho(
            "No Microsoft Azure subscription given, so Azure instances will not be terminated", color=typer.colors.YELLOW, bold=True
        )
    else:
        azure = AzureCloudProvider(azure_subscription=azure_subscription)
        instances += azure.get_matching_instances()

    if True or not gcp_project_id:
        typer.secho("No GCP project ID given, so GCP instances will not be deprovisioned", color=typer.colors.YELLOW, bold=True)
    else:
        gcp = GCPCloudProvider(gcp_project=gcp_project_id)
        instances += gcp.get_matching_instances()

    if instances:
        typer.secho(f"Deprovisioning {len(instances)} instances", color=typer.colors.YELLOW, bold=True)
        do_parallel(lambda instance: instance.terminate_instance(), instances, progress_bar=True, desc="Deprovisioning")
    else:
        typer.secho("No instances to deprovision, exiting...", color=typer.colors.YELLOW, bold=True)
