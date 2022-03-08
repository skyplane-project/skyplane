import concurrent.futures
import os
import re
import resource
import subprocess
from pathlib import Path
from shutil import copyfile
from typing import Dict, List, Optional

import boto3
import typer
from skylark.config import SkylarkConfig
from skylark.utils import logger
from skylark.compute.aws.aws_cloud_provider import AWSCloudProvider
from skylark.compute.azure.azure_cloud_provider import AzureCloudProvider
from skylark.compute.gcp.gcp_cloud_provider import GCPCloudProvider
from skylark.obj_store.object_store_interface import ObjectStoreInterface, ObjectStoreObject
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


def copy_local_objstore(object_interface: ObjectStoreInterface, src: Path, dst_bucket: str, dst_key: str):
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


def copy_objstore_local(object_interface: ObjectStoreInterface, src_bucket: str, src_key: str, dst: Path):
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


def deprovision_skylark_instances():
    config = SkylarkConfig.load()
    instances = []

    aws = AWSCloudProvider()
    for _, instance_list in do_parallel(
        aws.get_matching_instances, aws.region_list(), progress_bar=True, leave_pbar=False, desc="Retrieve AWS instances"
    ):
        instances += instance_list

    if config.azure_enabled:
        azure = AzureCloudProvider()
        instances += azure.get_matching_instances()

    if config.gcp_enabled:
        gcp = GCPCloudProvider()
        instances += gcp.get_matching_instances()

    if instances:
        typer.secho(f"Deprovisioning {len(instances)} instances", fg="yellow", bold=True)
        do_parallel(lambda instance: instance.terminate_instance(), instances, progress_bar=True, desc="Deprovisioning")
    else:
        typer.secho("No instances to deprovision, exiting...", fg="yellow", bold=True)


def load_aws_config(config: SkylarkConfig, force_init: bool = False) -> SkylarkConfig:
    if force_init:
        typer.secho("    AWS credentials will be re-initialized", fg="red")
        config.aws_enabled = False
        config.aws_access_key_id = None
        config.aws_secret_access_key = None

    if config.aws_enabled and config.aws_access_key_id is not None and config.aws_secret_access_key is not None:
        typer.secho("    AWS credentials already configured! To reconfigure AWS, run `skylark init --reinit-aws`.", fg="blue")
        return config

    # get AWS credentials from boto3
    session = boto3.Session()
    credentials = session.get_credentials()
    credentials = credentials.get_frozen_credentials()
    if credentials.access_key is None or credentials.secret_key is None:
        typer.secho("    AWS credentials not found in boto3 session, please use the AWS CLI to set them via `aws configure`", fg="red")
        typer.secho("    https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html", fg="red")
        typer.secho("    Disabling AWS support", fg="blue")
        config.aws_enabled = False
        config.aws_access_key_id = None
        config.aws_secret_access_key = None
        return config

    typer.secho(f"    Loaded AWS credentials from the AWS CLI [IAM access key ID: ...{credentials.access_key[-6:]}]", fg="blue")
    config.aws_enabled = True
    config.aws_access_key_id = credentials.access_key
    config.aws_secret_access_key = credentials.secret_key
    return config


def load_azure_config(config: SkylarkConfig, force_init: bool = False) -> SkylarkConfig:
    if force_init:
        typer.secho("    Azure credentials will be re-initialized", fg="red")
        config.azure_enabled = False
        config.azure_tenant_id = None
        config.azure_client_id = None
        config.azure_client_secret = None
        config.azure_subscription_id = None

    if (
        config.azure_enabled
        and config.azure_tenant_id is not None
        and config.azure_client_id is not None
        and config.azure_client_secret is not None
        and not force_init
    ):
        typer.secho("    Azure credentials already configured! To reconfigure Azure, run `skylark init --reinit-azure`.", fg="blue")
        return config

    # get Azure credentials from Azure default credential provider
    azure_tenant_id = os.environ.get("AZURE_TENANT_ID", config.azure_tenant_id)
    azure_client_id = os.environ.get("AZURE_CLIENT_ID", config.azure_client_id)
    azure_client_secret = os.environ.get("AZURE_CLIENT_SECRET", config.azure_client_secret)
    azure_subscription_id = os.environ.get("AZURE_SUBSCRIPTION_ID", config.azure_subscription_id)

    # prompt for missing credentials
    if not azure_tenant_id or not azure_client_id or not azure_client_secret or not azure_subscription_id:
        typer.secho(
            "    Azure credentials not found in environment variables, please use the Azure CLI to set them via `az login`", fg="red"
        )
        typer.secho("    Azure config can be generated using: az ad sp create-for-rbac -n api://skylark --sdk-auth", fg="red")
        if not typer.confirm("    Do you want to manually enter your service principal keys?", default=False):
            typer.secho("    Disabling Azure support in Skylark", fg="blue")
            config.azure_enabled = False
            config.azure_tenant_id = None
            config.azure_client_id = None
            config.azure_client_secret = None
            return config

        if not azure_tenant_id:
            azure_tenant_id = typer.prompt("    Azure tenant ID")
        if not azure_client_id:
            azure_client_id = typer.prompt("    Azure client ID")
        if not azure_client_secret:
            azure_client_secret = typer.prompt("    Azure client secret")
        if not azure_subscription_id:
            azure_subscription_id = typer.prompt("    Azure subscription ID")

    config.azure_enabled = True
    config.azure_tenant_id = azure_tenant_id
    config.azure_client_id = azure_client_id
    config.azure_client_secret = azure_client_secret
    config.azure_subscription_id = azure_subscription_id
    return config


def load_gcp_config(config: SkylarkConfig, force_init: bool = False) -> SkylarkConfig:
    if force_init:
        typer.secho("    GCP credentials will be re-initialized", fg="red")
        config.gcp_enabled = False
        config.gcp_application_credentials_file = None
        config.gcp_project_id = None

    if config.gcp_enabled and config.gcp_project_id is not None and config.gcp_application_credentials_file is not None:
        typer.secho("    GCP already configured! To reconfigure GCP, run `skylark init --reinit-gcp`.", fg="blue")
        return config

    # load from environment variables
    gcp_application_credentials_file = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", config.gcp_application_credentials_file)
    if not gcp_application_credentials_file:
        typer.secho(
            "    GCP credentials not found in environment variables, please use the GCP CLI to set them via `gcloud auth application-default login`",
            fg="red",
        )
        typer.secho("    https://cloud.google.com/docs/authentication/getting-started", fg="red")
        if not typer.confirm("    Do you want to manually enter your service account key?", default=False):
            typer.secho("    Disabling GCP support in Skylark", fg="blue")
            config.gcp_enabled = False
            config.gcp_project_id = None
            config.gcp_application_credentials_file = None
            return config
        gcp_application_credentials_file = typer.prompt("    GCP application credentials file path")

    # check if the file exists
    gcp_application_credentials_file = Path(gcp_application_credentials_file).expanduser().resolve()
    if not gcp_application_credentials_file.exists():
        typer.secho(f"    GCP application credentials file not found at {gcp_application_credentials_file}", fg="red")
        typer.secho("    Disabling GCP support in Skylark", fg="blue")
        config.gcp_enabled = False
        config.gcp_project_id = None
        config.gcp_application_credentials_file = None
        return config

    config.gcp_enabled = True
    config.gcp_application_credentials_file = str(gcp_application_credentials_file)
    project_id = os.environ.get("GOOGLE_CLOUD_PROJECT", config.gcp_project_id)
    if not project_id:
        project_id = typer.prompt("    GCP project ID")
    config.gcp_project_id = project_id
    return config
