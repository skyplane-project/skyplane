import concurrent.futures
from functools import partial
import atexit
import json
import os
import re
import logging
import resource
import subprocess
from pathlib import Path
from shutil import copyfile
from typing import Dict, List
from sys import platform
from typing import Dict, List
from urllib.parse import ParseResultBytes, parse_qs


import boto3
import typer
from skylark import config_path, GB, MB, print_header
from skylark.compute.aws.aws_auth import AWSAuthentication
from skylark.compute.azure.azure_auth import AzureAuthentication
from skylark.compute.gcp.gcp_auth import GCPAuthentication
from skylark.config import SkylarkConfig
from skylark.utils import logger
from skylark.compute.aws.aws_cloud_provider import AWSCloudProvider
from skylark.compute.azure.azure_cloud_provider import AzureCloudProvider
from skylark.compute.gcp.gcp_cloud_provider import GCPCloudProvider
from skylark.obj_store.object_store_interface import ObjectStoreInterface, ObjectStoreObject
from skylark.obj_store.s3_interface import S3Interface
from skylark.obj_store.gcs_interface import GCSInterface
from skylark.obj_store.azure_interface import AzureInterface
from skylark.obj_store.object_store_interface import ObjectStoreInterface
from skylark.replicate.replication_plan import ReplicationJob, ReplicationTopology
from skylark.replicate.replicator_client import ReplicatorClient
from skylark.utils.utils import do_parallel
from typing import Optional
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
        return "azure", (account, container), blob_path
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


def copy_local_objstore(object_interface: ObjectStoreInterface, src: Path, dst_key: str):
    with concurrent.futures.ThreadPoolExecutor(max_workers=64) as executor:
        ops: List[concurrent.futures.Future] = []
        path_mapping: Dict[concurrent.futures.Future, Path] = {}

        def _copy(path: Path, dst_key: str, total_size=0.0):
            if path.is_dir():
                for child in path.iterdir():
                    total_size += _copy(child, os.path.join(dst_key, child.name))
                return total_size
            else:
                future = executor.submit(object_interface.upload_object, path, dst_key)
                ops.append(future)
                path_mapping[future] = path
                return path.stat().st_size

        total_bytes = _copy(src, dst_key)

        # wait for all uploads to complete, displaying a progress bar
        with tqdm(total=total_bytes, unit="B", unit_scale=True, unit_divisor=1024, desc="Uploading") as pbar:
            for op in concurrent.futures.as_completed(ops):
                op.result()
                pbar.update(path_mapping[op].stat().st_size)


def copy_objstore_local(object_interface: ObjectStoreInterface, src_key: str, dst: Path):
    with concurrent.futures.ThreadPoolExecutor(max_workers=64) as executor:
        ops: List[concurrent.futures.Future] = []
        obj_mapping: Dict[concurrent.futures.Future, ObjectStoreObject] = {}

        # copy single object
        def _copy(src_obj: ObjectStoreObject, dst: Path):
            dst.parent.mkdir(exist_ok=True, parents=True)
            future = executor.submit(object_interface.download_object, src_obj.key, dst)
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
    return copy_local_objstore(gcs, src, dst_key)


def copy_gcs_local(src_bucket: str, src_key: str, dst: Path):
    gcs = GCSInterface(None, src_bucket)
    return copy_objstore_local(gcs, src_key, dst)


def copy_local_azure(src: Path, dst_account_name: str, dst_container_name: str, dst_key: str):
    azure = AzureInterface(None, dst_account_name, dst_container_name)
    return copy_local_objstore(azure, src, dst_key)


def copy_azure_local(src_account_name: str, src_container_name: str, src_key: str, dst: Path):
    azure = AzureInterface(None, src_account_name, src_container_name)
    return copy_objstore_local(azure, src_key, dst)


def copy_local_s3(src: Path, dst_bucket: str, dst_key: str, use_tls: bool = True):
    s3 = S3Interface(None, dst_bucket, use_tls=use_tls)
    return copy_local_objstore(s3, src, dst_key)


def copy_s3_local(src_bucket: str, src_key: str, dst: Path):
    s3 = S3Interface(None, src_bucket)
    return copy_objstore_local(s3, src_key, dst)


def replicate_helper(
    topo: ReplicationTopology,
    size_total_mb: int = 2048,
    n_chunks: int = 512,
    random: bool = False,
    # bucket options
    source_bucket: str = typer.Option(None),
    dest_bucket: str = typer.Option(None),
    src_key_prefix: str = "/",
    dest_key_prefix: str = "/",
    # gateway provisioning options
    reuse_gateways: bool = False,
    gateway_docker_image: str = os.environ.get("SKYLARK_DOCKER_IMAGE", "ghcr.io/parasj/skylark:main"),
    # cloud provider specific options
    aws_instance_class: str = "m5.8xlarge",
    azure_instance_class: str = "Standard_D32_v4",
    gcp_instance_class: Optional[str] = "n2-standard-32",
    gcp_use_premium_network: bool = True,
    # logging options
    time_limit_seconds: Optional[int] = None,
    log_interval_s: float = 1.0,
):
    if reuse_gateways:
        logger.warning(
            f"Instances will remain up and may result in continued cloud billing. Remember to call `skylark deprovision` to deprovision gateways."
        )

    if random:
        chunk_size_mb = size_total_mb // n_chunks
        job = ReplicationJob(
            source_region=topo.source_region(),
            source_bucket=None,
            dest_region=topo.sink_region(),
            dest_bucket=None,
            src_objs=[f"/{i}" for i in range(n_chunks)],
            dest_objs=[f"/{i}" for i in range(n_chunks)],
            random_chunk_size_mb=chunk_size_mb,
        )
    else:
        # make replication job
        objs = list(ObjectStoreInterface.create(topo.source_region(), source_bucket).list_objects(src_key_prefix))
        job = ReplicationJob(
            source_region=topo.source_region(),
            source_bucket=source_bucket,
            dest_region=topo.sink_region(),
            dest_bucket=dest_bucket,
            src_objs=[obj.key for obj in objs],
            dest_objs=[dest_key_prefix + obj.key for obj in objs],
            obj_sizes={obj.key: obj.size for obj in objs},
        )

    rc = ReplicatorClient(
        topo,
        gateway_docker_image=gateway_docker_image,
        aws_instance_class=aws_instance_class,
        azure_instance_class=azure_instance_class,
        gcp_instance_class=gcp_instance_class,
        gcp_use_premium_network=gcp_use_premium_network,
    )
    try:
        rc.provision_gateways(reuse_gateways)
        for node, gw in rc.bound_nodes.items():
            logger.info(f"Provisioned {node}: {gw.gateway_log_viewer_url}")
        job = rc.run_replication_plan(job)
        if random:
            total_bytes = n_chunks * chunk_size_mb * MB
        else:
            total_bytes = sum([chunk_req.chunk.chunk_length_bytes for chunk_req in job.chunk_requests])
        logger.info(f"{total_bytes / GB:.2f}GByte replication job launched")
        stats = rc.monitor_transfer(job, show_pbar=True, log_interval_s=log_interval_s, time_limit_seconds=time_limit_seconds)
    except KeyboardInterrupt:
        if not reuse_gateways:
            logger.warning("Deprovisioning gateways then exiting...")
            rc.deprovision_gateways()
        os._exit(1)  # exit now
    if not reuse_gateways:
        rc.deprovision_gateways()
    stats = stats if stats else {}
    stats["success"] = stats["monitor_status"] == "completed"
    out_json = {k: v for k, v in stats.items() if k not in ["log", "completed_chunk_ids"]}
    typer.echo(f"\n{json.dumps(out_json)}")
    return 0 if stats["success"] else 1


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
                raise typer.Abort()
            else:
                typer.secho(f"Successfully increased ulimit to {new_limit}", fg="green")
    if current_limit_soft < soft_limit and (platform == "linux" or platform == "linux2"):
        increase_soft_limit = ["sudo", "prlimit", "--pid", str(os.getpid()), f"--nofile={soft_limit}:{hard_limit}"]
        logger.warning(
            f"Warning: soft file limit is set to {current_limit_soft}, increasing for process with `{' '.join(increase_soft_limit)}`"
        )
        subprocess.check_output(increase_soft_limit)


def deprovision_skylark_instances():
    instances = []
    query_jobs = []

    # TODO remove when skylark init explicitly configures regions
    def catch_error(fn):
        def run():
            try:
                return fn()
            except Exception as e:
                logger.error(f"Error encountered during deprovision: {e}")
                return []

        return run

    if AWSAuthentication().enabled():
        logger.debug("AWS authentication enabled, querying for instances")
        aws = AWSCloudProvider()
        for region in aws.region_list():
            query_jobs.append(catch_error(partial(aws.get_matching_instances, region)))
    if AzureAuthentication().enabled():
        logger.debug("Azure authentication enabled, querying for instances")
        query_jobs.append(catch_error(lambda: AzureCloudProvider().get_matching_instances()))
    if GCPAuthentication().enabled():
        logger.debug("GCP authentication enabled, querying for instances")
        query_jobs.append(catch_error(lambda: GCPCloudProvider().get_matching_instances()))

    # query in parallel
    for _, instance_list in do_parallel(lambda f: f(), query_jobs, progress_bar=True, desc="Query instances", hide_args=True, n=-1):
        instances.extend(instance_list)

    if instances:
        typer.secho(f"Deprovisioning {len(instances)} instances", fg="yellow", bold=True)
        do_parallel(lambda instance: instance.terminate_instance(), instances, progress_bar=True, desc="Deprovisioning")
    else:
        typer.secho("No instances to deprovision, exiting...", fg="yellow", bold=True)


def load_aws_config(config: SkylarkConfig) -> SkylarkConfig:
    # get AWS credentials from boto3
    session = boto3.Session()
    credentials = session.get_credentials()
    credentials = credentials.get_frozen_credentials()
    if credentials.access_key is None or credentials.secret_key is None:
        typer.secho("    AWS credentials not found in boto3 session, please use the AWS CLI to set them via `aws configure`", fg="red")
        typer.secho("    https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html", fg="red")
        typer.secho("    Disabling AWS support", fg="blue")
        return config

    typer.secho(f"    Loaded AWS credentials from the AWS CLI [IAM access key ID: ...{credentials.access_key[-6:]}]", fg="blue")
    return config


def load_azure_config(config: SkylarkConfig, force_init: bool = False) -> SkylarkConfig:
    if force_init:
        typer.secho("    Azure credentials will be re-initialized", fg="red")
        config.azure_subscription_id = None

    if config.azure_subscription_id:
        typer.secho("    Azure credentials already configured! To reconfigure Azure, run `skylark init --reinit-azure`.", fg="blue")
        return config

    # check if Azure is enabled
    logging.disable(logging.WARNING)  # disable Azure logging, we have our own
    auth = AzureAuthentication()
    try:
        auth.credential.get_token("https://management.azure.com/")
        azure_enabled = True
    except:
        azure_enabled = False
    logging.disable(logging.NOTSET)  # reenable logging
    if not azure_enabled:
        typer.secho("    No local Azure credentials! Run `az login` to set them up.", fg="red")
        typer.secho("    https://docs.microsoft.com/en-us/azure/developer/python/azure-sdk-authenticate", fg="red")
        typer.secho("    Disabling Azure support", fg="blue")
        return config
    typer.secho("    Azure credentials found in Azure CLI", fg="blue")
    inferred_subscription_id = AzureAuthentication.infer_subscription_id()
    if typer.confirm("    Azure credentials found, do you want to enable Azure support in Skylark?", default=True):
        config.azure_subscription_id = typer.prompt("    Enter the Azure subscription ID:", default=inferred_subscription_id)
    else:
        config.azure_subscription_id = None
        typer.secho("    Disabling Azure support", fg="blue")
    return config


def load_gcp_config(config: SkylarkConfig, force_init: bool = False) -> SkylarkConfig:
    if force_init:
        typer.secho("    GCP credentials will be re-initialized", fg="red")
        config.gcp_project_id = None

    if config.gcp_project_id is not None:
        typer.secho("    GCP already configured! To reconfigure GCP, run `skylark init --reinit-gcp`.", fg="blue")
        return config

    # check if GCP is enabled
    auth = GCPAuthentication()
    if not auth.credentials:
        typer.secho(
            "    Default GCP credentials are not set up yet. Run `gcloud auth application-default login`.",
            fg="red",
        )
        typer.secho("    https://cloud.google.com/docs/authentication/getting-started", fg="red")
        typer.secho("    Disabling GCP support", fg="blue")
        return config
    else:
        typer.secho("    GCP credentials found in GCP CLI", fg="blue")
        if typer.confirm("    GCP credentials found, do you want to enable GCP support in Skylark?", default=True):
            config.gcp_project_id = typer.prompt("    Enter the GCP project ID:", default=auth.project_id)
            assert config.gcp_project_id is not None, "GCP project ID must not be None"
            return config
        else:
            config.gcp_project_id = None
            typer.secho("    Disabling GCP support", fg="blue")
            return config
