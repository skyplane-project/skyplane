import concurrent.futures
import json
import logging
import os
import re
import resource
import signal
import subprocess
from functools import partial
from pathlib import Path
from shutil import copyfile
from sys import platform
from typing import Dict, List, Optional

import boto3
import typer
from tqdm import tqdm

from skylark import GB, MB, exceptions, gcp_config_path
from skylark.compute.aws.aws_auth import AWSAuthentication
from skylark.compute.aws.aws_cloud_provider import AWSCloudProvider
from skylark.compute.azure.azure_auth import AzureAuthentication
from skylark.compute.azure.azure_cloud_provider import AzureCloudProvider
from skylark.compute.gcp.gcp_auth import GCPAuthentication
from skylark.compute.gcp.gcp_cloud_provider import GCPCloudProvider
from skylark.config import SkylarkConfig
from skylark.obj_store.azure_interface import AzureInterface
from skylark.obj_store.gcs_interface import GCSInterface
from skylark.obj_store.object_store_interface import ObjectStoreInterface, ObjectStoreObject
from skylark.obj_store.s3_interface import S3Interface
from skylark.replicate.replication_plan import ReplicationJob, ReplicationTopology
from skylark.replicate.replicator_client import ReplicatorClient
from skylark.utils import logger
from skylark.utils.utils import do_parallel


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
    if path.startswith("s3://") or path.startswith("gs://"):
        provider, parsed = path[:2], path[5:]
        if len(parsed) == 0:
            typer.secho(f"Invalid path: '{path}'", fg="red")
            raise typer.Exit(code=1)
        bucket, *keys = parsed.split("/", 1)
        key = keys[0] if len(keys) > 0 else ""
        return provider, bucket, key
    elif (path.startswith("https://") or path.startswith("http://")) and "blob.core.windows.net" in path:
        # Azure blob storage
        regex = re.compile(r"https?://([^/]+).blob.core.windows.net/([^/]+)/?(.*)")
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

    raise ValueError(f"Parse error {path}")


# skylark ls implementation
def ls_local(path: Path):
    if not path.exists():
        raise FileNotFoundError(path)
    if path.is_dir():
        for child in path.iterdir():
            yield child.name
    else:
        yield path.name


def ls_objstore(obj_store: str, bucket_name: str, key_name: str):
    client = ObjectStoreInterface.create(obj_store, bucket_name)
    for obj in client.list_objects(prefix=key_name):
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

        obj_count = 0
        total_bytes = 0.0
        for obj in object_interface.list_objects(prefix=src_key):
            sub_key = obj.key[len(src_key) :]
            sub_key = sub_key.lstrip("/")
            dest_path = dst / sub_key
            total_bytes += _copy(obj, dest_path)
            obj_count += 1

        if not obj_count:
            logger.error("Specified object does not exist.")
            raise exceptions.MissingObjectException()

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


def copy_local_s3(src: Path, dst_bucket: str, dst_key: str):
    s3 = S3Interface(None, dst_bucket)
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
    source_bucket: Optional[str] = None,
    dest_bucket: Optional[str] = None,
    src_key_prefix: str = "",
    dest_key_prefix: str = "",
    # maximum chunk size to breakup objects into
    max_chunk_size_mb: Optional[int] = None,
    # gateway provisioning options
    reuse_gateways: bool = False,
    gateway_docker_image: str = os.environ.get("SKYLARK_DOCKER_IMAGE", "ghcr.io/skyplane-project/skyplane:main"),
    use_bbr: bool = False,
    # cloud provider specific options
    aws_instance_class: str = "m5.8xlarge",
    azure_instance_class: str = "Standard_D32_v4",
    gcp_instance_class: Optional[str] = "n2-standard-32",
    gcp_use_premium_network: bool = True,
    # logging options
    time_limit_seconds: Optional[int] = None,
    log_interval_s: float = 1.0,
):
    if "SKYLARK_DOCKER_IMAGE" in os.environ:
        logger.debug(f"Using docker image: {gateway_docker_image}")
    if reuse_gateways:
        typer.secho(
            f"Instances will remain up and may result in continued cloud billing. Remember to call `skylark deprovision` to deprovision gateways.",
            fg="red",
            bold=True,
        )
    if random:
        random_chunk_size_mb = size_total_mb // n_chunks
        if max_chunk_size_mb:
            logger.error("Cannot set chunk size for random data replication, set `random_chunk_size_mb` instead")
            raise ValueError("Cannot set max chunk size")
        job = ReplicationJob(
            source_region=topo.source_region(),
            source_bucket=None,
            dest_region=topo.sink_region(),
            dest_bucket=None,
            src_objs=[str(i) for i in range(n_chunks)],
            dest_objs=[str(i) for i in range(n_chunks)],
            random_chunk_size_mb=random_chunk_size_mb,
        )
    else:
        # make replication job
        src_objs = list(ObjectStoreInterface.create(topo.source_region(), source_bucket).list_objects(src_key_prefix))
        if not src_objs:
            logger.error("Specified object does not exist.")
            raise exceptions.MissingObjectException()

        if dest_key_prefix.endswith("/"):
            dest_is_directory = True

        # map objects to destination object paths
        # todo isolate this logic and test independently
        src_objs_job = []
        dest_objs_job = []
        # if only one object exists, replicate it
        if len(src_objs) == 1 and src_objs[0].key == src_key_prefix:
            src_objs_job.append(src_objs[0].key)
            if dest_key_prefix.endswith("/"):
                dest_objs_job.append(dest_key_prefix + src_objs[0].key.split("/")[-1])
            else:
                dest_objs_job.append(dest_key_prefix)
        # multiple objects to replicate
        else:
            for src_obj in src_objs:
                src_objs_job.append(src_obj.key)
                # remove prefix from object key
                src_path_no_prefix = src_obj.key[len(src_key_prefix) :] if src_obj.key.startswith(src_key_prefix) else src_obj.key
                # remove single leading slash if present
                src_path_no_prefix = src_path_no_prefix[1:] if src_path_no_prefix.startswith("/") else src_path_no_prefix
                if len(dest_key_prefix) == 0:
                    dest_objs_job.append(src_path_no_prefix)
                elif dest_key_prefix.endswith("/"):
                    dest_objs_job.append(dest_key_prefix + src_path_no_prefix)
                else:
                    dest_objs_job.append(dest_key_prefix + "/" + src_path_no_prefix)
        job = ReplicationJob(
            source_region=topo.source_region(),
            source_bucket=source_bucket,
            dest_region=topo.sink_region(),
            dest_bucket=dest_bucket,
            src_objs=src_objs_job,
            dest_objs=dest_objs_job,
            obj_sizes={obj.key: obj.size for obj in src_objs},
            max_chunk_size_mb=max_chunk_size_mb,
        )

    rc = ReplicatorClient(
        topo,
        gateway_docker_image=gateway_docker_image,
        aws_instance_class=aws_instance_class,
        azure_instance_class=azure_instance_class,
        gcp_instance_class=gcp_instance_class,
        gcp_use_premium_network=gcp_use_premium_network,
    )
    typer.secho(f"Storing debug information for transfer in {rc.transfer_dir / 'client.log'}", fg="yellow")
    (rc.transfer_dir / "topology.json").write_text(topo.to_json())

    stats = {}
    try:
        rc.provision_gateways(reuse_gateways, use_bbr=use_bbr)
        for node, gw in rc.bound_nodes.items():
            logger.fs.info(f"Log URLs for {gw.uuid()} ({node.region}:{node.instance})")
            logger.fs.info(f"\tLog viewer: {gw.gateway_log_viewer_url}")
            logger.fs.info(f"\tAPI: {gw.gateway_api_url}")
        job = rc.run_replication_plan(job)
        if random:
            total_bytes = n_chunks * random_chunk_size_mb * MB
        else:
            total_bytes = sum([chunk_req.chunk.chunk_length_bytes for chunk_req in job.chunk_requests])
        typer.secho(f"{total_bytes / GB:.2f}GByte replication job launched", fg="green")
        stats = rc.monitor_transfer(
            job,
            show_spinner=True,
            log_interval_s=log_interval_s,
            time_limit_seconds=time_limit_seconds,
            multipart=max_chunk_size_mb is not None,
        )
    except KeyboardInterrupt:
        if not reuse_gateways:
            logger.fs.warning("Deprovisioning gateways then exiting...")
            # disable sigint to prevent repeated KeyboardInterrupts
            s = signal.signal(signal.SIGINT, signal.SIG_IGN)
            rc.deprovision_gateways()
            signal.signal(signal.SIGINT, s)

        stats["success"] = False
        out_json = {k: v for k, v in stats.items() if k not in ["log", "completed_chunk_ids"]}
        typer.echo(f"\n{json.dumps(out_json)}")
        os._exit(1)  # exit now
    if not reuse_gateways:
        s = signal.signal(signal.SIGINT, signal.SIG_IGN)
        rc.deprovision_gateways()
        signal.signal(signal.SIGINT, s)
    stats = stats if stats else {}
    stats["success"] = stats["monitor_status"] == "completed"

    if stats["monitor_status"] == "error":
        for instance, errors in stats["errors"].items():
            for error in errors:
                typer.secho(f"\n❌ {instance} encountered error:", fg="red", bold=True)
                typer.secho(error, fg="red")
        raise typer.Exit(1)

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
        aws = AWSCloudProvider()
        for region in aws.region_list():
            query_jobs.append(catch_error(partial(aws.get_matching_instances, region)))
    if AzureAuthentication().enabled():
        query_jobs.append(catch_error(lambda: AzureCloudProvider().get_matching_instances()))
    if GCPAuthentication().enabled():
        query_jobs.append(catch_error(lambda: GCPCloudProvider().get_matching_instances()))

    # query in parallel
    for instance_list in do_parallel(
        lambda f: f(), query_jobs, n=-1, return_args=False, spinner=True, desc="Querying clouds for instances"
    ):
        instances.extend(instance_list)

    if instances:
        typer.secho(f"Deprovisioning {len(instances)} instances", fg="yellow", bold=True)
        do_parallel(lambda instance: instance.terminate_instance(), instances, desc="Deprovisioning", spinner=True, spinner_persist=True)
    else:
        typer.secho("No instances to deprovision", fg="yellow", bold=True)

    if AWSAuthentication().enabled():
        aws = AWSCloudProvider()
        # remove skylark vpc
        vpcs = do_parallel(partial(aws.get_vpcs), aws.region_list(), desc="Querying VPCs", spinner=True)
        args = [(x[0], vpc.id) for x in vpcs for vpc in x[1]]
        do_parallel(lambda args: aws.remove_sg_ips(*args), args, desc="Removing IPs from VPCs", spinner=True, spinner_persist=True)
        # remove all instance profiles
        profiles = aws.list_instance_profiles(prefix="skylark-aws")
        if profiles:
            do_parallel(aws.delete_instance_profile, profiles, desc="Deleting instance profiles", spinner=True, spinner_persist=True, n=4)


def load_aws_config(config: SkylarkConfig) -> SkylarkConfig:
    # get AWS credentials from boto3
    session = boto3.Session()
    credentials = session.get_credentials()
    credentials = credentials.get_frozen_credentials()
    auth = AWSAuthentication(config=config)
    if credentials.access_key is None or credentials.secret_key is None:
        config.aws_enabled = False
        typer.secho("    AWS credentials not found in boto3 session, please use the AWS CLI to set them via `aws configure`", fg="red")
        typer.secho("    https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html", fg="red")
        typer.secho("    Disabling AWS support", fg="blue")
        auth.clear_region_config()
        return config

    typer.secho(f"    Loaded AWS credentials from the AWS CLI [IAM access key ID: ...{credentials.access_key[-6:]}]", fg="blue")
    config.aws_enabled = True
    auth.save_region_config(config)
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
    auth = AzureAuthentication(config=config)
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
        config.azure_enabled = False
        return config
    typer.secho("    Azure credentials found in Azure CLI", fg="blue")
    inferred_subscription_id = AzureAuthentication.infer_subscription_id()
    if typer.confirm("    Azure credentials found, do you want to enable Azure support in Skylark?", default=True):
        config.azure_subscription_id = typer.prompt("    Enter the Azure subscription ID:", default=inferred_subscription_id)
        config.azure_enabled = True
    else:
        config.azure_subscription_id = None
        typer.secho("    Disabling Azure support", fg="blue")
        config.azure_enabled = False
    return config


def load_gcp_config(config: SkylarkConfig, force_init: bool = False) -> SkylarkConfig:
    if force_init:
        typer.secho("    GCP credentials will be re-initialized", fg="red")
        config.gcp_project_id = None
    elif not Path(gcp_config_path).is_file():
        typer.secho("    GCP region config missing! GCP will be reconfigured.", fg="red")
        config.gcp_project_id = None

    if config.gcp_project_id is not None:
        typer.secho("    GCP already configured! To reconfigure GCP, run `skylark init --reinit-gcp`.", fg="blue")
        config.gcp_enabled = True
        return config

    # check if GCP is enabled
    auth = GCPAuthentication(config=config)
    if not auth.credentials:
        typer.secho(
            "    Default GCP credentials are not set up yet. Run `gcloud auth application-default login`.",
            fg="red",
        )
        typer.secho("    https://cloud.google.com/docs/authentication/getting-started", fg="red")
        typer.secho("    Disabling GCP support", fg="blue")
        config.gcp_enabled = False
        auth.clear_region_config()
        return config
    else:
        typer.secho("    GCP credentials found in GCP CLI", fg="blue")
        if typer.confirm("    GCP credentials found, do you want to enable GCP support in Skylark?", default=True):
            config.gcp_project_id = typer.prompt("    Enter the GCP project ID", default=auth.project_id)
            assert config.gcp_project_id is not None, "GCP project ID must not be None"
            config.gcp_enabled = True
            auth.save_region_config(project_id=config.gcp_project_id)
            return config
        else:
            config.gcp_project_id = None
            typer.secho("    Disabling GCP support", fg="blue")
            config.gcp_enabled = False
            auth.clear_region_config()
            return config
