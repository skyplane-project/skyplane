"""
CLI for the Skylark object store.

Usage mostly matches the aws-cli command line tool:
`skylark [command] [subcommand] [flags] [args]`

Current support:
* `skylark ls /local/path`
* `skylark ls s3://bucket/path`
* `skylark cp /local/path /local/path`
* `skylark cp /local/path s3://bucket/path`
* `skylark cp s3://bucket/path /local/path`
"""


import atexit
import json
import os
from pathlib import Path
from typing import Optional

import skylark.cli.cli_aws
import skylark.cli.cli_azure
import skylark.cli.cli_solver
import skylark.cli.experiments
import typer
from skylark.utils import logger
from skylark import GB, MB, config_file, print_header
from skylark.cli.cli_helper import (
    check_ulimit,
    copy_azure_local,
    copy_gcs_local,
    copy_local_azure,
    copy_local_gcs,
    copy_local_local,
    copy_local_s3,
    copy_s3_local,
    copy_gcs_local,
    copy_local_gcs,
    deprovision_skylark_instances,
    ls_local,
    ls_s3,
    parse_path,
)
from skylark.config import load_config
from skylark.replicate.replication_plan import ReplicationJob, ReplicationTopology
from skylark.replicate.replicator_client import ReplicatorClient
from skylark.obj_store.object_store_interface import ObjectStoreInterface

app = typer.Typer(name="skylark")
app.add_typer(skylark.cli.experiments.app, name="experiments")
app.add_typer(skylark.cli.cli_aws.app, name="aws")
app.add_typer(skylark.cli.cli_azure.app, name="azure")
app.add_typer(skylark.cli.cli_solver.app, name="solver")


@app.command()
def ls(directory: str):
    """List objects in the object store."""
    config = load_config()
    gcp_project = config.get("gcp_project_id")
    azure_subscription = config.get("azure_subscription_id")
    logger.debug(f"Loaded gcp_project: {gcp_project}, azure_subscription: {azure_subscription}")
    check_ulimit()
    provider, bucket, key = parse_path(directory)
    if provider == "local":
        for path in ls_local(Path(directory)):
            typer.echo(path)
    elif provider == "s3":
        for path in ls_s3(bucket, key):
            typer.echo(path)


@app.command()
def cp(src: str, dst: str):
    """Copy objects from the object store to the local filesystem."""
    print_header()
    config = load_config()
    gcp_project = config.get("gcp_project_id")
    azure_subscription = config.get("azure_subscription_id")
    logger.debug(f"Loaded gcp_project: {gcp_project}, azure_subscription: {azure_subscription}")
    check_ulimit()

    provider_src, bucket_src, path_src = parse_path(src)
    provider_dst, bucket_dst, path_dst = parse_path(dst)

    if provider_src == "local" and provider_dst == "local":
        copy_local_local(Path(path_src), Path(path_dst))
    elif provider_src == "local" and provider_dst == "s3":
        copy_local_s3(Path(path_src), bucket_dst, path_dst)
    elif provider_src == "s3" and provider_dst == "local":
        copy_s3_local(bucket_src, path_src, Path(path_dst))
    elif provider_src == "local" and provider_dst == "gs":
        copy_local_gcs(Path(path_src), bucket_dst, path_dst)
    elif provider_src == "gs" and provider_dst == "local":
        copy_gcs_local(bucket_src, path_src, Path(path_dst))
    elif provider_src == "local" and provider_dst == "azure":
        copy_local_azure(Path(path_src), bucket_dst, path_dst)
    elif provider_src == "azure" and provider_dst == "local":
        copy_azure_local(bucket_src, path_src, Path(path_dst))
    else:
        raise NotImplementedError(f"{provider_src} to {provider_dst} not supported yet")


@app.command()
def replicate_random(
    src_region: str,
    dst_region: str,
    inter_region: Optional[str] = typer.Argument(None),
    num_gateways: int = typer.Option(1, "--num-gateways", "-n", help="Number of gateways"),
    num_outgoing_connections: int = typer.Option(
        64, "--num-outgoing-connections", "-c", help="Number of outgoing connections between each gateway"
    ),
    total_transfer_size_mb: int = typer.Option(2048, "--size-total-mb", "-s", help="Total transfer size in MB."),
    chunk_size_mb: int = typer.Option(8, "--chunk-size-mb", help="Chunk size in MB."),
    reuse_gateways: bool = True,
    azure_subscription: Optional[str] = None,
    gcp_project: Optional[str] = None,
    gateway_docker_image: str = os.environ.get("SKYLARK_DOCKER_IMAGE", "ghcr.io/parasj/skylark:main"),
    aws_instance_class: str = "m5.8xlarge",
    azure_instance_class: str = "Standard_D32_v5",
    gcp_instance_class: Optional[str] = "n2-standard-32",
    gcp_use_premium_network: bool = True,
    key_prefix: str = "/test/replicate_random",
    time_limit_seconds: Optional[int] = None,
    log_interval_s: float = 1.0,
):
    """Replicate objects from remote object store to another remote object store."""
    print_header()
    config = load_config()
    gcp_project = gcp_project or config.get("gcp_project_id")
    azure_subscription = azure_subscription or config.get("azure_subscription_id")
    logger.debug(f"Loaded gcp_project: {gcp_project}, azure_subscription: {azure_subscription}")
    check_ulimit()

    if inter_region:
        assert inter_region not in [src_region, dst_region] and src_region != dst_region
        topo = ReplicationTopology()
        for i in range(num_gateways):
            topo.add_edge(src_region, i, inter_region, i, num_outgoing_connections)
            topo.add_edge(inter_region, i, dst_region, i, num_outgoing_connections)
    else:
        assert src_region != dst_region
        topo = ReplicationTopology()
        for i in range(num_gateways):
            topo.add_edge(src_region, i, dst_region, i, num_outgoing_connections)

    rc = ReplicatorClient(
        topo,
        azure_subscription=azure_subscription,
        gcp_project=gcp_project,
        gateway_docker_image=gateway_docker_image,
        aws_instance_class=aws_instance_class,
        azure_instance_class=azure_instance_class,
        gcp_instance_class=gcp_instance_class,
        gcp_use_premium_network=gcp_use_premium_network,
    )

    if not reuse_gateways:
        atexit.register(rc.deprovision_gateways)
    else:
        logger.warning(
            f"Instances will remain up and may result in continued cloud billing. Remember to call `skylark deprovision` to deprovision gateways."
        )
    rc.provision_gateways(reuse_gateways)
    for node, gw in rc.bound_nodes.items():
        logger.info(f"Provisioned {node}: {gw.gateway_log_viewer_url}")

    if total_transfer_size_mb % chunk_size_mb != 0:
        logger.warning(f"total_transfer_size_mb ({total_transfer_size_mb}) is not a multiple of chunk_size_mb ({chunk_size_mb})")
    n_chunks = int(total_transfer_size_mb / chunk_size_mb)
    job = ReplicationJob(
        source_region=src_region,
        source_bucket=None,
        dest_region=dst_region,
        dest_bucket=None,
        objs=[f"{key_prefix}/{i}" for i in range(n_chunks)],
        random_chunk_size_mb=chunk_size_mb,
    )

    total_bytes = n_chunks * chunk_size_mb * MB
    job = rc.run_replication_plan(job)
    logger.info(f"{total_bytes / GB:.2f}GByte replication job launched")
    stats = rc.monitor_transfer(job, show_pbar=False, log_interval_s=log_interval_s, time_limit_seconds=time_limit_seconds)
    stats["success"] = stats["monitor_status"] == "completed"
    stats["log"] = rc.get_chunk_status_log_df()

    out_json = {k: v for k, v in stats.items() if k not in ["log", "completed_chunk_ids"]}
    typer.echo(f"\n{json.dumps(out_json)}")
    return 0 if stats["success"] else 1


@app.command()
def replicate_json(
    path: Path = typer.Argument(..., exists=True, file_okay=True, dir_okay=False, help="Path to JSON file describing replication plan"),
    size_total_mb: int = typer.Option(2048, "--size-total-mb", "-s", help="Total transfer size in MB (across n_chunks chunks)"),
    n_chunks: int = typer.Option(512, "--n-chunks", "-n", help="Number of chunks"),
    # bucket options
    use_random_data: bool = False,
    source_bucket: str = typer.Option(None, "--source-bucket", help="Source bucket url"),
    dest_bucket: str = typer.Option(None, "--dest-bucket", help="Destination bucket url"),
    key_prefix: str = "/test/replicate_random",
    # gateway provisioning options
    reuse_gateways: bool = True,
    gateway_docker_image: str = os.environ.get("SKYLARK_DOCKER_IMAGE", "ghcr.io/parasj/skylark:main"),
    # cloud provider specific options
    azure_subscription: Optional[str] = None,
    gcp_project: Optional[str] = None,
    aws_instance_class: str = "m5.8xlarge",
    azure_instance_class: str = "Standard_D32_v5",
    gcp_instance_class: Optional[str] = "n2-standard-32",
    gcp_use_premium_network: bool = True,
    # logging options
    time_limit_seconds: Optional[int] = None,
    log_interval_s: float = 1.0,
):
    """Replicate objects from remote object store to another remote object store."""
    print_header()
    config = load_config()
    gcp_project = gcp_project or config.get("gcp_project_id")
    azure_subscription = azure_subscription or config.get("azure_subscription_id")
    logger.debug(f"Loaded gcp_project: {gcp_project}, azure_subscription: {azure_subscription}")
    check_ulimit()

    with path.open("r") as f:
        topo = ReplicationTopology.from_json(f.read())

    rc = ReplicatorClient(
        topo,
        azure_subscription=azure_subscription,
        gcp_project=gcp_project,
        gateway_docker_image=gateway_docker_image,
        aws_instance_class=aws_instance_class,
        azure_instance_class=azure_instance_class,
        gcp_instance_class=gcp_instance_class,
        gcp_use_premium_network=gcp_use_premium_network,
    )

    if not reuse_gateways:
        atexit.register(rc.deprovision_gateways)
    else:
        logger.warning(
            f"Instances will remain up and may result in continued cloud billing. Remember to call `skylark deprovision` to deprovision gateways."
        )
    rc.provision_gateways(reuse_gateways)
    for node, gw in rc.bound_nodes.items():
        logger.info(f"Provisioned {node}: {gw.gateway_log_viewer_url}")

    if size_total_mb % n_chunks != 0:
        logger.warning(f"total_transfer_size_mb ({size_total_mb}) is not a multiple of n_chunks ({n_chunks})")
    chunk_size_mb = size_total_mb // n_chunks

    print("REGION", topo.source_region())

    if use_random_data:
        job = ReplicationJob(
            source_region=topo.source_region(),
            source_bucket=None,
            dest_region=topo.sink_region(),
            dest_bucket=None,
            objs=[f"{key_prefix}/{i}" for i in range(n_chunks)],
            random_chunk_size_mb=chunk_size_mb,
        )
        job = rc.run_replication_plan(job)
        total_bytes = n_chunks * chunk_size_mb * MB
    else:

        # get object keys with prefix
        objs = ObjectStoreInterface.create(topo.source_region(), source_bucket).list_objects(key_prefix)
        obj_keys = list([obj.key for obj in objs])

        # create replication job
        job = ReplicationJob(
            source_region=topo.source_region(),
            source_bucket=source_bucket,
            dest_region=topo.sink_region(),
            dest_bucket=dest_bucket,
            objs=obj_keys,
        )
        job = rc.run_replication_plan(job)

        # query chunk sizes
        total_bytes = sum([chunk_req.chunk.chunk_length_bytes for chunk_req in job.chunk_requests])

    logger.info(f"{total_bytes / GB:.2f}GByte replication job launched")
    stats = rc.monitor_transfer(
        job, show_pbar=True, log_interval_s=log_interval_s, time_limit_seconds=time_limit_seconds, cancel_pending=False
    )
    stats["success"] = stats["monitor_status"] == "completed"
    stats["log"] = rc.get_chunk_status_log_df()

    out_json = {k: v for k, v in stats.items() if k not in ["log", "completed_chunk_ids"]}
    typer.echo(f"\n{json.dumps(out_json)}")
    return 0 if stats["success"] else 1


@app.command()
def deprovision(azure_subscription: Optional[str] = None, gcp_project: Optional[str] = None):
    """Deprovision gateways."""
    config = load_config()
    gcp_project = gcp_project or config.get("gcp_project_id")
    azure_subscription = azure_subscription or config.get("azure_subscription_id")
    logger.debug(f"Loaded from config file: gcp_project={gcp_project}, azure_subscription={azure_subscription}")
    deprovision_skylark_instances(azure_subscription=azure_subscription, gcp_project_id=gcp_project)


@app.command()
def init(
    azure_tenant_id: str = typer.Option(None, envvar="AZURE_TENANT_ID", prompt="`Azure tenant ID"),
    azure_client_id: str = typer.Option(None, envvar="AZURE_CLIENT_ID", prompt="Azure client ID"),
    azure_client_secret: str = typer.Option(None, envvar="AZURE_CLIENT_SECRET", prompt="Azure client secret"),
    azure_subscription_id: str = typer.Option(None, envvar="AZURE_SUBSCRIPTION_ID", prompt="Azure subscription ID"),
    gcp_application_credentials_file: Path = typer.Option(
        None,
        envvar="GOOGLE_APPLICATION_CREDENTIALS",
        exists=True,
        file_okay=True,
        dir_okay=False,
        readable=True,
        help="Path to GCP application credentials file (usually a JSON file)",
    ),
    gcp_project: str = typer.Option(None, envvar="GCP_PROJECT_ID", prompt="GCP project ID"),
):
    out_config = {}
    if config_file.exists():
        typer.confirm("Config file already exists. Overwrite?", abort=True)

    # AWS config
    def load_aws_credentials():
        if "AWS_ACCESS_KEY_ID" in os.environ and "AWS_SECRET_ACCESS_KEY" in os.environ:
            return os.environ["AWS_ACCESS_KEY_ID"], os.environ["AWS_SECRET_ACCESS_KEY"]
        if (Path.home() / ".aws" / "credentials").exists():
            with open(Path.home() / ".aws" / "credentials") as f:
                access_key, secret_key = None, None
                lines = f.readlines()
                for line in lines:
                    if line.startswith("aws_access_key_id"):
                        access_key = line.split("=")[1].strip()
                    if line.startswith("aws_secret_access_key"):
                        secret_key = line.split("=")[1].strip()
                if access_key and secret_key:
                    return access_key, secret_key
        return None, None

    aws_access_key, aws_secret_key = load_aws_credentials()
    if aws_access_key is None:
        aws_access_key = typer.prompt("AWS access key")
        assert aws_access_key is not None and aws_access_key != ""
    if aws_secret_key is None:
        aws_secret_key = typer.prompt("AWS secret key")
        assert aws_secret_key is not None and aws_secret_key != ""
    out_config["aws_access_key_id"] = aws_access_key
    out_config["aws_secret_access_key"] = aws_secret_key

    # Azure config
    typer.secho("Azure config can be generated using: az ad sp create-for-rbac -n api://skylark --sdk-auth", fg=typer.colors.GREEN)
    if azure_tenant_id is not None or len(azure_tenant_id) > 0:
        logger.info(f"Setting Azure tenant ID to {azure_tenant_id}")
        out_config["azure_tenant_id"] = azure_tenant_id
    if azure_client_id is not None or len(azure_client_id) > 0:
        logger.info(f"Setting Azure client ID to {azure_client_id}")
        out_config["azure_client_id"] = azure_client_id
    if azure_client_secret is not None or len(azure_client_secret) > 0:
        logger.info(f"Setting Azure client secret to {azure_client_secret}")
        out_config["azure_client_secret"] = azure_client_secret
    if azure_subscription_id is not None or len(azure_subscription_id) > 0:
        logger.info(f"Setting Azure subscription ID to {azure_subscription_id}")
        out_config["azure_subscription_id"] = azure_subscription_id

    # GCP config
    if gcp_application_credentials_file is not None and gcp_application_credentials_file.exists():
        logger.info(f"Setting GCP application credentials file to {gcp_application_credentials_file}")
        out_config["gcp_application_credentials_file"] = str(gcp_application_credentials_file)
    if gcp_project is not None or len(gcp_project) > 0:
        logger.info(f"Setting GCP project ID to {gcp_project}")
        out_config["gcp_project_id"] = gcp_project

    # write to config file
    config_file.parent.mkdir(parents=True, exist_ok=True)
    with config_file.open("w") as f:
        json.dump(out_config, f)
    typer.secho(f"Config: {out_config}", fg=typer.colors.GREEN)
    typer.secho(f"Wrote config to {config_file}", fg=typer.colors.GREEN)
    return 0


if __name__ == "__main__":
    app()
