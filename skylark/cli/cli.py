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
import sys
from typing import Optional

import typer
from loguru import logger

from skylark import GB, MB, print_header, config_file
import skylark.cli.cli_aws
import skylark.cli.cli_azure
import skylark.cli.experiments
from skylark.cli.cli_helper import (
    copy_local_local,
    copy_local_s3,
    copy_s3_local,
    deprovision_skylark_instances,
    load_config,
    ls_local,
    ls_s3,
    parse_path,
)
from skylark.replicate.replication_plan import ReplicationJob, ReplicationTopology
from skylark.replicate.replicator_client import ReplicatorClient

app = typer.Typer(name="skylark")
app.add_typer(skylark.cli.experiments.app, name="experiments")
app.add_typer(skylark.cli.cli_aws.app, name="aws")
app.add_typer(skylark.cli.cli_azure.app, name="azure")

# config logger
logger.remove()
logger.add(sys.stderr, format="{function:>20}:{line:<3} | <level>{message}</level>", enqueue=True)


@app.command()
def ls(directory: str):
    """List objects in the object store."""
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

    provider_src, bucket_src, path_src = parse_path(src)
    provider_dst, bucket_dst, path_dst = parse_path(dst)

    if provider_src == "local" and provider_dst == "local":
        copy_local_local(Path(path_src), Path(path_dst))
    elif provider_src == "local" and provider_dst == "s3":
        copy_local_s3(Path(path_src), bucket_dst, path_dst)
    elif provider_src == "s3" and provider_dst == "local":
        copy_s3_local(bucket_src, path_src, Path(path_dst))
    else:
        raise NotImplementedError(f"{provider_src} to {provider_dst} not supported yet")


@app.command()
def replicate_random(
    src_region: str,
    dst_region: str,
    inter_region: Optional[str] = typer.Argument(None),
    num_gateways: int = 1,
    num_outgoing_connections: int = 16,
    chunk_size_mb: int = 8,
    n_chunks: int = 2048,
    reuse_gateways: bool = True,
    azure_subscription: Optional[str] = None,
    gcp_project: Optional[str] = None,
    gateway_docker_image: str = os.environ.get("SKYLARK_DOCKER_IMAGE", "ghcr.io/parasj/skylark:main"),
    aws_instance_class: str = "m5.8xlarge",
    azure_instance_class: str = "Standard_D32_v5",
    gcp_instance_class: Optional[str] = "n2-standard-32",
    gcp_use_premium_network: bool = False,
    key_prefix: str = "/test/replicate_random",
    time_limit_seconds: Optional[int] = None,
    log_interval_s: float = 1.0,
    serve_web_dashboard: bool = True,
):
    """Replicate objects from remote object store to another remote object store."""
    print_header()
    config = load_config()
    gcp_project = gcp_project or config.get("gcp_project_id")
    azure_subscription = azure_subscription or config.get("azure_subscription_id")
    logger.debug(f"Loaded gcp_project: {gcp_project}, azure_subscription: {azure_subscription}")

    if inter_region:
        topo = ReplicationTopology(paths=[[src_region, inter_region, dst_region] for _ in range(num_gateways)])
        num_conn = num_outgoing_connections
    else:
        topo = ReplicationTopology(paths=[[src_region, dst_region] for _ in range(num_gateways)])
        num_conn = num_outgoing_connections
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
    rc.provision_gateways(
        reuse_instances=reuse_gateways,
        num_outgoing_connections=num_conn,
    )
    for path in rc.bound_paths:
        logger.info(f"Provisioned path {' -> '.join(path[i].region_tag for i in range(len(path)))}")
        for gw in path:
            logger.info(f"\t[{gw.region_tag}] {gw.gateway_log_viewer_url}")

    job = ReplicationJob(
        source_region=src_region,
        source_bucket="random",
        dest_region=dst_region,
        dest_bucket="random",
        objs=[f"{key_prefix}/{i}" for i in range(n_chunks)],
        random_chunk_size_mb=chunk_size_mb,
    )

    total_bytes = n_chunks * chunk_size_mb * MB
    crs = rc.run_replication_plan(job)
    logger.info(f"{total_bytes / GB:.2f}GByte replication job launched")
    stats = rc.monitor_transfer(
        crs,
        show_pbar=False,
        log_interval_s=log_interval_s,
        serve_web_dashboard=serve_web_dashboard,
        time_limit_seconds=time_limit_seconds,
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
    azure_tenant_id: str = typer.Option(None, envvar="AZURE_TENANT_ID", prompt="Azure tenant ID"),
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
