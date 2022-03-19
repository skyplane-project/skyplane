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
import skylark.cli.cli_gcp
import skylark.cli.cli_solver
import skylark.cli.experiments
import typer
from skylark.config import SkylarkConfig
from skylark.utils import logger
from skylark import config_path, GB, MB, print_header
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
    load_aws_config,
    load_azure_config,
    load_gcp_config,
    ls_local,
    ls_s3,
    parse_path,
)
from skylark.replicate.replication_plan import ReplicationJob, ReplicationTopology
from skylark.replicate.replicator_client import ReplicatorClient
from skylark.obj_store.object_store_interface import ObjectStoreInterface

app = typer.Typer(name="skylark")
app.add_typer(skylark.cli.experiments.app, name="experiments")
app.add_typer(skylark.cli.cli_aws.app, name="aws")
app.add_typer(skylark.cli.cli_azure.app, name="azure")
app.add_typer(skylark.cli.cli_gcp.app, name="gcp")
app.add_typer(skylark.cli.cli_solver.app, name="solver")


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
        account_name, container_name = bucket_dst
        copy_local_azure(Path(path_src), account_name, container_name, path_dst)
    elif provider_src == "azure" and provider_dst == "local":
        account_name, container_name = bucket_dst
        copy_azure_local(account_name, container_name, path_src, Path(path_dst))
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
    reuse_gateways: bool = False,
    gateway_docker_image: str = os.environ.get("SKYLARK_DOCKER_IMAGE", "ghcr.io/parasj/skylark:main"),
    aws_instance_class: str = "m5.8xlarge",
    azure_instance_class: str = "Standard_D32_v4",
    gcp_instance_class: Optional[str] = "n2-standard-32",
    gcp_use_premium_network: bool = True,
    key_prefix: str = "/test/replicate_random",
    time_limit_seconds: Optional[int] = None,
    log_interval_s: float = 1.0,
):
    """Replicate objects from remote object store to another remote object store."""
    print_header()
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
    stats = rc.monitor_transfer(job, show_pbar=True, log_interval_s=log_interval_s, time_limit_seconds=time_limit_seconds)
    stats["success"] = stats["monitor_status"] == "completed"
    out_json = {k: v for k, v in stats.items() if k not in ["log", "completed_chunk_ids"]}

    if not reuse_gateways:
        atexit.unregister(rc.deprovision_gateways)
        rc.deprovision_gateways()

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
    key_prefix: str = "/",
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
    """Replicate objects from remote object store to another remote object store."""
    print_header()
    check_ulimit()

    with path.open("r") as f:
        topo = ReplicationTopology.from_json(f.read())

    rc = ReplicatorClient(
        topo,
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
        obj_keys = []
        obj_sizes = dict()
        for obj in objs:
            obj_keys.append(obj.key)
            obj_sizes[obj.key] = obj.size

        # create replication job
        job = ReplicationJob(
            source_region=topo.source_region(),
            source_bucket=source_bucket,
            dest_region=topo.sink_region(),
            dest_bucket=dest_bucket,
            objs=obj_keys,
            obj_sizes=obj_sizes,
        )
        job = rc.run_replication_plan(job)

        # query chunk sizes
        total_bytes = sum([chunk_req.chunk.chunk_length_bytes for chunk_req in job.chunk_requests])

    logger.info(f"{total_bytes / GB:.2f}GByte replication job launched")
    stats = rc.monitor_transfer(job, show_pbar=True, log_interval_s=log_interval_s, time_limit_seconds=time_limit_seconds)
    stats["success"] = stats["monitor_status"] == "completed"
    out_json = {k: v for k, v in stats.items() if k not in ["log", "completed_chunk_ids"]}

    # deprovision
    if not reuse_gateways:
        atexit.unregister(rc.deprovision_gateways)
        rc.deprovision_gateways()

    typer.echo(f"\n{json.dumps(out_json)}")
    return 0 if stats["success"] else 1


@app.command()
def deprovision():
    """Deprovision gateways."""
    deprovision_skylark_instances()


@app.command()
def init(reinit_azure: bool = False, reinit_gcp: bool = False):
    print_header()
    if config_path.exists():
        cloud_config = SkylarkConfig.load_config(config_path)
    else:
        cloud_config = SkylarkConfig()

    # load AWS config
    typer.secho("\n(1) Configuring AWS:", fg="yellow", bold=True)
    cloud_config = load_aws_config(cloud_config)

    # load Azure config
    typer.secho("\n(2) Configuring Azure:", fg="yellow", bold=True)
    cloud_config = load_azure_config(cloud_config, force_init=reinit_azure)

    # load GCP config
    typer.secho("\n(3) Configuring GCP:", fg="yellow", bold=True)
    cloud_config = load_gcp_config(cloud_config, force_init=reinit_gcp)

    cloud_config.to_config_file(config_path)
    typer.secho(f"\nConfig file saved to {config_path}", fg="green")
    return 0


if __name__ == "__main__":
    app()
