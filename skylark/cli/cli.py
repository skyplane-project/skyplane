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

import typer
from loguru import logger
from skylark import GB, MB
from skylark.cli.cli_helper import (
    copy_local_local,
    copy_local_s3,
    copy_s3_local,
    deprovision_skylark_instances,
    ls_local,
    ls_s3,
    parse_path,
)
from skylark.replicate.replication_plan import ReplicationJob, ReplicationTopology
from skylark.replicate.replicator_client import ReplicatorClient

app = typer.Typer(name="skylark")


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
    inter_region: Optional[str] = None,
    num_gateways: int = 1,
    num_outgoing_connections: int = 16,
    chunk_size_mb: int = 8,
    n_chunks: int = 2048,
    reuse_gateways: bool = True,
    gcp_project: str = "skylark-333700",
    gateway_docker_image: str = os.environ.get("SKYLARK_DOCKER_IMAGE", "ghcr.io/parasj/skylark:main"),
    aws_instance_class: str = "m5.8xlarge",
    gcp_instance_class: Optional[str] = None,
    gcp_use_premium_network: bool = False,
    key_prefix: str = "/test/replicate_random",
    time_limit_seconds: Optional[int] = None,
    log_interval_s: float = 1.0,
    serve_web_dashboard: bool = True,
):
    """Replicate objects from remote object store to another remote object store."""
    if inter_region:
        topo = ReplicationTopology(paths=[[src_region, inter_region, dst_region] for _ in range(num_gateways)])
        num_conn = num_outgoing_connections
    else:
        topo = ReplicationTopology(paths=[[src_region, dst_region] for _ in range(num_gateways)])
        num_conn = num_outgoing_connections
    rc = ReplicatorClient(
        topo,
        gcp_project=gcp_project,
        gateway_docker_image=gateway_docker_image,
        aws_instance_class=aws_instance_class,
        gcp_instance_class=gcp_instance_class,
        gcp_use_premium_network=gcp_use_premium_network,
    )

    if not reuse_gateways:
        logger.warning(f"Deprovisioning gateways because reuse_gateways=False")
        atexit.register(rc.deprovision_gateways)
    else:
        logger.warning(
            f"Gateways not deprovisioned because reuse_gateways=True. Remember to call `skylark deprovision` to deprovision gateways."
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
        crs, serve_web_dashboard=serve_web_dashboard, show_pbar=False, log_interval_s=log_interval_s, time_limit_seconds=time_limit_seconds
    )
    stats["success"] = True
    stats["log"] = rc.get_chunk_status_log_df()

    out_json = {k: v for k, v in stats.items() if k not in ["log", "completed_chunk_ids"]}
    print(f"\n{json.dumps(out_json)}")


@app.command()
def deprovision(gcp_project: Optional[str] = None):
    """Deprovision gateways."""
    deprovision_skylark_instances(gcp_project_id=gcp_project)


if __name__ == "__main__":
    app()
