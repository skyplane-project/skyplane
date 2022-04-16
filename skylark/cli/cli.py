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

import os
from pathlib import Path
from typing import Optional

from skylark import skylark_root
from skylark import exceptions
import skylark.cli.cli_aws
import skylark.cli.cli_azure
import skylark.cli.cli_gcp
import skylark.cli.cli_solver
import skylark.cli.experiments
from skylark.obj_store.object_store_interface import ObjectStoreInterface
from skylark.replicate.solver import ThroughputProblem, ThroughputSolverILP
import typer
from skylark.config import SkylarkConfig
from skylark.utils import logger
from skylark.utils.utils import Timer
from skylark import GB, config_path, print_header
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
    ls_objstore,
    parse_path,
    replicate_helper,
)
from skylark.replicate.replication_plan import ReplicationTopology

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
        for path in ls_objstore("aws:infer", bucket, key):
            typer.echo(path)
    elif provider == "gs":
        for path in ls_objstore("gcp:infer", bucket, key):
            typer.echo(path)
    elif provider == "azure":
        for path in ls_objstore("azure:infer", bucket, key):
            typer.echo(path)
    else:
        raise NotImplementedError(f"Unrecognized object store provider")


@app.command()
def cp(
    src: str,
    dst: str,
    num_connections: int = typer.Option(64, help="Number of connections to open for replication"),
    max_instances: int = typer.Option(1, help="Max number of instances per overlay region."),
    reuse_gateways: bool = typer.Option(False, help="If true, will leave provisioned instances running to be reused"),
    solve: bool = typer.Option(False, help="If true, will use solver to optimize transfer, else direct path is chosen"),
    solver_required_throughput_gbits: float = typer.Option(2, help="Solver option: Required throughput in gbps."),
    solver_throughput_grid: Path = typer.Option(
        skylark_root / "profiles" / "throughput.csv", "--throughput-grid", help="Throughput grid file"
    ),
    solver_verbose: bool = False,
):
    """Copy objects from the object store to the local filesystem."""
    print_header()

    provider_src, bucket_src, path_src = parse_path(src)
    provider_dst, bucket_dst, path_dst = parse_path(dst)

    clouds = {"s3": "aws:infer", "gs": "gcp:infer", "azure": "azure:infer"}

    # raise file limits for local transfers
    if provider_src == "local" or provider_dst == "local":
        check_ulimit()

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
    elif provider_src in clouds and provider_dst in clouds:
        src_client = ObjectStoreInterface.create(clouds[provider_src], bucket_src)
        src_region = src_client.region_tag()

        dst_client = ObjectStoreInterface.create(clouds[provider_dst], bucket_dst)
        dst_region = dst_client.region_tag()

        # Set up replication topology
        if solve:
            objs = list(src_client.list_objects(path_src))
            if not objs:
                logger.error("Specified object does not exist.")
                raise exceptions.MissingObjectException()

            total_gbyte_to_transfer = sum([obj.size for obj in objs]) / GB

            # build problem and solve
            tput = ThroughputSolverILP(solver_throughput_grid)
            problem = ThroughputProblem(
                src=src_region,
                dst=dst_region,
                required_throughput_gbits=solver_required_throughput_gbits,
                gbyte_to_transfer=total_gbyte_to_transfer,
                instance_limit=max_instances,
            )
            with Timer("Solve throughput problem"):
                solution = tput.solve_min_cost(
                    problem,
                    solver=ThroughputSolverILP.choose_solver(),
                    solver_verbose=solver_verbose,
                    save_lp_path=None,
                )
            topo = tput.to_replication_topology(solution)
        else:
            topo = ReplicationTopology()
            for i in range(max_instances):
                topo.add_edge(src_region, i, dst_region, i, num_connections)

        replicate_helper(topo, source_bucket=bucket_src, dest_bucket=bucket_dst, src_key_prefix=path_src, dest_key_prefix=path_dst, reuse_gateways=reuse_gateways)
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
    gateway_docker_image: str = os.environ.get("SKYLARK_DOCKER_IMAGE", "ghcr.io/skyplane-project/skyplane:main"),
    aws_instance_class: str = "m5.8xlarge",
    azure_instance_class: str = "Standard_D32_v4",
    gcp_instance_class: Optional[str] = "n2-standard-32",
    gcp_use_premium_network: bool = True,
    time_limit_seconds: Optional[int] = None,
    log_interval_s: float = 1.0,
):
    """Replicate objects from remote object store to another remote object store."""
    print_header()
    if reuse_gateways:
        logger.warning(
            f"Instances will remain up and may result in continued cloud billing. Remember to call `skylark deprovision` to deprovision gateways."
        )

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

    if total_transfer_size_mb % chunk_size_mb != 0:
        logger.warning(f"total_transfer_size_mb ({total_transfer_size_mb}) is not a multiple of chunk_size_mb ({chunk_size_mb})")
    n_chunks = int(total_transfer_size_mb / chunk_size_mb)

    return replicate_helper(
        topo,
        size_total_mb=total_transfer_size_mb,
        n_chunks=n_chunks,
        random=True,
        reuse_gateways=reuse_gateways,
        gateway_docker_image=gateway_docker_image,
        aws_instance_class=aws_instance_class,
        gcp_instance_class=gcp_instance_class,
        azure_instance_class=azure_instance_class,
        gcp_use_premium_network=gcp_use_premium_network,
        time_limit_seconds=time_limit_seconds,
        log_interval_s=log_interval_s,
    )


@app.command()
def replicate_json(
    path: Path = typer.Argument(..., exists=True, file_okay=True, dir_okay=False, help="Path to JSON file describing replication plan"),
    size_total_mb: int = typer.Option(2048, "--size-total-mb", "-s", help="Total transfer size in MB (across n_chunks chunks)"),
    n_chunks: int = typer.Option(512, "--n-chunks", "-n", help="Number of chunks"),
    # bucket options
    use_random_data: bool = False,
    source_bucket: str = typer.Option(None, "--source-bucket", help="Source bucket url"),
    dest_bucket: str = typer.Option(None, "--dest-bucket", help="Destination bucket url"),
    src_key_prefix: str = "/",
    dest_key_prefix: str = "/",
    # gateway provisioning options
    reuse_gateways: bool = False,
    gateway_docker_image: str = os.environ.get("SKYLARK_DOCKER_IMAGE", "ghcr.io/skyplane-project/skyplane:main"),
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

    with path.open("r") as f:
        topo = ReplicationTopology.from_json(f.read())

    return replicate_helper(
        topo,
        size_total_mb=size_total_mb,
        n_chunks=n_chunks,
        random=use_random_data,
        source_bucket=source_bucket,
        dest_bucket=dest_bucket,
        src_key_prefix=src_key_prefix,
        dest_key_prefix=dest_key_prefix,
        reuse_gateways=reuse_gateways,
        gateway_docker_image=gateway_docker_image,
        aws_instance_class=aws_instance_class,
        gcp_instance_class=gcp_instance_class,
        azure_instance_class=azure_instance_class,
        gcp_use_premium_network=gcp_use_premium_network,
        time_limit_seconds=time_limit_seconds,
        log_interval_s=log_interval_s,
    )


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
        cloud_config = SkylarkConfig.default_config()

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
