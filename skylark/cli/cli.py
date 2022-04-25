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

from pathlib import Path

import skylark.cli.cli_aws
import skylark.cli.cli_azure
import skylark.cli.cli_gcp
import skylark.cli.cli_solver
import skylark.cli.experiments
import typer
from skylark import GB, config_path, print_header, skylark_root
from skylark.cli.cli_helper import (
    check_ulimit,
    copy_azure_local,
    copy_gcs_local,
    copy_local_azure,
    copy_local_gcs,
    copy_local_local,
    copy_local_s3,
    copy_s3_local,
    deprovision_skylark_instances,
    load_aws_config,
    load_azure_config,
    load_gcp_config,
    ls_local,
    ls_objstore,
    parse_path,
    replicate_helper,
)
from skylark.config import SkylarkConfig
from skylark.obj_store.object_store_interface import ObjectStoreInterface
from skylark.replicate.replication_plan import ReplicationTopology
from skylark.replicate.solver import ThroughputProblem, ThroughputSolverILP
from skylark.utils.utils import Timer

app = typer.Typer(name="skylark")
app.add_typer(skylark.cli.experiments.app, name="experiments")
app.add_typer(skylark.cli.cli_aws.app, name="aws")
app.add_typer(skylark.cli.cli_azure.app, name="azure")
app.add_typer(skylark.cli.cli_gcp.app, name="gcp")
app.add_typer(skylark.cli.cli_solver.app, name="solver")


@app.command()
def ls(directory: str):
    """
    It takes a directory path, parses it, and then calls the appropriate function to list the contents
    of that directory. If the path is on an object store, it will list the contents of the object store
    at that prefix.

    :param directory: str
    :type directory: str
    """
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
    solver_required_throughput_gbits: float = typer.Option(4, help="Solver option: Required throughput in Gbps"),
    solver_throughput_grid: Path = typer.Option(
        skylark_root / "profiles" / "throughput.csv", "--throughput-grid", help="Throughput grid file"
    ),
    solver_verbose: bool = False,
):
    """
    `cp` copies a file or folder from one location to another. If the source is on an object store,
    it will copy all objects with that prefix. If it is a local path, it will copy the entire file
    or directory tree.

    By default, it will copy objects using a direct connection between instances. However, if you would
    like to use the solver, call `--solve`. Note that the solver requires a throughput grid file to be
    specified. We provide a default one but it may be out-of-date.

    :param src: Source prefix to copy from
    :type src: str
    :param dst: The destination of the transfer
    :type dst: str
    :param num_connections: Number of connections to use between each gateway instance pair (default: 64)
    :type num_connections: int
    :param max_instances: The maximum number of instances to use per region (default: 1)
    :type max_instances: int
    :param reuse_gateways: If true, will leave provisioned instances running to be reused. You must run `skylark deprovision` to clean up.
    :type reuse_gateways: bool
    :param solve: If true, will use solver to optimize transfer, else direct path is chosen
    :type solve: bool
    :param solver_required_throughput_gbits: The required throughput in Gbps when using the solver (default: 4)
    :type solver_required_throughput_gbits: float
    :param solver_throughput_grid: The throughput grid profile to use for the solver, defaults to author-provided profile
    :type solver_throughput_grid: Path
    :param solver_verbose: If true, will print out the solver's output, defaults to False
    :type solver_verbose: bool (optional)
    """
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
            topo, _ = tput.to_replication_topology(solution)
        else:
            topo = ReplicationTopology()
            for i in range(max_instances):
                topo.add_edge(src_region, i, dst_region, i, num_connections)

        replicate_helper(
            topo,
            source_bucket=bucket_src,
            dest_bucket=bucket_dst,
            src_key_prefix=path_src,
            dest_key_prefix=path_dst,
            reuse_gateways=reuse_gateways,
        )
    else:
        raise NotImplementedError(f"{provider_src} to {provider_dst} not supported yet")


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
