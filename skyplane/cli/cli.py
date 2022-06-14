"""CLI for the Skyplane object store"""
import subprocess
from functools import partial
from pathlib import Path
from shlex import split

import questionary
import typer

import skyplane.cli.cli_aws
import skyplane.cli.cli_azure
import skyplane.cli.cli_config
import skyplane.cli.cli_internal as cli_internal
import skyplane.cli.cli_solver
import skyplane.cli.experiments
from skyplane import GB, config_path, skyplane_root, cloud_config
from skyplane.cli.common import print_header
from skyplane.cli.cli_impl.cp_local import (
    copy_azure_local,
    copy_gcs_local,
    copy_local_azure,
    copy_local_gcs,
    copy_local_local,
    copy_local_s3,
    copy_s3_local,
)
from skyplane.cli.cli_impl.cp_replicate import (
    generate_full_transferobjlist,
    generate_topology,
    generate_transfer_obj_list,
    confirm_transfer,
    launch_replication_job,
)
from skyplane.replicate.replication_plan import TransferObjectList, ReplicationJob
from skyplane.cli.cli_impl.init import load_aws_config, load_azure_config, load_gcp_config
from skyplane.cli.cli_impl.ls import ls_local, ls_objstore
from skyplane.cli.common import check_ulimit, parse_path, query_instances
from skyplane.compute.aws.aws_auth import AWSAuthentication
from skyplane.compute.aws.aws_cloud_provider import AWSCloudProvider
from skyplane.config import SkyplaneConfig
from skyplane.obj_store.object_store_interface import ObjectStoreInterface
from skyplane.utils import logger
from skyplane.utils.fn import do_parallel

app = typer.Typer(name="skyplane")
app.command()(cli_internal.replicate_random)
app.command()(cli_internal.replicate_random_solve)
app.add_typer(skyplane.cli.experiments.app, name="experiments")
app.add_typer(skyplane.cli.cli_aws.app, name="aws")
app.add_typer(skyplane.cli.cli_azure.app, name="azure")
app.add_typer(skyplane.cli.cli_config.app, name="config")
app.add_typer(skyplane.cli.cli_solver.app, name="solver")


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
    num_connections: int = typer.Option(32, "--num-connections", "-c", help="Number of connections between gateways"),
    max_instances: int = typer.Option(1, "--max-instances", "-n", help="Number of gateways"),
    reuse_gateways: bool = typer.Option(False, help="If true, will leave provisioned instances running to be reused"),
    max_chunk_size_mb: int = typer.Option(None, help="Maximum size (MB) of chunks for multipart uploads/downloads"),
    confirm: bool = typer.Option(False, "--confirm", "-y", "-f", help="Confirm all transfer prompts"),
    debug: bool = typer.Option(False, help="If true, will write debug information to debug directory."),
    use_bbr: bool = typer.Option(True, help="If true, will use BBR congestion control"),
    use_compression: bool = typer.Option(False, help="If true, will use compression for uploads/downloads"),
    solve: bool = typer.Option(False, help="If true, will use solver to optimize transfer, else direct path is chosen"),
    solver_required_throughput_gbits: float = typer.Option(4, help="Solver option: Required throughput in Gbps"),
    solver_throughput_grid: Path = typer.Option(
        skyplane_root / "profiles" / "throughput.csv", "--throughput-grid", help="Throughput grid file"
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
    :param reuse_gateways: If true, will leave provisioned instances running to be reused. You must run `skyplane deprovision` to clean up.
    :type reuse_gateways: bool
    :param max_chunk_size_mb: If set, `cp` will subdivide objects into chunks at most this size.
    :type max_chunk_size_mb: int
    :param confirm: If true, will not prompt for confirmation of transfer.
    :type confirm: bool
    :param debug: If true, will write debug information to debug directory.
    :type debug: bool
    :param use_bbr: If set, will use BBR for transfers by default.
    :type use_bbr: bool
    :param use_compression: If set, will use compression for transfers.
    :type use_compression: bool
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

    if (provider_src == "azure" or provider_dst == "azure") and max_chunk_size_mb:
        raise ValueError(f"Multipart uploads not supported for Azure")

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
        transfer_pairs = generate_full_transferobjlist(src_region, bucket_src, path_src, dst_region, bucket_dst, path_dst)
        topo = generate_topology(
            src_region,
            dst_region,
            solve,
            num_connections=num_connections,
            max_instances=max_instances,
            solver_total_gbyte_to_transfer=sum(src_obj.size for src_obj, _ in transfer_pairs) if solve else None,
            solver_required_throughput_gbits=solver_required_throughput_gbits,
            solver_throughput_grid=solver_throughput_grid,
            solver_verbose=solver_verbose,
        )
        job = ReplicationJob(
            source_region=topo.source_region(),
            source_bucket=bucket_src,
            dest_region=topo.sink_region(),
            dest_bucket=bucket_dst,
            transfer_pairs=transfer_pairs,
            max_chunk_size_mb=max_chunk_size_mb,
        )
        confirm_transfer(
            topo=topo,
            n_objs=len(transfer_pairs),
            est_size_bytes=job.transfer_size,
            ask_to_confirm_transfer=not cloud_config.get_flag("autoconfirm") and not confirm,
        )
        stats = launch_replication_job(
            topo=topo,
            job=job,
            debug=debug,
            reuse_gateways=reuse_gateways,
            use_bbr=use_bbr,
            use_compression=use_compression,
        )
        return 0 if stats["success"] else 1
    else:
        raise NotImplementedError(f"{provider_src} to {provider_dst} not supported yet")


@app.command()
def sync(
    src: str,
    dst: str,
    num_connections: int = typer.Option(32, "--num-connections", "-c", help="Number of connections between gateways"),
    max_instances: int = typer.Option(1, "--max-instances", "-n", help="Number of gateways"),
    reuse_gateways: bool = typer.Option(False, help="If true, will leave provisioned instances running to be reused"),
    max_chunk_size_mb: int = typer.Option(None, help="Maximum size (MB) of chunks for multipart uploads/downloads"),
    confirm: bool = typer.Option(False, "--confirm", "-y", "-f", help="Confirm all transfer prompts"),
    debug: bool = typer.Option(False, help="If true, will write debug info to debug directory"),
    use_bbr: bool = typer.Option(True, help="If true, will use BBR congestion control"),
    use_compression: bool = typer.Option(False, help="If true, will use compression for uploads/downloads"),
    solve: bool = typer.Option(False, help="If true, will use solver to optimize transfer, else direct path is chosen"),
    solver_required_throughput_gbits: float = typer.Option(4, help="Solver option: Required throughput in Gbps"),
    solver_throughput_grid: Path = typer.Option(
        skyplane_root / "profiles" / "throughput.csv", "--throughput-grid", help="Throughput grid file"
    ),
    solver_verbose: bool = False,
):
    """
    'sync` synchronizes files or folders from one location to another. If the source is on an object store,
    it will copy all objects with that prefix. If it is a local path, it will copy the entire file
    or directory tree.

    By default, it will copy objects using a direct connection between instances. However, if you would
    like to use the solver, call `--solve`. Note that the solver requires a throughput grid file to be
    specified. We provide a default one but it may be out-of-date.

    For each file in the source, it is copied over if the file does not exist in the destination, it has
    a different size in the destination, or if the source version of the file was more recently modified
    than the destination. This behavior is similar to 'aws sync'.

    :param src: Source prefix to copy from
    :type src: str
    :param dst: The destination of the transfer
    :type dst: str
    :param num_connections: Number of connections to use between each gateway instance pair (default: 64)
    :type num_connections: int
    :param max_instances: The maximum number of instances to use per region (default: 1)
    :type max_instances: int
    :param reuse_gateways: If true, will leave provisioned instances running to be reused. You must run `skyplane deprovision` to clean up.
    :type reuse_gateways: bool
    :param max_chunk_size_mb: If set, `cp` will subdivide objects into chunks at most this size.
    :type max_chunk_size_mb: int
    :param confirm: If true, will not prompt for confirmation of transfer.
    :type confirm: bool
    :param debug: If true, will write debug info to debug directory
    :type debug: bool
    :param use_bbr: If set, will use BBR for transfers by default.
    :type use_bbr: bool
    :param use_compression: If set, will use compression for transfers by default.
    :type use_compression: bool
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

    src_client = ObjectStoreInterface.create(clouds[provider_src], bucket_src)
    src_region = src_client.region_tag()
    dst_client = ObjectStoreInterface.create(clouds[provider_dst], bucket_dst)
    dst_region = dst_client.region_tag()
    full_transfer_pairs = generate_full_transferobjlist(src_region, bucket_src, path_src, dst_region, bucket_dst, path_dst)

    # filter out any transfer pairs that are already in the destination
    transfer_pairs = []
    for src_obj, dst_obj in full_transfer_pairs:
        if not dst_obj.exists or (src_obj.last_modified > dst_obj.last_modified or src_obj.size != dst_obj.size):
            transfer_pairs.append((src_obj, dst_obj))

    if not transfer_pairs:
        typer.secho("No objects need updating. Exiting...")
        raise typer.Exit(0)

    topo = generate_topology(
        src_region,
        dst_region,
        solve,
        num_connections=num_connections,
        max_instances=max_instances,
        solver_total_gbyte_to_transfer=sum(src_obj.size for src_obj, _ in transfer_pairs) if solve else None,
        solver_required_throughput_gbits=solver_required_throughput_gbits,
        solver_throughput_grid=solver_throughput_grid,
        solver_verbose=solver_verbose,
    )

    job = ReplicationJob(
        source_region=topo.source_region(),
        source_bucket=bucket_src,
        dest_region=topo.sink_region(),
        dest_bucket=bucket_dst,
        transfer_pairs=transfer_pairs,
        max_chunk_size_mb=max_chunk_size_mb,
    )
    confirm_transfer(
        topo=topo,
        n_objs=len(transfer_pairs),
        est_size_bytes=job.transfer_size,
        ask_to_confirm_transfer=not cloud_config.get_flag("autoconfirm") and not confirm,
    )
    stats = launch_replication_job(
        topo=topo,
        job=job,
        debug=debug,
        reuse_gateways=reuse_gateways,
        use_bbr=use_bbr,
        use_compression=use_compression,
    )
    return 0 if stats["success"] else 1


@app.command()
def deprovision():
    """Deprovision all resources created by skyplane."""
    instances = query_instances()

    if instances:
        typer.secho(f"Deprovisioning {len(instances)} instances", fg="yellow", bold=True)
        do_parallel(lambda instance: instance.terminate_instance(), instances, desc="Deprovisioning", spinner=True, spinner_persist=True)
    else:
        typer.secho("No instances to deprovision", fg="yellow", bold=True)

    if AWSAuthentication().enabled():
        aws = AWSCloudProvider()
        # remove skyplane vpc
        vpcs = do_parallel(partial(aws.get_vpcs), aws.region_list(), desc="Querying VPCs", spinner=True)
        args = [(x[0], vpc.id) for x in vpcs for vpc in x[1]]
        do_parallel(lambda args: aws.remove_sg_ips(*args), args, desc="Removing IPs from VPCs", spinner=True, spinner_persist=True)
        # remove all instance profiles
        profiles = aws.list_instance_profiles(prefix="skyplane-aws")
        if profiles:
            do_parallel(aws.delete_instance_profile, profiles, desc="Deleting instance profiles", spinner=True, spinner_persist=True, n=4)


@app.command()
def ssh():
    """SSH into a running gateway."""
    instances = query_instances()
    if len(instances) == 0:
        typer.secho(f"No instances found", fg="red")
        raise typer.Abort()

    instance_map = {f"{i.region_tag}, {i.public_ip()} ({i.instance_state()})": i for i in instances}
    choices = list(sorted(instance_map.keys()))
    instance_name = questionary.select("Select an instance", choices=choices).ask()
    if instance_name is not None and instance_name in instance_map:
        instance = instance_map[instance_name]
        cmd = instance.get_ssh_cmd()
        logger.info(f"Running SSH command: {cmd}")
        logger.info("It may ask for a private key password, try `skyplane`.")
        proc = subprocess.Popen(split(cmd))
        proc.wait()
    else:
        typer.secho(f"No instance selected", fg="red")


@app.command()
def init(
    non_interactive: bool = typer.Option(False, "--non-interactive", "-y", help="Run non-interactively"),
    reinit_azure: bool = False,
    reinit_gcp: bool = False,
):
    """
    It loads the configuration file, and if it doesn't exist, it creates a default one. Then it creates
    AWS, Azure, and GCP region list configurations.

    :param reinit_azure: If true, will reinitialize the Azure region list and credentials
    :type reinit_azure: bool
    :param reinit_gcp: If true, will reinitialize the GCP region list and credentials
    :type reinit_gcp: bool
    """
    print_header()

    if non_interactive:
        logger.warning("Non-interactive mode enabled. Automatically confirming interactive questions.")

    if config_path.exists():
        cloud_config = SkyplaneConfig.load_config(config_path)
    else:
        cloud_config = SkyplaneConfig.default_config()

    # load AWS config
    typer.secho("\n(1) Configuring AWS:", fg="yellow", bold=True)
    cloud_config = load_aws_config(cloud_config, non_interactive=non_interactive)

    # load Azure config
    typer.secho("\n(2) Configuring Azure:", fg="yellow", bold=True)
    cloud_config = load_azure_config(cloud_config, force_init=reinit_azure, non_interactive=non_interactive)

    # load GCP config
    typer.secho("\n(3) Configuring GCP:", fg="yellow", bold=True)
    cloud_config = load_gcp_config(cloud_config, force_init=reinit_gcp, non_interactive=non_interactive)

    cloud_config.to_config_file(config_path)
    typer.secho(f"\nConfig file saved to {config_path}", fg="green")
    return 0


if __name__ == "__main__":
    app()
