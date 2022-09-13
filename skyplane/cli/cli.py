"""CLI for the Skyplane object store"""
import subprocess
from functools import partial
from pathlib import Path
from shlex import split
import traceback
import uuid

from rich import print as rprint

import skyplane.cli
import skyplane.cli.usage.definitions
import skyplane.cli.usage.client
from skyplane.cli.usage.client import UsageClient, UsageStatsStatus
from skyplane.replicate.replicator_client import ReplicatorClient

import typer
from rich.progress import Progress, SpinnerColumn, TextColumn

import skyplane.cli.cli_aws
import skyplane.cli.cli_azure
import skyplane.cli.cli_config
import skyplane.cli.cli_internal as cli_internal
import skyplane.cli.cli_solver
import skyplane.cli.experiments
from skyplane import config_path, exceptions, skyplane_root, cloud_config, tmp_log_dir
from skyplane.cli.common import print_header, console
from skyplane.cli.cli_impl.cp_replicate import (
    enrich_dest_objs,
    generate_full_transferobjlist,
    generate_topology,
    confirm_transfer,
    launch_replication_job,
)
from skyplane.replicate.replication_plan import ReplicationJob
from skyplane.cli.cli_impl.init import load_aws_config, load_azure_config, load_gcp_config
from skyplane.cli.common import parse_path, query_instances
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
def cp(
    src: str,
    dst: str,
    recursive: bool = typer.Option(False, "--recursive", "-r", help="If true, will copy objects at folder prefix recursively"),
    reuse_gateways: bool = typer.Option(False, help="If true, will leave provisioned instances running to be reused"),
    debug: bool = typer.Option(False, help="If true, will write debug information to debug directory."),
    multipart: bool = typer.Option(cloud_config.get_flag("multipart_enabled"), help="If true, will use multipart uploads."),
    # transfer flags
    confirm: bool = typer.Option(cloud_config.get_flag("autoconfirm"), "--confirm", "-y", "-f", help="Confirm all transfer prompts"),
    max_instances: int = typer.Option(cloud_config.get_flag("max_instances"), "--max-instances", "-n", help="Number of gateways"),
    # solver
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
    :param recursive: If true, will copy objects at folder prefix recursively
    :type recursive: bool
    :param reuse_gateways: If true, will leave provisioned instances running to be reused. You must run `skyplane deprovision` to clean up.
    :type reuse_gateways: bool
    :param debug: If true, will write debug information to debug directory.
    :type debug: bool
    :param multipart: If true, will use multipart uploads.
    :type multipart: bool
    :param confirm: If true, will not prompt for confirmation of transfer.
    :type confirm: bool
    :param max_instances: The maximum number of instances to use per region (default: 1)
    :type max_instances: int
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

    args = {
        "cmd": "cp",
        "recursive": recursive,
        "reuse_gateways": reuse_gateways,
        "debug": debug,
        "multipart": multipart,
        "confirm": confirm,
        "max_instances": max_instances,
        "solve": solve,
    }

    # check config
    try:
        cloud_config.check_config()
    except exceptions.BadConfigException:
        typer.secho(
            f"Skyplane configuration file is not valid. Please reset your config by running `rm {config_path}` and then rerunning `skyplane init` to fix.",
            fg="red",
        )
        raise typer.Exit(1)

    if provider_src == "local" or provider_dst == "local":
        typer.secho("Local transfers are not yet supported (but will be soon!)", fg="red", err=True)
        typer.secho("Skyplane is currently most optimized for cloud to cloud transfers.", fg="yellow", err=True)
        typer.secho(
            "Please provide feedback for on prem transfers at: https://github.com/skyplane-project/skyplane/discussions/424",
            fg="yellow",
            err=True,
        )
        raise typer.Exit(code=1)
    if provider_src in clouds and provider_dst in clouds:
        try:
            src_client = ObjectStoreInterface.create(clouds[provider_src], bucket_src)
            dst_client = ObjectStoreInterface.create(clouds[provider_dst], bucket_dst)
            src_region = src_client.region_tag()
            dst_region = dst_client.region_tag()

            transfer_pairs = generate_full_transferobjlist(
                src_region, bucket_src, path_src, dst_region, bucket_dst, path_dst, recursive=recursive
            )
        except exceptions.SkyplaneException as e:
            console.print(f"[bright_black]{traceback.format_exc()}[/bright_black]")
            console.print(e.pretty_print_str())

            client = UsageClient()
            src_region_tag = provider_src + ":" + bucket_src
            dst_region_tag = provider_dst + ":" + bucket_dst
            error_dict = {"loc": "create_pairs", "message": str(e)[:150]}
            stats = client.make_error(src_region_tag, dst_region_tag, error_dict, args)
            destination = client.write_usage_data(stats)
            client.report_usage_data("error", stats, destination)

            raise typer.Exit(1)

        if multipart and (provider_src == "azure" or provider_dst == "azure"):
            typer.secho(
                "Warning: Azure is not yet supported for multipart transfers, you may observe slow performance", fg="yellow", err=True
            )
            multipart = False

        topo = generate_topology(
            src_region,
            dst_region,
            solve,
            num_connections=cloud_config.get_flag("num_connections"),
            max_instances=max_instances,
            solver_total_gbyte_to_transfer=sum(src_obj.size for src_obj, _ in transfer_pairs) if solve else None,
            solver_required_throughput_gbits=solver_required_throughput_gbits,
            solver_throughput_grid=solver_throughput_grid,
            solver_verbose=solver_verbose,
            args=args,
        )
        job = ReplicationJob(
            source_region=topo.source_region(),
            source_bucket=bucket_src,
            dest_region=topo.sink_region(),
            dest_bucket=bucket_dst,
            transfer_pairs=transfer_pairs,
        )
        confirm_transfer(topo=topo, job=job, ask_to_confirm_transfer=not confirm)

        transfer_stats = launch_replication_job(
            topo=topo,
            job=job,
            debug=debug,
            reuse_gateways=reuse_gateways,
            use_bbr=cloud_config.get_flag("bbr"),
            use_compression=cloud_config.get_flag("compress") if src_region != dst_region else False,
            use_e2ee=cloud_config.get_flag("encrypt_e2e") if src_region != dst_region else False,
            use_socket_tls=cloud_config.get_flag("encrypt_socket_tls") if src_region != dst_region else False,
            aws_instance_class=cloud_config.get_flag("aws_instance_class"),
            aws_use_spot_instances=cloud_config.get_flag("aws_use_spot_instances"),
            azure_instance_class=cloud_config.get_flag("azure_instance_class"),
            azure_use_spot_instances=cloud_config.get_flag("azure_use_spot_instances"),
            gcp_instance_class=cloud_config.get_flag("gcp_instance_class"),
            gcp_use_premium_network=cloud_config.get_flag("gcp_use_premium_network"),
            gcp_use_spot_instances=cloud_config.get_flag("gcp_use_spot_instances"),
            multipart_enabled=multipart,
            multipart_min_threshold_mb=cloud_config.get_flag("multipart_min_threshold_mb"),
            multipart_min_size_mb=cloud_config.get_flag("multipart_min_size_mb"),
            multipart_max_chunks=cloud_config.get_flag("multipart_max_chunks"),
            error_reporting_args=args,
        )

        if cloud_config.get_flag("verify_checksums"):
            provider_dst = topo.sink_region().split(":")[0]
            with Progress(SpinnerColumn(), TextColumn("Verifying all files were copied{task.description}")) as progress:
                progress.add_task("", total=None)
                try:
                    ReplicatorClient.verify_transfer_prefix(dest_prefix=path_dst, job=job)
                except exceptions.TransferFailedException as e:
                    console.print(f"[bright_black]{traceback.format_exc()}[/bright_black]")
                    console.print(e.pretty_print_str())

                    client = UsageClient()
                    src_region_tag = provider_src + ":" + bucket_src
                    dst_region_tag = provider_dst + ":" + bucket_dst
                    error_dict = {"loc": "create_pairs", "message": str(e)[:150]}
                    stats = client.make_error(src_region_tag, dst_region_tag, error_dict, args)
                    destination = client.write_usage_data(stats)
                    client.report_usage_data("error", stats, destination)
                    raise typer.Exit(1)

        if transfer_stats.monitor_status == "completed":
            rprint(f"\n:white_check_mark: [bold green]Transfer completed successfully[/bold green]")
            runtime_line = f"[white]Transfer runtime:[/white] [bright_black]{transfer_stats.total_runtime_s:.2f}s[/bright_black]"
            throughput_line = f"[white]Throughput:[/white] [bright_black]{transfer_stats.throughput_gbits:.2f}Gbps[/bright_black]"
            rprint(f"{runtime_line}, {throughput_line}")

        client = UsageClient()
        if client.enabled():
            if transfer_stats.monitor_status == "completed":
                stats = client.make_stat(src_region, dst_region, arguments_dict=args, transfer_stats=transfer_stats)
                destination = client.write_usage_data(stats)
                client.report_usage_data("usage", stats, destination)
        return 0 if transfer_stats.monitor_status == "completed" else 1
    else:
        raise NotImplementedError(f"{provider_src} to {provider_dst} not supported yet")


@app.command()
def sync(
    src: str,
    dst: str,
    recursive: bool = typer.Option(False, "--recursive", "-r", help="If true, will copy objects at folder prefix recursively"),
    reuse_gateways: bool = typer.Option(False, help="If true, will leave provisioned instances running to be reused"),
    debug: bool = typer.Option(False, help="If true, will write debug information to debug directory."),
    # transfer flags
    confirm: bool = typer.Option(cloud_config.get_flag("autoconfirm"), "--confirm", "-y", "-f", help="Confirm all transfer prompts"),
    max_instances: int = typer.Option(cloud_config.get_flag("max_instances"), "--max-instances", "-n", help="Number of gateways"),
    multipart: bool = typer.Option(cloud_config.get_flag("multipart_enabled"), help="If true, will use multipart uploads."),
    # solver
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
    :param recursive: If true, will copy objects at folder prefix recursively
    :type recursive: bool
    :param reuse_gateways: If true, will leave provisioned instances running to be reused. You must run `skyplane deprovision` to clean up.
    :type reuse_gateways: bool
    :param debug: If true, will write debug information to debug directory.
    :type debug: bool
    :param multipart: If true, will use multipart uploads.
    :type multipart: bool
    :param confirm: If true, will not prompt for confirmation of transfer.
    :type confirm: bool
    :param max_instances: The maximum number of instances to use per region (default: 1)
    :type max_instances: int
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

    args = {
        "cmd": "sync",
        "recursive": recursive,
        "reuse_gateways": reuse_gateways,
        "debug": debug,
        "multipart": multipart,
        "confirm": confirm,
        "max_instances": max_instances,
        "solve": solve,
    }

    # check config
    try:
        cloud_config.check_config()
    except exceptions.BadConfigException:
        typer.secho(
            f"Skyplane configuration file is not valid. Please reset your config by running `rm {config_path}` and then rerunning `skyplane init` to fix.",
            fg="red",
        )
        raise typer.Exit(1)

    try:
        src_client = ObjectStoreInterface.create(clouds[provider_src], bucket_src)
        src_region = src_client.region_tag()
        dst_client = ObjectStoreInterface.create(clouds[provider_dst], bucket_dst)
        dst_region = dst_client.region_tag()
        full_transfer_pairs = generate_full_transferobjlist(
            src_region, bucket_src, path_src, dst_region, bucket_dst, path_dst, recursive=recursive
        )
    except exceptions.SkyplaneException as e:
        console.print(f"[bright_black]{traceback.format_exc()}[/bright_black]")
        console.print(e.pretty_print_str())

        client = UsageClient()
        src_region_tag = provider_src + ":" + bucket_src
        dst_region_tag = provider_dst + ":" + bucket_dst
        error_dict = {"loc": "create_pairs", "message": str(e)[:150]}
        stats = client.make_error(src_region_tag, dst_region_tag, error_dict, args)
        destination = client.write_usage_data(stats)
        client.report_usage_data("error", stats, destination)

        raise typer.Exit(1)

    enrich_dest_objs(dst_region, path_dst, bucket_dst, [i[1] for i in full_transfer_pairs])

    # filter out any transfer pairs that are already in the destination
    transfer_pairs = []
    for src_obj, dst_obj in full_transfer_pairs:
        if not dst_obj.exists or (src_obj.last_modified > dst_obj.last_modified or src_obj.size != dst_obj.size):
            transfer_pairs.append((src_obj, dst_obj))

    if not transfer_pairs:
        err = "No objects need updating. Exiting..."
        typer.secho(err)

        client = UsageClient()
        error_dict = {"loc": "create_pairs", "message": err}
        stats = client.make_error(src_region, dst_region, error_dict, args)
        destination = client.write_usage_data(stats)
        client.report_usage_data("error", stats, destination)

        raise typer.Exit(0)

    if multipart and (provider_src == "azure" or provider_dst == "azure"):
        typer.secho("Warning: Azure is not yet supported for multipart transfers, you may observe slow performance", fg="yellow", err=True)
        multipart = False

    topo = generate_topology(
        src_region,
        dst_region,
        solve,
        num_connections=cloud_config.get_flag("num_connections"),
        max_instances=max_instances,
        solver_total_gbyte_to_transfer=sum(src_obj.size for src_obj, _ in transfer_pairs) if solve else None,
        solver_required_throughput_gbits=solver_required_throughput_gbits,
        solver_throughput_grid=solver_throughput_grid,
        solver_verbose=solver_verbose,
        args=args,
    )

    job = ReplicationJob(
        source_region=topo.source_region(),
        source_bucket=bucket_src,
        dest_region=topo.sink_region(),
        dest_bucket=bucket_dst,
        transfer_pairs=transfer_pairs,
    )
    confirm_transfer(topo=topo, job=job, ask_to_confirm_transfer=not confirm)
    transfer_stats = launch_replication_job(
        topo=topo,
        job=job,
        debug=debug,
        reuse_gateways=reuse_gateways,
        use_bbr=cloud_config.get_flag("bbr"),
        use_compression=cloud_config.get_flag("compress") if src_region != dst_region else False,
        use_e2ee=cloud_config.get_flag("encrypt_e2e") if src_region != dst_region else False,
        use_socket_tls=cloud_config.get_flag("encrypt_socket_tls") if src_region != dst_region else False,
        aws_instance_class=cloud_config.get_flag("aws_instance_class"),
        aws_use_spot_instances=cloud_config.get_flag("aws_use_spot_instances"),
        azure_instance_class=cloud_config.get_flag("azure_instance_class"),
        azure_use_spot_instances=cloud_config.get_flag("azure_use_spot_instances"),
        gcp_instance_class=cloud_config.get_flag("gcp_instance_class"),
        gcp_use_premium_network=cloud_config.get_flag("gcp_use_premium_network"),
        gcp_use_spot_instances=cloud_config.get_flag("gcp_use_spot_instances"),
        multipart_enabled=multipart,
        multipart_min_threshold_mb=cloud_config.get_flag("multipart_min_threshold_mb"),
        multipart_min_size_mb=cloud_config.get_flag("multipart_min_size_mb"),
        multipart_max_chunks=cloud_config.get_flag("multipart_max_chunks"),
        error_reporting_args=args,
    )

    if cloud_config.get_flag("verify_checksums"):
        provider_dst = topo.sink_region().split(":")[0]
        if provider_dst == "azure":
            typer.secho("Note: Azure post-transfer verification is not yet supported.", fg="yellow", bold=True, err=True)
        else:
            with Progress(SpinnerColumn(), TextColumn("Verifying all files were copied{task.description}"), transient=True) as progress:
                progress.add_task("", total=None)
                try:
                    ReplicatorClient.verify_transfer_prefix(dest_prefix=path_dst, job=job)
                except exceptions.TransferFailedException as e:
                    console.print(f"[bright_black]{traceback.format_exc()}[/bright_black]")
                    console.print(e.pretty_print_str())

                    client = UsageClient()
                    src_region_tag = provider_src + ":" + bucket_src
                    dst_region_tag = provider_dst + ":" + bucket_dst
                    error_dict = {"loc": "create_pairs", "message": str(e)[:150]}
                    stats = client.make_error(src_region_tag, dst_region_tag, error_dict, args)
                    destination = client.write_usage_data(stats)
                    client.report_usage_data("error", stats, destination)
                    raise typer.Exit(1)

    if transfer_stats.monitor_status == "completed":
        rprint(f"\n:white_check_mark: [bold green]Transfer completed successfully[/bold green]")
        runtime_line = f"[white]Transfer runtime:[/white] [bright_black]{transfer_stats.total_runtime_s:.2f}s[/bright_black]"
        throughput_line = f"[white]Throughput:[/white] [bright_black]{transfer_stats.throughput_gbits:.2f}Gbps[/bright_black]"
        rprint(f"{runtime_line}, {throughput_line}")

    client = UsageClient()
    if client.enabled():
        if transfer_stats.monitor_status == "completed":
            stats = client.make_stat(src_region, dst_region, arguments_dict=args, transfer_stats=transfer_stats)
            destination = client.write_usage_data(stats)
            client.report_usage_data("usage", stats, destination)
    return 0 if transfer_stats.monitor_status == "completed" else 1


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
        typer.secho(f"No instances found", fg="red", err=True)
        raise typer.Abort()

    instance_map = {f"{i.region_tag}, {i.public_ip()} ({i.instance_state()})": i for i in instances}
    choices = list(sorted(instance_map.keys()))

    # ask for selection
    typer.secho("Select an instance:", fg="yellow", bold=True)
    for i, choice in enumerate(choices):
        typer.secho(f"{i+1}) {choice}", fg="yellow")
    choice = typer.prompt(f"Enter a number: ", validators=[typer.Range(1, len(choices))])
    instance = instance_map[choices[choice - 1]]

    # ssh
    cmd = instance.get_ssh_cmd()
    logger.info(f"Running SSH command: {cmd}")
    logger.info("It may ask for a private key password, try `skyplane`.")
    proc = subprocess.Popen(split(cmd))
    proc.wait()


@app.command()
def init(
    non_interactive: bool = typer.Option(False, "--non-interactive", "-y", help="Run non-interactively"),
    reinit_azure: bool = False,
    reinit_gcp: bool = False,
    disable_config_aws: bool = False,
    disable_config_azure: bool = False,
    disable_config_gcp: bool = False,
):
    """
    It loads the configuration file, and if it doesn't exist, it creates a default one. Then it creates
    AWS, Azure, and GCP region list configurations.

    :param reinit_azure: If true, will reinitialize the Azure region list and credentials
    :type reinit_azure: bool
    :param reinit_gcp: If true, will reinitialize the GCP region list and credentials
    :type reinit_gcp: bool
    :param disable_config_aws: If true, will disable AWS configuration (may still be enabled if environment variables are set)
    :type disable_config_aws: bool
    :param disable_config_azure: If true, will disable Azure configuration (may still be enabled if environment variables are set)
    :type disable_config_azure: bool
    :param disable_config_gcp: If true, will disable GCP configuration (may still be enabled if environment variables are set)
    :type disable_config_gcp: bool
    """
    print_header()

    if non_interactive:
        logger.warning("Non-interactive mode enabled. Automatically confirming interactive questions.")

    if config_path.exists():
        logger.debug(f"Found existing configuration file at {config_path}, loading")
        cloud_config = SkyplaneConfig.load_config(config_path)
    else:
        cloud_config = SkyplaneConfig.default_config()

    # create client_id
    if cloud_config.anon_clientid is None:
        cloud_config.anon_clientid = str(uuid.uuid4())

    # load AWS config
    typer.secho("\n(1) Configuring AWS:", fg="yellow", bold=True)
    if not disable_config_aws:
        cloud_config = load_aws_config(cloud_config, non_interactive=non_interactive)

    # load Azure config
    typer.secho("\n(2) Configuring Azure:", fg="yellow", bold=True)
    if not disable_config_azure:
        cloud_config = load_azure_config(cloud_config, force_init=reinit_azure, non_interactive=non_interactive)

    # load GCP config
    typer.secho("\n(3) Configuring GCP:", fg="yellow", bold=True)
    if not disable_config_gcp:
        cloud_config = load_gcp_config(cloud_config, force_init=reinit_gcp, non_interactive=non_interactive)

    cloud_config.to_config_file(config_path)
    typer.secho(f"\nConfig file saved to {config_path}", fg="green")

    # Set metrics collection by default
    print("\n")
    usage_stats_var = UsageClient.usage_stats_status()
    if usage_stats_var is UsageStatsStatus.DISABLED_EXPLICITLY:
        rprint(skyplane.cli.usage.definitions.USAGE_STATS_DISABLED_MESSAGE)
    elif usage_stats_var in [UsageStatsStatus.ENABLED_BY_DEFAULT, UsageStatsStatus.ENABLED_EXPLICITLY]:
        rprint(skyplane.cli.usage.definitions.USAGE_STATS_ENABLED_MESSAGE)
    else:
        raise Exception("Prompt message unknown.")
    return 0


typer_click_object = typer.main.get_command(app)

if __name__ == "__main__":
    app()
