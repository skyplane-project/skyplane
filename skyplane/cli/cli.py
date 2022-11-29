import os
import subprocess
import time
import traceback
from importlib.resources import path
from shlex import split

import typer
from rich import print as rprint
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.prompt import IntPrompt
from typing import Optional

import skyplane.cli
import skyplane.cli
import skyplane.cli.cli_cloud
import skyplane.cli.cli_config
import skyplane.cli.cli_internal as cli_internal
import skyplane.cli.experiments
import skyplane.cli.usage.client
import skyplane.cli.usage.client
import skyplane.cli.usage.definitions
import skyplane.cli.usage.definitions
from skyplane import compute
from skyplane import exceptions
from skyplane.utils.path import parse_path
from skyplane.cli.cli_impl.cp_replicate import (
    confirm_transfer,
    enrich_dest_objs,
    generate_full_transferobjlist,
    generate_topology,
    launch_replication_job,
)
from skyplane.cli.cli_impl.cp_replicate_fallback import (
    replicate_onprem_cp_cmd,
    replicate_onprem_sync_cmd,
    replicate_small_cp_cmd,
    replicate_small_sync_cmd,
)
from skyplane.cli.cli_impl.init import load_aws_config, load_azure_config, load_gcp_config
from skyplane.cli.common import console, print_header, print_stats_completed, query_instances
from skyplane.cli.usage.client import UsageClient, UsageStatsStatus
from skyplane.config import SkyplaneConfig
from skyplane.config_paths import config_path, cloud_config
from skyplane.obj_store.object_store_interface import ObjectStoreInterface
from skyplane.replicate.replication_plan import ReplicationJob
from skyplane.replicate.replicator_client import ReplicatorClient, TransferStats
from skyplane.utils import logger
from skyplane.utils.definitions import GB
from skyplane.utils.fn import do_parallel

app = typer.Typer(name="skyplane")
app.command()(cli_internal.replicate_random)
app.add_typer(skyplane.cli.experiments.app, name="experiments")
app.add_typer(skyplane.cli.cli_cloud.app, name="cloud")
app.add_typer(skyplane.cli.cli_config.app, name="config")


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
    solver_target_tput_per_vm_gbits: float = typer.Option(4, help="Solver option: Required throughput in Gbps"),
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
    :param throughput_per_instance_gbits: The required throughput in Gbps when using the solver (default: 4)
    :type throughput_per_instance_gbits: float
    :param solver_throughput_grid: The throughput grid profile to use for the solver, defaults to author-provided profile
    :type solver_throughput_grid: Path
    :param solver_verbose: If true, will print out the solver's output, defaults to False
    :type solver_verbose: bool (optional)
    """
    print_header()

    provider_src, bucket_src, path_src = parse_path(src)
    provider_dst, bucket_dst, path_dst = parse_path(dst)
    src_region_tag, dst_region_tag = f"{provider_src}:infer", f"{provider_dst}:infer"
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
    except exceptions.BadConfigException as e:
        typer.secho(
            f"Skyplane configuration file is not valid. Please reset your config by running `rm {config_path}` and then rerunning `skyplane init` to fix.",
            fg="red",
        )
        UsageClient.log_exception("cli_check_config", e, args, src_region_tag, dst_region_tag)
        return 1

    if provider_src in ("local", "hdfs", "nfs") or provider_dst in ("local", "hdfs", "nfs"):
        if provider_src == "hdfs" or provider_dst == "hdfs":
            typer.secho("HDFS is not supported yet.", fg="red")
            return 1
        cmd = replicate_onprem_cp_cmd(src, dst, recursive)
        if cmd:
            typer.secho(f"Delegating to: {cmd}", fg="yellow")
            start = time.perf_counter()
            rc = os.system(cmd)
            request_time = time.perf_counter() - start
            # print stats - we do not measure throughput for on-prem
            if not rc:
                print_stats_completed(request_time, 0)
                transfer_stats = TransferStats(monitor_status="completed", total_runtime_s=request_time, throughput_gbits=0)
                UsageClient.log_transfer(transfer_stats, args, src_region_tag, dst_region_tag)
            return 0
        else:
            typer.secho("Transfer not supported", fg="red")
            return 1

    elif provider_src in ("aws", "gcp", "azure") and provider_dst in ("aws", "gcp", "azure"):
        try:
            src_client = ObjectStoreInterface.create(src_region_tag, bucket_src)
            dst_client = ObjectStoreInterface.create(dst_region_tag, bucket_dst)
            src_region_tag = src_client.region_tag()
            dst_region_tag = dst_client.region_tag()
            if cloud_config.get_flag("requester_pays"):
                src_client.set_requester_bool(True)
                dst_client.set_requester_bool(True)
            transfer_pairs = generate_full_transferobjlist(
                src_region_tag, bucket_src, path_src, dst_region_tag, bucket_dst, path_dst, recursive=recursive
            )
        except exceptions.SkyplaneException as e:
            console.print(f"[bright_black]{traceback.format_exc()}[/bright_black]")
            console.print(e.pretty_print_str())
            UsageClient.log_exception("cli_query_objstore", e, args, src_region_tag, dst_region_tag)
            return 1

        if multipart and (provider_src == "azure" or provider_dst == "azure"):
            typer.secho("Warning: Azure is not yet supported for multipart transfers. Disabling multipart.", fg="yellow", err=True)
            multipart = False

        with path("skyplane.data", "throughput.csv") as throughput_grid_path:
            topo = generate_topology(
                src_region_tag,
                dst_region_tag,
                solve,
                num_connections=cloud_config.get_flag("num_connections"),
                max_instances=max_instances,
                solver_total_gbyte_to_transfer=sum(src_obj.size for src_obj, _ in transfer_pairs) if solve else None,
                solver_target_tput_per_vm_gbits=solver_target_tput_per_vm_gbits,
                solver_throughput_grid=throughput_grid_path,
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

        small_transfer_cmd = replicate_small_cp_cmd(src, dst, recursive)
        if (
            cloud_config.get_flag("native_cmd_enabled")
            and (job.transfer_size / GB) < cloud_config.get_flag("native_cmd_threshold_gb")
            and small_transfer_cmd
        ):
            typer.secho(
                f"Transfer of {job.transfer_size / GB} is small enough to delegate to native tools. Delegating to: {small_transfer_cmd}",
                fg="yellow",
            )
            typer.secho(f"You can change this by `skyplane config set native_cmd_threshold_gb <value>`")
            os.system(small_transfer_cmd)
            return 0
        else:
            transfer_stats = launch_replication_job(
                topo=topo,
                job=job,
                debug=debug,
                reuse_gateways=reuse_gateways,
                use_bbr=cloud_config.get_flag("bbr"),
                use_compression=cloud_config.get_flag("compress") if src_region_tag != dst_region_tag else False,
                use_e2ee=cloud_config.get_flag("encrypt_e2e") if src_region_tag != dst_region_tag else False,
                use_socket_tls=cloud_config.get_flag("encrypt_socket_tls") if src_region_tag != dst_region_tag else False,
                aws_instance_class=cloud_config.get_flag("aws_instance_class"),
                aws_use_spot_instances=cloud_config.get_flag("aws_use_spot_instances"),
                azure_instance_class=cloud_config.get_flag("azure_instance_class"),
                azure_use_spot_instances=cloud_config.get_flag("azure_use_spot_instances"),
                gcp_instance_class=cloud_config.get_flag("gcp_instance_class"),
                gcp_use_premium_network=cloud_config.get_flag("gcp_use_premium_network"),
                gcp_use_spot_instances=cloud_config.get_flag("gcp_use_spot_instances"),
                multipart_enabled=multipart,
                multipart_min_threshold_mb=cloud_config.get_flag("multipart_min_threshold_mb"),
                multipart_chunk_size_mb=cloud_config.get_flag("multipart_chunk_size_mb"),
                multipart_max_chunks=cloud_config.get_flag("multipart_max_chunks"),
                error_reporting_args=args,
                host_uuid=cloud_config.anon_clientid,
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
                        UsageClient.log_exception("cli_verify_checksums", e, args, src_region_tag, dst_region_tag)
                        return 1
            if transfer_stats.monitor_status == "completed":
                print_stats_completed(transfer_stats.total_runtime_s, transfer_stats.throughput_gbits)
            UsageClient.log_transfer(transfer_stats, args, src_region_tag, dst_region_tag)
            return 0 if transfer_stats.monitor_status == "completed" else 1
    else:
        raise NotImplementedError(f"{provider_src} to {provider_dst} not supported yet")


@app.command()
def sync(
    src: str,
    dst: str,
    reuse_gateways: bool = typer.Option(False, help="If true, will leave provisioned instances running to be reused"),
    debug: bool = typer.Option(False, help="If true, will write debug information to debug directory."),
    # transfer flags
    confirm: bool = typer.Option(cloud_config.get_flag("autoconfirm"), "--confirm", "-y", "-f", help="Confirm all transfer prompts"),
    max_instances: int = typer.Option(cloud_config.get_flag("max_instances"), "--max-instances", "-n", help="Number of gateways"),
    multipart: bool = typer.Option(cloud_config.get_flag("multipart_enabled"), help="If true, will use multipart uploads."),
    # solver
    solve: bool = typer.Option(False, help="If true, will use solver to optimize transfer, else direct path is chosen"),
    solver_target_tput_per_vm_gbits: float = typer.Option(4, help="Solver option: Required throughput in Gbps per instance"),
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
    :param throughput_per_instance_gbits: The required throughput in Gbps per instance when using the solver (default: 4)
    :type throughput_per_instance_gbits: float
    :param solver_throughput_grid: The throughput grid profile to use for the solver, defaults to author-provided profile
    :type solver_throughput_grid: Path
    :param solver_verbose: If true, will print out the solver's output, defaults to False
    :type solver_verbose: bool (optional)
    """

    print_header()

    provider_src, bucket_src, path_src = parse_path(src)
    provider_dst, bucket_dst, path_dst = parse_path(dst)
    src_region_tag, dst_region_tag = f"{provider_src}:infer", f"{provider_dst}:infer"
    args = {
        "cmd": "sync",
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
    except exceptions.BadConfigException as e:
        typer.secho(
            f"Skyplane configuration file is not valid. Please reset your config by running `rm {config_path}` and then rerunning `skyplane init` to fix.",
            fg="red",
        )
        UsageClient.log_exception("cli_check_config", e, args, src_region_tag, dst_region_tag)
        return 1

    if provider_src == "local" or provider_dst == "local":
        cmd = replicate_onprem_sync_cmd(src, dst)
        if cmd:
            typer.secho(f"Delegating to: {cmd}", fg="yellow")
            os.system(cmd)
            return 0
        else:
            typer.secho("Transfer not supported", fg="red")
            return 1
    elif provider_src in ["aws", "gcp", "azure"] and provider_dst in ["aws", "gcp", "azure"]:
        try:
            src_client = ObjectStoreInterface.create(src_region_tag, bucket_src)
            src_region_tag = src_client.region_tag()
            dst_client = ObjectStoreInterface.create(dst_region_tag, bucket_dst)
            dst_region_tag = dst_client.region_tag()
            full_transfer_pairs = generate_full_transferobjlist(
                src_region_tag, bucket_src, path_src, dst_region_tag, bucket_dst, path_dst, recursive=True
            )
            enrich_dest_objs(dst_region_tag, path_dst, bucket_dst, [i[1] for i in full_transfer_pairs])
        except exceptions.SkyplaneException as e:
            console.print(f"[bright_black]{traceback.format_exc()}[/bright_black]")
            console.print(e.pretty_print_str())
            UsageClient.log_exception("cli_query_objstore", e, args, src_region_tag, dst_region_tag)
            return 1

        # filter out any transfer pairs that are already in the destination
        transfer_pairs = []
        for src_obj, dst_obj in full_transfer_pairs:
            if not dst_obj.exists or (src_obj.last_modified > dst_obj.last_modified or src_obj.size != dst_obj.size):
                transfer_pairs.append((src_obj, dst_obj))

        if not transfer_pairs:
            err = "No objects need updating. Exiting..."
            typer.secho(err)
            return 0

        if multipart and (provider_src == "azure" or provider_dst == "azure"):
            typer.secho("Warning: Azure is not yet supported for multipart transfers. Disabling multipart.", fg="yellow", err=True)
            multipart = False

        with path("skyplane.data", "throughput.csv") as throughput_grid_path:
            topo = generate_topology(
                src_region_tag,
                dst_region_tag,
                solve,
                num_connections=cloud_config.get_flag("num_connections"),
                max_instances=max_instances,
                solver_total_gbyte_to_transfer=sum(src_obj.size for src_obj, _ in transfer_pairs) if solve else None,
                solver_target_tput_per_vm_gbits=solver_target_tput_per_vm_gbits,
                solver_throughput_grid=throughput_grid_path,
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

        small_transfer_cmd = replicate_small_sync_cmd(src, dst)
        if (
            cloud_config.get_flag("native_cmd_enabled")
            and (job.transfer_size / GB) < cloud_config.get_flag("native_cmd_threshold_gb")
            and small_transfer_cmd
        ):
            typer.secho(f"Transfer is small enough to delegate to native tools. Delegating to: {small_transfer_cmd}", fg="yellow")
            os.system(small_transfer_cmd)
            return 0
        else:
            transfer_stats = launch_replication_job(
                topo=topo,
                job=job,
                debug=debug,
                reuse_gateways=reuse_gateways,
                use_bbr=cloud_config.get_flag("bbr"),
                use_compression=cloud_config.get_flag("compress") if src_region_tag != dst_region_tag else False,
                use_e2ee=cloud_config.get_flag("encrypt_e2e") if src_region_tag != dst_region_tag else False,
                use_socket_tls=cloud_config.get_flag("encrypt_socket_tls") if src_region_tag != dst_region_tag else False,
                aws_instance_class=cloud_config.get_flag("aws_instance_class"),
                aws_use_spot_instances=cloud_config.get_flag("aws_use_spot_instances"),
                azure_instance_class=cloud_config.get_flag("azure_instance_class"),
                azure_use_spot_instances=cloud_config.get_flag("azure_use_spot_instances"),
                gcp_instance_class=cloud_config.get_flag("gcp_instance_class"),
                gcp_use_premium_network=cloud_config.get_flag("gcp_use_premium_network"),
                gcp_use_spot_instances=cloud_config.get_flag("gcp_use_spot_instances"),
                multipart_enabled=multipart,
                multipart_min_threshold_mb=cloud_config.get_flag("multipart_min_threshold_mb"),
                multipart_chunk_size_mb=cloud_config.get_flag("multipart_chunk_size_mb"),
                multipart_max_chunks=cloud_config.get_flag("multipart_max_chunks"),
                error_reporting_args=args,
                host_uuid=cloud_config.anon_clientid,
            )
            if cloud_config.get_flag("verify_checksums"):
                provider_dst = topo.sink_region().split(":")[0]
                if provider_dst == "azure":
                    typer.secho("Note: Azure post-transfer verification is not yet supported.", fg="yellow", bold=True, err=True)
                else:
                    with Progress(
                        SpinnerColumn(), TextColumn("Verifying all files were copied{task.description}"), transient=True
                    ) as progress:
                        progress.add_task("", total=None)
                        try:
                            ReplicatorClient.verify_transfer_prefix(dest_prefix=path_dst, job=job)
                        except exceptions.TransferFailedException as e:
                            console.print(f"[bright_black]{traceback.format_exc()}[/bright_black]")
                            console.print(e.pretty_print_str())
                            UsageClient.log_exception("cli_verify_checksums", e, args, src_region_tag, dst_region_tag)
                            return 1
            if transfer_stats.monitor_status == "completed":
                rprint(f"\n:white_check_mark: [bold green]Transfer completed successfully[/bold green]")
                runtime_line = f"[white]Transfer runtime:[/white] [bright_black]{transfer_stats.total_runtime_s:.2f}s[/bright_black]"
                throughput_line = f"[white]Throughput:[/white] [bright_black]{transfer_stats.throughput_gbits:.2f}Gbps[/bright_black]"
                rprint(f"{runtime_line}, {throughput_line}")
            UsageClient.log_transfer(transfer_stats, args, src_region_tag, dst_region_tag)
            return 0 if transfer_stats.monitor_status == "completed" else 1
    else:
        raise NotImplementedError(f"{provider_src} to {provider_dst} not supported yet")


@app.command()
def deprovision(
    all: bool = typer.Option(False, "--all", "-a", help="Deprovision all resources including networks."),
    filter_client_id: Optional[str] = typer.Option(None, help="Only deprovision instances with this client ID under the instance tag."),
):
    """Deprovision all resources created by skyplane."""
    instances = query_instances()
    if filter_client_id:
        instances = [instance for instance in instances if instance.tags().get("skyplaneclientid") == filter_client_id]

    if instances:
        typer.secho(f"Deprovisioning {len(instances)} instances", fg="yellow", bold=True)
        do_parallel(lambda instance: instance.terminate_instance(), instances, desc="Deprovisioning", spinner=True, spinner_persist=True)
    else:
        typer.secho("No instances to deprovision", fg="yellow", bold=True)

    if all:
        if compute.AWSAuthentication().enabled():
            aws = compute.AWSCloudProvider()
            aws.teardown_global()
        if compute.GCPAuthentication().enabled():
            gcp = compute.GCPCloudProvider()
            gcp.teardown_global()
        if compute.AzureAuthentication().enabled():
            azure = compute.AzureCloudProvider()
            azure.teardown_global()


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
    choice = IntPrompt.ask("Enter an instance number", choices=list([str(i) for i in range(1, len(choices) + 1)]), show_choices=False)
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
