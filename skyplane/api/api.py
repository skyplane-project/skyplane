"""API for the Skyplane object store"""
import subprocess
from functools import partial
from pathlib import Path
from shlex import split
from tkinter import ON
import traceback
import uuid
import os

from rich import print as rprint

import skyplane.cli
import skyplane.cli.usage.definitions
import skyplane.cli.usage.client
from skyplane import GB
from skyplane.cli.usage.client import UsageClient, UsageStatsStatus
from skyplane.replicate.replicator_client import ReplicatorClient

import typer
from rich.progress import Progress, SpinnerColumn, TextColumn

import skyplane.cli.cli_aws
import skyplane.cli.cli_azure
import skyplane.cli.cli_config
import skyplane.cli.cli_internal as cli_internal
import skyplane.cli.experiments
from skyplane import cloud_config, config_path, exceptions, skyplane_root
from skyplane.cli.common import print_header, console
from skyplane.cli.cli_impl.cp_replicate import (
    enrich_dest_objs,
    generate_full_transferobjlist,
    generate_topology,
    confirm_transfer,
    launch_replication_job,
)
from skyplane.cli.cli_impl.cp_replicate_fallback import (
    replicate_onprem_cp_cmd,
    replicate_onprem_sync_cmd,
    replicate_small_cp_cmd,
    replicate_small_sync_cmd,
)
from skyplane.replicate.replication_plan import ReplicationJob
from skyplane.cli.cli_impl.init import load_aws_config, load_azure_config, load_gcp_config
from skyplane.cli.common import parse_path, query_instances
from skyplane.compute.aws.aws_auth_provider import AWSAuthenticationProvider
from skyplane.compute.aws.aws_cloud_provider import AWSCloudProvider
from skyplane.config import SkyplaneConfig
from skyplane.config import _DEFAULT_FLAGS as config_default_flags
from skyplane.obj_store.object_store_interface import ObjectStoreInterface
from skyplane.utils import logger
from skyplane.utils.fn import do_parallel
import asyncio

async def cp(
    src: str,
    src_client: ObjectStoreInterface,
    dst: str,
    dst_client: ObjectStoreInterface,
    recursive: bool = False,
    reuse_gateways: bool = True, # All in the session and will be deprovisioned after
    debug: bool = False,
    multipart: bool = config_default_flags.get("multipart_enabled"),
    # transfer flags
    confirm: bool = True,
    max_instances: int = config_default_flags.get("max_instances"),
    # solver
    solve: bool = False, # use direct path
    solver_class: str = "ILP", # used only if solve = True
    solver_target_tput_per_vm_gbits: float = 4,
    solver_throughput_grid: Path = Path(skyplane_root / "profiles" / "throughput.csv"),
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
    # TODO: What do we deal with printing the prompts? Log it instead
    # print_header()

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

    # There is no cloud_config in API
    # TODO: anon_client_id? Use fixed MAC Address

    if provider_src == "local" or provider_dst == "local":
        cmd = replicate_onprem_cp_cmd(src, dst, recursive)
        if cmd:
            # typer.secho(f"Delegating to: {cmd}", fg="yellow")
            os.system(cmd)
            return 0
        else:
            # typer.secho("Transfer not supported", fg="red")
            return 1
    elif provider_src in ["aws", "gcp", "azure"] and provider_dst in ["aws", "gcp", "azure"]:
        try:
            src_region_tag = src_client.region_tag()
            dst_region_tag = dst_client.region_tag()
            requester_pays = config_default_flags.get("requester_pays")
            if requester_pays:
                src_client.set_requester_bool(True)
                dst_client.set_requester_bool(True)
            transfer_pairs = generate_full_transferobjlist(
                src_region_tag, bucket_src, path_src, src_client, dst_region_tag, bucket_dst, path_dst, dst_client, recursive=recursive, requester_pays=requester_pays,
            )
        except exceptions.SkyplaneException as e:
            console.print(f"[bright_black]{traceback.format_exc()}[/bright_black]")
            console.print(e.pretty_print_str())
            UsageClient.log_exception("cli_query_objstore", e, args, src_region_tag, dst_region_tag)
            return 1

        if multipart and (provider_src == "azure" or provider_dst == "azure"):
            # typer.secho("Warning: Azure is not yet supported for multipart transfers. Disabling multipart.", fg="yellow", err=True)
            multipart = False

        topo = generate_topology(
            src_region_tag,
            dst_region_tag,
            solve,
            solver_class = "ILP",
            num_connections=config_default_flags.get("num_connections"),
            max_instances=max_instances,
            solver_total_gbyte_to_transfer=sum(src_obj.size for src_obj, _ in transfer_pairs) if solve else None,
            solver_target_tput_per_vm_gbits=solver_target_tput_per_vm_gbits,
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

        small_transfer_cmd = replicate_small_cp_cmd(src, dst, recursive)
        if (
            config_default_flags.get("native_cmd_enabled")
            and (job.transfer_size / GB) < config_default_flags.get("native_cmd_threshold_gb")
            and small_transfer_cmd
        ):
            # typer.secho(f"Transfer is small enough to delegate to native tools. Delegating to: {small_transfer_cmd}", fg="yellow")
            os.system(small_transfer_cmd)
            return 0
        else:
            transfer_stats = launch_replication_job(
                topo=topo,
                job=job,
                debug=debug,
                reuse_gateways=reuse_gateways,
                use_bbr=config_default_flags.get("bbr"),
                use_compression=config_default_flags.get("compress") if src_region_tag != dst_region_tag else False,
                use_e2ee=config_default_flags.get("encrypt_e2e") if src_region_tag != dst_region_tag else False,
                use_socket_tls=config_default_flags.get("encrypt_socket_tls") if src_region_tag != dst_region_tag else False,
                aws_instance_class=config_default_flags.get("aws_instance_class"),
                aws_use_spot_instances=config_default_flags.get("aws_use_spot_instances"),
                azure_instance_class=config_default_flags.get("azure_instance_class"),
                azure_use_spot_instances=config_default_flags.get("azure_use_spot_instances"),
                gcp_instance_class=config_default_flags.get("gcp_instance_class"),
                gcp_use_premium_network=config_default_flags.get("gcp_use_premium_network"),
                gcp_use_spot_instances=config_default_flags.get("gcp_use_spot_instances"),
                multipart_enabled=multipart,
                multipart_min_threshold_mb=config_default_flags.get("multipart_min_threshold_mb"),
                multipart_chunk_size_mb=config_default_flags.get("multipart_chunk_size_mb"),
                multipart_max_chunks=config_default_flags.get("multipart_max_chunks"),
                error_reporting_args=args,
            )
            if config_default_flags.get("verify_checksums"):
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
            # if transfer_stats.monitor_status == "completed":
                # rprint(f"\n:white_check_mark: [bold green]Transfer completed successfully[/bold green]")
                # runtime_line = f"[white]Transfer runtime:[/white] [bright_black]{transfer_stats.total_runtime_s:.2f}s[/bright_black]"
                # throughput_line = f"[white]Throughput:[/white] [bright_black]{transfer_stats.throughput_gbits:.2f}Gbps[/bright_black]"
                # rprint(f"{runtime_line}, {throughput_line}")
            UsageClient.log_transfer(transfer_stats, args, src_region_tag, dst_region_tag)
            return 0 if transfer_stats.monitor_status == "completed" else 1
    else:
        raise NotImplementedError(f"{provider_src} to {provider_dst} not supported yet")



def deprovision():
    """Deprovision all resources created by skyplane."""
    instances = query_instances()

    if instances:
        # typer.secho(f"Deprovisioning {len(instances)} instances", fg="yellow", bold=True)
        do_parallel(lambda instance: instance.terminate_instance(), instances, desc="Deprovisioning", spinner=True, spinner_persist=True)
    else:
        pass
        # typer.secho("No instances to deprovision", fg="yellow", bold=True)

    if AWSAuthenticationProvider().enabled():
        aws = AWSCloudProvider()
        # remove skyplane vpc
        vpcs = do_parallel(partial(aws.get_vpcs), aws.region_list(), desc="Querying VPCs", spinner=True)
        args = [(x[0], vpc.id) for x in vpcs for vpc in x[1]]
        do_parallel(lambda args: aws.remove_sg_ips(*args), args, desc="Removing IPs from VPCs", spinner=True, spinner_persist=True)
        # remove all instance profiles
        profiles = aws.list_instance_profiles(prefix="skyplane-aws")
        if profiles:
            do_parallel(aws.delete_instance_profile, profiles, desc="Deleting instance profiles", spinner=True, spinner_persist=True, n=4)