import subprocess
import time
from functools import partial
from pathlib import Path
from shlex import split
import traceback
from urllib import request
import uuid
import os

from rich import print as rprint

from skyplane.obj_store.object_store_interface import ObjectStoreObject
import skyplane.cli
import skyplane.cli.usage.definitions
import skyplane.cli.usage.client
from skyplane import GB
from skyplane.cli.usage.client import UsageClient, UsageStatsStatus
from skyplane.replicate.replicator_client import ReplicatorClient, TransferStats

import typer
from rich.progress import Progress, SpinnerColumn, TextColumn

import skyplane.cli.cli_aws
import skyplane.cli.cli_azure
import skyplane.cli.cli_config
import skyplane.cli.cli_internal as cli_internal
import skyplane.cli.experiments
from skyplane import cloud_config, config_path, exceptions, skyplane_root
from skyplane.cli.common import print_header, console, print_stats_completed
from skyplane.cli.cli_impl.cp_replicate import (
    enrich_dest_objs,
    generate_full_transferobjlist,
    generate_topology,
    generate_broadcast_topology,
    confirm_transfer,
    launch_replication_job,
)
from skyplane.cli.cli_impl.cp_replicate_fallback import (
    replicate_onprem_cp_cmd,
    replicate_onprem_sync_cmd,
    replicate_small_cp_cmd,
    replicate_small_sync_cmd,
    get_usage_gbits,
)
from skyplane.replicate.replication_plan import ReplicationJob, BroadcastReplicationJob
from skyplane.cli.cli_impl.init import load_aws_config, load_azure_config, load_gcp_config
from skyplane.cli.common import parse_path, query_instances
from skyplane.compute.aws.aws_auth import AWSAuthentication
from skyplane.compute.aws.aws_cloud_provider import AWSCloudProvider
from skyplane.config import SkyplaneConfig
from skyplane.obj_store.object_store_interface import ObjectStoreInterface
from skyplane.utils import logger
from skyplane.utils.fn import do_parallel

from typing import List, Optional, Tuple, Dict

app = typer.Typer(name="broadcast")


@app.command()
def cp(
    src: str,
    dsts: List[str],
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
    :param throughput_per_instance_gbits: The required throughput in Gbps when using the solver (default: 4)
    :type throughput_per_instance_gbits: float
    :param solver_throughput_grid: The throughput grid profile to use for the solver, defaults to author-provided profile
    :type solver_throughput_grid: Path
    :param solver_verbose: If true, will print out the solver's output, defaults to False
    :type solver_verbose: bool (optional)
    """
    print_header()

    provider_src, bucket_src, path_src = parse_path(src)
    src_region_tag = f"{provider_src}:infer"

    dst_region_tags = []
    bucket_dsts = []
    path_dsts = []
    for dst in dsts: 
        provider_dst, bucket_dst, path_dst = parse_path(dst)
        dst_region_tag = f"{provider_dst}:infer"
        dst_region_tags.append(dst_region_tag) 
        bucket_dsts.append(bucket_dst)
        path_dsts.append(path_dst)
        

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
        #UsageClient.log_exception("cli_check_config", e, args, src_region_tag, dst_region_tag)
        return 1

    if provider_src == "local" or provider_dst == "local":
        raise ValueError("Local not supported for broadcast")
    elif provider_src in ["aws", "gcp", "azure"] and provider_dst in ["aws", "gcp", "azure"]:
        try:

            def setup_bucket(region_tag, bucket): 
                client = ObjectStoreInterface.create(region_tag, bucket)
                tag = client.region_tag()
                if cloud_config.get_flag("requester_pays"):
                    client.set_requester_bool(True)
                return tag

            src_region = setup_bucket(src_region_tag, bucket_src) 
            dst_regions = []
            for dst_region_tag, bucket_dst in zip(dst_region_tags, bucket_dsts): 
                dst_regions.append(setup_bucket(dst_region_tag, bucket_dst))

            # get transfer pairs for only first dest
            #transfer_pairs = generate_full_transferobjlist(
            #    src_region_tag, bucket_src, path_src, dst_region_tags[0], bucket_dsts[0], path_dsts[0], recursive=recursive
            #)
        except exceptions.SkyplaneException as e:
            console.print(f"[bright_black]{traceback.format_exc()}[/bright_black]")
            console.print(e.pretty_print_str())
            UsageClient.log_exception("cli_query_objstore", e, args, src_region_tag, dst_region_tag)
            return 1

        # create transfer list
        n_chunks = 20
        transfer_list = []
        for i in range(n_chunks):
            src_obj = ObjectStoreObject(src_region.split(":")[0], "", str(i))
            dst_obj = ObjectStoreObject(dst_regions[0].split(":")[0], "", str(i))
            transfer_list.append((src_obj, dst_obj))

        print(transfer_list)

        topo = generate_broadcast_topology(
            src_region,
            dst_regions,
            solve,
            num_connections=cloud_config.get_flag("num_connections"),
            max_instances=max_instances,
            solver_total_gbyte_to_transfer=sum(src_obj.size for src_obj, _ in transfer_list) if solve else None,
            solver_target_tput_per_vm_gbits=solver_target_tput_per_vm_gbits,
            solver_throughput_grid=solver_throughput_grid,
            solver_verbose=solver_verbose,
            args=args,
        )
        print("Generate topology", topo.to_json())

        random_chunk_size_mb = 8 # sets replicate random (no object store)
        job = BroadcastReplicationJob(
            source_region=topo.source_region(),
            source_bucket=bucket_src,
            dest_regions=topo.sink_regions(),
            dest_buckets=bucket_dsts,
            transfer_pairs=transfer_list, # can't have transfer pairs for broadcast
            random_chunk_size_mb=random_chunk_size_mb
        )
        #confirm_transfer(topo=topo, job=job, ask_to_confirm_transfer=not confirm)
        confirm_transfer(topo=topo, job=job, ask_to_confirm_transfer=False)
        stats = launch_replication_job(
            topo=topo, job=job, debug=debug, reuse_gateways=reuse_gateways, use_compression=False, use_e2ee=True
        )
        return 0 if stats.monitor_status == "completed" else 1

if __name__ == "__main__":
    app()
