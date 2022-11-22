import time

import typer

from skyplane.cli.common import print_header
from skyplane.cli.v2.cli_impl import SkyplaneCLI
from skyplane.cli.v2.progress_reporter.simple_reporter import SimpleReporter
from skyplane.config_paths import cloud_config, config_path
from skyplane.obj_store.object_store_interface import ObjectStoreInterface
from skyplane.utils.definitions import GB
from skyplane.utils.path import parse_path
from skyplane.utils import logger


def cp(
    src: str,
    dst: str,
    recursive: bool = typer.Option(False, "--recursive", "-r", help="If true, will copy objects at folder prefix recursively"),
    debug: bool = typer.Option(False, help="If true, will write debug information to debug directory."),
    multipart: bool = typer.Option(cloud_config.get_flag("multipart_enabled"), help="If true, will use multipart uploads."),
    # transfer flags
    confirm: bool = typer.Option(cloud_config.get_flag("autoconfirm"), "--confirm", "-y", "-f", help="Confirm all transfer prompts"),
    max_instances: int = typer.Option(cloud_config.get_flag("max_instances"), "--max-instances", "-n", help="Number of gateways"),
    max_connections: int = typer.Option(
        cloud_config.get_flag("num_connections"), "--max-connections", help="Number of connections per gateway"
    ),
    # todo - add solver params once API supports it
    # solver
    solver: str = typer.Option("direct", "--solver", help="Solver to use for transfer"),
):
    print_header()
    provider_src, bucket_src, path_src = parse_path(src)
    provider_dst, bucket_dst, path_dst = parse_path(dst)
    src_region_tag = ObjectStoreInterface.create(f"{provider_src}:infer", bucket_src).region_tag()
    dst_region_tag = ObjectStoreInterface.create(f"{provider_dst}:infer", bucket_dst).region_tag()
    args = {
        "cmd": "cp",
        "recursive": recursive,
        "debug": debug,
        "multipart": multipart,
        "confirm": confirm,
        "max_instances": max_instances,
        "max_connections": max_connections,
        "solver": solver,
    }

    cli = SkyplaneCLI(src_region_tag=src_region_tag, dst_region_tag=dst_region_tag, args=args)
    if not cli.check_config():
        typer.secho(
            f"Skyplane configuration file is not valid. Please reset your config by running `rm {config_path}` and then rerunning `skyplane init` to fix.",
            fg="red",
        )
        return 1

    if provider_src in ("local", "hdfs", "nfs") or provider_dst in ("local", "hdfs", "nfs"):
        if provider_src == "hdfs" or provider_dst == "hdfs":
            typer.secho("HDFS is not supported yet.", fg="red")
            return 1
        return 0 if cli.transfer_cp_onprem(src, dst, recursive) else 1
    elif provider_src in ("aws", "gcp", "azure") and provider_dst in ("aws", "gcp", "azure"):
        # transfer via Skyplane's API
        # todo support ILP solver params
        dp = cli.make_dataplane(
            solver_type=solver,
            n_vms=max_instances,
            n_connections=max_connections,
        )
        with dp.auto_deprovision():
            dp.queue_copy(src, dst, recursive=recursive)
            if cloud_config.get_flag("native_cmd_enabled") and cli.estimate_small_transfer(
                dp, cloud_config.get_flag("native_cmd_threshold_gb") * GB
            ):
                small_transfer_status = cli.transfer_cp_small(src, dst, recursive)
                if small_transfer_status:
                    return 0
            dp.provision()
            tracker = dp.run_async()
            reporter = SimpleReporter(tracker)
            while reporter.update():
                time.sleep(1)
