import os
import signal
import time
import traceback
from dataclasses import dataclass
from typing import Dict, Any, Optional, List

import typer
from rich.progress import Progress, TextColumn, SpinnerColumn

import skyplane
from skyplane.api.config import TransferConfig, AWSConfig, GCPConfig, AzureConfig, IBMCloudConfig
from skyplane.api.transfer_job import CopyJob, SyncJob, TransferJob
from skyplane.cli.impl.cp_replicate_fallback import (
    replicate_onprem_cp_cmd,
    replicate_onprem_sync_cmd,
    replicate_small_cp_cmd,
    replicate_small_sync_cmd,
)
from skyplane.cli.impl.common import print_header, console, print_stats_completed, register_exception_handler
from skyplane.api.usage import UsageClient
from skyplane.config import SkyplaneConfig
from skyplane.config_paths import cloud_config, config_path
from skyplane.obj_store.object_store_interface import StorageInterface
from skyplane.cli.impl.progress_bar import ProgressBarTransferHook
from skyplane.utils import logger
from skyplane.utils.definitions import GB, format_bytes
from skyplane.utils.path import parse_path


@dataclass
class TransferStats:
    monitor_status: str
    total_runtime_s: Optional[float] = None
    throughput_gbits: Optional[float] = None
    errors: Optional[Dict[str, List[str]]] = None

    @classmethod
    def empty(cls):
        return TransferStats(monitor_status="empty")

    def to_dict(self) -> Dict[str, Optional[Any]]:
        return {
            "monitor_status": self.monitor_status,
            "total_runtime_s": self.total_runtime_s,
            "throughput_gbits": self.throughput_gbits,
            "errors": [str(e) for e in self.errors.values()] if self.errors else None,
        }


class SkyplaneCLI:
    def __init__(self, src_region_tag: str, dst_region_tag: str, args: Dict[str, Any], skyplane_config: Optional[SkyplaneConfig] = None):
        self.src_region_tag, self.dst_region_tag = src_region_tag, dst_region_tag
        self.args = args
        self.aws_config, self.azure_config, self.gcp_config, self.ibmcloud_config = self.to_api_config(skyplane_config or cloud_config)

        # update config
        # TODO: set remaining config params
        if skyplane_config:
            skyplane_config.set_flag("multipart_enabled", str(self.args["multipart"]))
        if cloud_config:
            cloud_config.set_flag("multipart_enabled", str(self.args["multipart"]))

        self.transfer_config = self.make_transfer_config(skyplane_config or cloud_config)
        self.client = skyplane.SkyplaneClient(
            aws_config=self.aws_config,
            azure_config=self.azure_config,
            gcp_config=self.gcp_config,
            transfer_config=self.transfer_config,
            ibmcloud_config=self.ibmcloud_config,
        )
        typer.secho(f"Using Skyplane version {skyplane.__version__}", fg="bright_black")

    def to_api_config(self, config: SkyplaneConfig):
        aws_config = AWSConfig(aws_enabled=config.aws_enabled)
        # todo: fix azure config support by collecting azure umi name and resource group and store in skyplane config
        gcp_config = GCPConfig(gcp_project_id=config.gcp_project_id, gcp_enabled=config.gcp_enabled)
        ibmcloud_config = IBMCloudConfig(
            ibmcloud_access_id=config.ibmcloud_access_id,
            ibmcloud_secret_key=config.ibmcloud_secret_key,
            ibmcloud_iam_key=config.ibmcloud_iam_key,
            ibmcloud_iam_endpoint=config.ibmcloud_iam_endpoint,
            ibmcloud_useragent=config.ibmcloud_useragent,
            ibmcloud_resource_group_id=config.ibmcloud_resource_group_id,
            ibmcloud_enabled=config.ibmcloud_enabled,
        )
        if not config.azure_resource_group or not config.azure_umi_name:
            typer.secho(
                "Azure resource group and UMI name not configured correctly. Please reinit Azure with `skyplane init --reinit-azure`.",
                fg=typer.colors.RED,
                err=True,
            )
            return aws_config, None, gcp_config, ibmcloud_config
        azure_config = AzureConfig(
            config.azure_subscription_id,
            config.azure_resource_group,
            config.azure_principal_id,
            config.azure_umi_name,
            config.azure_client_id,
            config.azure_enabled,
        )
        return aws_config, azure_config, gcp_config, ibmcloud_config

    def make_transfer_config(self, config: SkyplaneConfig) -> TransferConfig:
        intraregion = self.src_region_tag == self.dst_region_tag
        return TransferConfig(
            autoterminate_minutes=config.get_flag("autoshutdown_minutes"),
            requester_pays=config.get_flag("requester_pays"),
            use_bbr=config.get_flag("bbr"),
            use_compression=config.get_flag("compress") if not intraregion else False,
            use_e2ee=config.get_flag("encrypt_e2e") if not intraregion else False,
            use_socket_tls=config.get_flag("encrypt_socket_tls") if not intraregion else False,
            aws_use_spot_instances=config.get_flag("aws_use_spot_instances"),
            azure_use_spot_instances=config.get_flag("azure_use_spot_instances"),
            gcp_use_spot_instances=config.get_flag("gcp_use_spot_instances"),
            aws_instance_class=config.get_flag("aws_instance_class"),
            azure_instance_class=config.get_flag("azure_instance_class"),
            gcp_instance_class=config.get_flag("gcp_instance_class"),
            ibmcloud_instance_class=config.get_flag("ibmcloud_instance_class"),
            gcp_use_premium_network=config.get_flag("gcp_use_premium_network"),
            multipart_enabled=config.get_flag("multipart_enabled"),
            multipart_threshold_mb=config.get_flag("multipart_min_threshold_mb"),
            multipart_chunk_size_mb=config.get_flag("multipart_chunk_size_mb"),
            multipart_max_chunks=config.get_flag("multipart_max_chunks"),
        )

    def check_config(self) -> bool:
        try:
            cloud_config.check_config()
            return True
        except skyplane.exceptions.BadConfigException as e:
            logger.exception(e)
            UsageClient.log_exception("cli_check_config", e, self.args, self.src_region_tag, [self.dst_region_tag])
            return False

    def transfer_cp_onprem(self, src: str, dst: str, recursive: bool) -> bool:
        cmd = replicate_onprem_cp_cmd(src, dst, recursive)
        if cmd:
            typer.secho(f"Delegating to: {cmd}", fg="yellow")
            start = time.perf_counter()
            rc = os.system(cmd)
            request_time = time.perf_counter() - start
            if rc == 0:
                print_stats_completed(request_time, None)
                transfer_stats = TransferStats(monitor_status="completed", total_runtime_s=request_time, throughput_gbits=0)
                UsageClient.log_transfer(transfer_stats.to_dict(), self.args, self.src_region_tag, [self.dst_region_tag])
            return True
        else:
            typer.secho("Transfer not supported", fg="red")
            return True

    def transfer_sync_onprem(self, src: str, dst: str) -> bool:
        cmd = replicate_onprem_sync_cmd(src, dst)
        if cmd:
            typer.secho(f"Delegating to: {cmd}", fg="yellow")
            start = time.perf_counter()
            rc = os.system(cmd)
            request_time = time.perf_counter() - start
            if rc == 0:
                print_stats_completed(request_time, None)
                transfer_stats = TransferStats(monitor_status="completed", total_runtime_s=request_time, throughput_gbits=0)
                UsageClient.log_transfer(transfer_stats.to_dict(), self.args, self.src_region_tag, [self.dst_region_tag])
            return True
        else:
            typer.secho("Transfer not supported", fg="red")
            return True

    def transfer_cp_small(self, src: str, dst: str, recursive: bool) -> bool:
        small_transfer_cmd = replicate_small_cp_cmd(src, dst, recursive)
        if small_transfer_cmd:
            typer.secho(f"Transfer is small enough to delegate to native tools. Delegating to: {small_transfer_cmd}", fg="yellow")
            typer.secho(f"You can disable this with `skyplane config set native_cmd_enabled false`", fg="bright_black")
            os.system(small_transfer_cmd)
            return True
        else:
            return False

    def transfer_sync_small(self, src: str, dst: str) -> bool:
        small_transfer_cmd = replicate_small_sync_cmd(src, dst)
        if small_transfer_cmd:
            typer.secho(f"Transfer is small enough to delegate to native tools. Delegating to: {small_transfer_cmd}", fg="yellow")
            typer.secho(f"You can disable this with `skyplane config set native_cmd_enabled false`", fg="bright_black")
            os.system(small_transfer_cmd)
            return True
        else:
            return False

    # def make_dataplane(self, **solver_args) -> skyplane.Dataplane:
    #    if self.src_region_tag.split(":")[0] == "hdfs":
    #        self.src_region_tag = self.dst_region_tag
    #    dp = self.client.dataplane(*self.src_region_tag.split(":"), *self.dst_region_tag.split(":"), **solver_args)
    #    logger.fs.debug(f"Using dataplane: {dp}")
    #    return dp

    def make_pipeline(self, **solver_args) -> skyplane.Pipeline:
        pipeline = self.client.pipeline(**solver_args)
        logger.fs.debug(f"Using pipeline: {pipeline}")
        return pipeline

    def confirm_transfer(
        self, pipeline: skyplane.Pipeline, src_region_tag: str, dest_region_tags: List[str], query_n: int = 5, ask_to_confirm_transfer=True
    ) -> bool:
        """Prompts the user to confirm their transfer by querying the first query_n files from the TransferJob"""
        if not len(pipeline.jobs_to_dispatch) > 0:
            typer.secho("No jobs to dispatch.")
            return False
        transfer_pair_gen = pipeline.jobs_to_dispatch[0].gen_transfer_pairs()  # type: ignore
        if len(dest_region_tags) == 1:
            console.print(f"[bold yellow]Will transfer objects from {src_region_tag} to {dest_region_tags[0]}[/bold yellow]")
        else:
            console.print(f"[bold yellow]Will transfer objects from {src_region_tag} to {dest_region_tags}[/bold yellow]")

        if src_region_tag.startswith("local") or dest_region_tags[0].startswith("local"):
            # TODO: should still pass cost estimate
            console.print(f"[yellow]Note: local transfers are not monitored by Skyplane[yellow]")
            return True

        topology = pipeline.planner.plan(pipeline.jobs_to_dispatch)
        sorted_counts = sorted(topology.per_region_count().items(), key=lambda x: x[0])
        console.print(
            f"  [bold][blue]VMs to provision:[/blue][/bold] [bright_black]{', '.join(f'{c}x {r}' for r, c in sorted_counts)}[/bright_black]"
        )
        if topology.cost_per_gb:
            console.print(
                f"  [bold][blue]Estimated egress cost:[/blue][/bold] [bright_black]${topology.cost_per_gb:,.2f}/GB[/bright_black]"
            )
        # show spinner
        with Progress(
            TextColumn(" "),
            SpinnerColumn(),
            TextColumn(f"[bright_black]Querying objects for transfer...[/bright_black]"),
            transient=True,
        ) as progress:
            progress.add_task("", total=None)
            obj_pairs = []
            for _ in range(query_n + 1):
                try:
                    obj_pairs.append(next(transfer_pair_gen))
                except StopIteration:
                    break
        if len(obj_pairs) == 0:
            typer.secho("No objects to transfer.")
            return False
        for pair in obj_pairs[:query_n]:
            src_obj = pair.src_obj
            for dst_obj in pair.dst_objs.values():
                console.print(
                    f"  [bright_black][bold]{src_obj.full_path()}[/bold] => [bold]{dst_obj.full_path()}[/bold] ({format_bytes(src_obj.size)})[/bright_black]"
                )
        if len(obj_pairs) > query_n:
            console.print(f"  [bright_black]...[/bright_black]")
        if ask_to_confirm_transfer:
            if typer.confirm("Continue?", default=True):
                logger.fs.debug("User confirmed transfer")
                console.print(
                    "[green]Transfer starting[/green] (Tip: Enable auto-confirmation with `skyplane config set autoconfirm true`)"
                )
                return True
            else:
                logger.fs.error("Transfer cancelled by user.")
                console.print("[bold][red]Transfer cancelled by user.[/red][/bold]")
                raise typer.Abort()
        else:
            console.print("[green]Transfer starting[/green]")
            return True

    def estimate_small_transfer(self, job: TransferJob, size_threshold_bytes: float, query_n: int = 1000) -> bool:
        """Estimates if the transfer is small by querying up to `query_n` files from the TransferJob. If it exceeds
        the file size limit, then it will fall back to the cloud CLIs."""

        # TODO: why shouldn't this include sync?
        if not isinstance(job, CopyJob):
            return False
        transfer_pair_gen = job.gen_transfer_pairs()
        total_size = 0
        generator_exhausted = False
        for _ in range(query_n):
            try:
                pair = next(transfer_pair_gen)
                src_obj = pair.src_obj
                total_size += src_obj.size
                if total_size > size_threshold_bytes:
                    return False
            except StopIteration:
                generator_exhausted = True
                break
        if generator_exhausted and total_size < size_threshold_bytes:
            return True
        else:
            return False


def force_deprovision(dp: skyplane.Dataplane):
    s = signal.signal(signal.SIGINT, signal.SIG_IGN)
    dp.deprovision()
    signal.signal(signal.SIGINT, s)


def run_transfer(
    src: str,
    dst: str,
    recursive: bool,
    debug: bool,
    multipart: bool,
    confirm: bool,
    max_instances: int,
    max_connections: int,
    solver: str,
    cmd: str,
):
    assert cmd == "cp" or cmd == "sync", f"Invalid command: {cmd}"
    if not debug:
        register_exception_handler()
    print_header()

    provider_src, bucket_src, path_src = parse_path(src)
    provider_dst, bucket_dst, path_dst = parse_path(dst)

    # update planner for one-sided transfer
    # somet process for other cloud providers with no VM support
    assert provider_src != "cloudflare" or provider_dst != "cloudflare", "Cannot transfer between two Cloudflare buckets"
    if provider_src == "cloudflare":
        solver = "dst_one_sided"
    elif provider_dst == "cloudflare":
        solver = "src_one_sided"

    src_region_tag = StorageInterface.create(f"{provider_src}:infer", bucket_src).region_tag()
    dst_region_tag = StorageInterface.create(f"{provider_dst}:infer", bucket_dst).region_tag()
    args = {
        "cmd": cmd,
        "recursive": True,
        "debug": debug,
        "multipart": multipart,
        "confirm": confirm,
        "max_instances": max_instances,
        "max_connections": max_connections,
        "solver": solver,
    }

    # create CLI object
    cli = SkyplaneCLI(src_region_tag=src_region_tag, dst_region_tag=dst_region_tag, args=args)
    if not cli.check_config():
        typer.secho(
            f"Skyplane configuration file is not valid. Please reset your config by running `rm {config_path}` and then rerunning `skyplane init` to fix.",
            fg="red",
        )
        return 1

    # create pipeline and queue transfer
    pipeline = cli.make_pipeline(planning_algorithm=solver, max_instances=max_instances)
    if cli.args["cmd"] == "cp":
        pipeline.queue_copy(src, dst, recursive=recursive)
    else:
        pipeline.queue_sync(src, dst)

    # confirm transfer
    if not cli.confirm_transfer(pipeline, src_region_tag, [dst_region_tag], 5, ask_to_confirm_transfer=not confirm):
        return 1

    # local->local transfers not supported (yet)
    if provider_src == "local" and provider_dst == "local":
        raise NotImplementedError("Local->local transfers not supported (yet)")

    # fall back options: local->cloud, cloud->local, small cloud->cloud transfers
    if provider_src == "local" or provider_dst == "local":
        if cli.args["cmd"] == "cp":
            return 0 if cli.transfer_cp_onprem(src, dst, recursive) else 1
        else:
            return 0 if cli.transfer_sync_onprem(src, dst) else 1
    elif cloud_config.get_flag("native_cmd_enabled"):
        # fallback option: transfer is too small
        if cli.args["cmd"] == "cp":
            job = CopyJob(src, [dst], recursive=recursive)  # TODO: rever to using pipeline
            if cli.estimate_small_transfer(job, cloud_config.get_flag("native_cmd_threshold_gb") * GB):
                small_transfer_status = cli.transfer_cp_small(src, dst, recursive)
                return 0 if small_transfer_status else 1
        else:
            job = SyncJob(src, [dst])
            if cli.estimate_small_transfer(job, cloud_config.get_flag("native_cmd_threshold_gb") * GB):
                small_transfer_status = cli.transfer_sync_small(src, dst)
                return 0 if small_transfer_status else 1

    # dataplane must be created after transfers are queued
    dp = pipeline.create_dataplane(debug=debug)
    with dp.auto_deprovision():
        try:
            dp.provision(spinner=True)
            dp.run(pipeline.jobs_to_dispatch, hooks=ProgressBarTransferHook(dp.topology.dest_region_tags))
        except KeyboardInterrupt:
            logger.fs.warning("Transfer cancelled by user (KeyboardInterrupt).")
            console.print("\n[red]Transfer cancelled by user. Copying gateway logs and exiting.[/red]")
            try:
                dp.copy_gateway_logs()
                force_deprovision(dp)
            except Exception as e:
                logger.fs.exception(e)
                console.print(f"[bright_black]{traceback.format_exc()}[/bright_black]")
                console.print(e)
                UsageClient.log_exception("cli_cp", e, args, cli.src_region_tag, [cli.dst_region_tag])
                console.print("[bold red]Deprovisioning was interrupted! VMs may still be running which will incur charges.[/bold red]")
                console.print("[bold red]Please manually deprovision the VMs by running `skyplane deprovision`.[/bold red]")
            return 1
        except skyplane.exceptions.SkyplaneException as e:
            console.print(f"[bright_black]{traceback.format_exc()}[/bright_black]")
            console.print(e.pretty_print_str())
            UsageClient.log_exception("cli_query_objstore", e, args, cli.src_region_tag, [cli.dst_region_tag])
            force_deprovision(dp)
        except Exception as e:
            logger.fs.exception(e)
            console.print(f"[bright_black]{traceback.format_exc()}[/bright_black]")
            console.print(e)
            UsageClient.log_exception("cli_query_objstore", e, args, cli.src_region_tag, [cli.dst_region_tag])
            force_deprovision(dp)


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
    solver_required_throughput_gbits: float = typer.Option(1, "--tput", "-t", help="Required throughput to be solved for in Gbps"),
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
    :param debug: If true, will write debug information to debug directory.
    :type debug: bool
    :param multipart: If true, will use multipart uploads.
    :type multipart: bool
    :param confirm: If true, will not prompt for confirmation of transfer.
    :type confirm: bool
    :param max_instances: The maximum number of instances to use per region (default: 1)
    :type max_instances: int
    :param max_connections: The maximum number of connections per instance (default: 32)
    :type max_connections: int
    :param solver: The solver to use for the transfer (default: direct)
    :type solver: str
    """
    return run_transfer(src, dst, recursive, debug, multipart, confirm, max_instances, max_connections, solver, "cp")


def sync(
    src: str,
    dst: str,
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
    solver_required_throughput_gbits: float = typer.Option(1, "--tput", "-t", help="Required throughput to be solved for"),
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
    :param debug: If true, will write debug information to debug directory.
    :type debug: bool
    :param multipart: If true, will use multipart uploads.
    :type multipart: bool
    :param confirm: If true, will not prompt for confirmation of transfer.
    :type confirm: bool
    :param max_instances: The maximum number of instances to use per region (default: 1)
    :type max_instances: int
    :param max_connections: The maximum number of connections per instance (default: 32)
    :type max_connections: int
    :param solver: The solver to use for the transfer (default: direct)
    :type solver: str
    """
    return run_transfer(src, dst, False, debug, multipart, confirm, max_instances, max_connections, solver, "sync")
