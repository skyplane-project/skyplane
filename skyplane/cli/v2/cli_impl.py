import os
import time
from typing import Any, Dict, Optional

import typer
from rich.progress import Progress, SpinnerColumn, TextColumn

import skyplane
from skyplane.api.config import TransferConfig
from skyplane.api.transfer_job import CopyJob
from skyplane.cli.cli_impl.cp_replicate_fallback import (
    replicate_onprem_cp_cmd,
    replicate_onprem_sync_cmd,
    replicate_small_cp_cmd,
    replicate_small_sync_cmd,
)
from skyplane.cli.common import console, print_stats_completed, to_api_config
from skyplane.cli.usage.client import UsageClient
from skyplane.config import SkyplaneConfig
from skyplane.config_paths import cloud_config
from skyplane.replicate.replicator_client import TransferStats
from skyplane.utils import logger
from skyplane.utils.definitions import format_bytes


class SkyplaneCLI:
    def __init__(self, src_region_tag: str, dst_region_tag: str, args: Dict[str, Any], skyplane_config: Optional[SkyplaneConfig] = None):
        self.src_region_tag, self.dst_region_tag = src_region_tag, dst_region_tag
        self.args = args
        self.aws_config, self.azure_config, self.gcp_config = to_api_config(skyplane_config or cloud_config)
        self.transfer_config = self.make_transfer_config(skyplane_config or cloud_config)
        self.client = skyplane.SkyplaneClient(aws_config=self.aws_config, azure_config=self.azure_config, gcp_config=self.gcp_config)
        typer.secho(f"Using Skyplane version {skyplane.__version__}", fg="bright_black")
        typer.secho(f"Logging to: {self.client.log_dir / 'client.log'}", fg="bright_black")

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
            UsageClient.log_exception("cli_check_config", e, self.args, self.src_region_tag, self.dst_region_tag)
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
                UsageClient.log_transfer(transfer_stats, self.args, self.src_region_tag, self.dst_region_tag)
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
                UsageClient.log_transfer(transfer_stats, self.args, self.src_region_tag, self.dst_region_tag)
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

    def make_dataplane(self, **solver_args) -> skyplane.Dataplane:
        dp = self.client.dataplane(*self.src_region_tag.split(":"), *self.dst_region_tag.split(":"), **solver_args)
        logger.fs.debug(f"Using dataplane: {dp}")
        return dp

    def confirm_transfer(self, dp: skyplane.Dataplane, query_n: int = 5, ask_to_confirm_transfer=True) -> bool:
        """Prompts the user to confirm their transfer by querying the first query_n files from the TransferJob"""
        if not len(dp.jobs_to_dispatch) > 0:
            typer.secho("No jobs to dispatch.")
            return False
        transfer_pair_gen = dp.jobs_to_dispatch[0].gen_transfer_pairs()  # type: ignore
        console.print(f"[bold yellow]Will transfer objects from {dp.src_region_tag} to {dp.dst_region_tag}[/bold yellow]")
        sorted_counts = sorted(dp.topology.per_region_count().items(), key=lambda x: x[0])
        console.print(
            f"  [bold][blue]VMs to provision:[/blue][/bold] [bright_black]{', '.join(f'{c}x {r}' for r, c in sorted_counts)}[/bright_black]"
        )
        if dp.topology.cost_per_gb:
            console.print(
                f"  [bold][blue]Estimated egress cost:[/blue][/bold] [bright_black]${dp.topology.cost_per_gb:,.2f}/GB[/bright_black]"
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
        for src_obj, dst_obj in obj_pairs[:query_n]:
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
            console.print("[green]Transfer starting[/bold]")
            return True

    def estimate_small_transfer(self, dp: skyplane.Dataplane, size_threshold_bytes: float, query_n: int = 1000) -> bool:
        """Estimates if the transfer is small by querying up to `query_n` files from the TransferJob. If it exceeds
        the file size limit, then it will fall back to the cloud CLIs."""
        if len(dp.jobs_to_dispatch) != 1:
            return False
        job = dp.jobs_to_dispatch[0]
        if not isinstance(job, CopyJob):
            return False
        transfer_pair_gen = job.gen_transfer_pairs()
        total_size = 0
        for _ in range(query_n):
            generator_exhuausted = False
            try:
                src_obj, _ = next(transfer_pair_gen)
                total_size += src_obj.size
                if total_size > size_threshold_bytes:
                    return False
            except StopIteration:
                generator_exhuausted = True
                break
        if generator_exhuausted and total_size < size_threshold_bytes:
            return True
        else:
            return False
