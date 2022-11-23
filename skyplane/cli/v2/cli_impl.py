import os
import time
from typing import Dict, Any

import typer

import skyplane
from skyplane.api.transfer_job import CopyJob
from skyplane.cli.cli_impl.cp_replicate_fallback import replicate_onprem_cp_cmd, replicate_small_cp_cmd
from skyplane.cli.common import to_api_config, print_stats_completed, console
from skyplane.cli.usage.client import UsageClient
from skyplane.config_paths import cloud_config
from skyplane.replicate.replicator_client import TransferStats
from skyplane.utils import logger


class SkyplaneCLI:
    def __init__(self, src_region_tag: str, dst_region_tag: str, args: Dict[str, Any]):
        self.src_region_tag, self.dst_region_tag = src_region_tag, dst_region_tag
        self.args = args
        self.aws_config, self.azure_config, self.gcp_config = to_api_config(cloud_config)
        self.client = skyplane.SkyplaneClient(aws_config=self.aws_config, azure_config=self.azure_config, gcp_config=self.gcp_config)
        typer.secho(f"Using Skyplane version {skyplane.__version__}", fg="bright_black")
        typer.secho(f"Logging to: {self.client.log_dir / 'client.log'}", fg="bright_black")

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
                print_stats_completed(request_time, 0)
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

    def make_dataplane(self, **solver_args) -> skyplane.Dataplane:
        dp = self.client.dataplane(*self.src_region_tag.split(":"), *self.dst_region_tag.split(":"), **solver_args)
        logger.fs.debug(f"Using dataplane: {dp}")
        return dp

    def confirm_transfer(self, dp: skyplane.Dataplane, query_n: int = 5, ask_to_confirm_transfer=True) -> bool:
        """Prompts the user to confirm their transfer by querying the first query_n files from the TransferJob"""
        if not len(dp.jobs_to_dispatch) > 0:
            typer.secho("No jobs to dispatch.")
            return False
        job: CopyJob = dp.jobs_to_dispatch[0]  # type: ignore
        transfer_pair_gen = job.gen_transfer_pairs()
        console.print(
            f"\n[bold yellow]Transfer preview: will transfer objects from {dp.src_region_tag} to {dp.dst_region_tag}[/bold yellow]"
        )
        # show spinner
        with console.status("[bright_black]Querying objects...[/bright_black]", spinner="dots"):
            obj_pairs = []
            for _ in range(query_n):
                try:
                    obj_pairs.append(next(transfer_pair_gen))
                except StopIteration:
                    break
        if len(obj_pairs) == 0:
            typer.secho("No objects to transfer.")
            return False
        for src_obj, dst_obj in obj_pairs:
            console.print(f"    [bright_black][bold]{src_obj.key}[/bold] => [bold]{dst_obj.key}[/bold][/bright_black]")
        if ask_to_confirm_transfer:
            if typer.confirm("Continue?", default=True):
                logger.fs.debug("User confirmed transfer")
                console.print(
                    "[bold green]Transfer starting[/bold green] (Tip: Enable auto-confirmation with `skyplane config set autoconfirm true`)"
                )
                console.print("")
                return True
            else:
                logger.fs.error("Transfer cancelled by user.")
                console.print("[bold][red]Transfer cancelled by user.[/red][/bold]")
                raise typer.Abort()

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
