import subprocess
from functools import partial
from typing import Optional

import typer
from rich.console import Console

from skyplane import compute
from skyplane.api.config import AzureConfig, AWSConfig, GCPConfig
from skyplane.config import SkyplaneConfig
from skyplane.utils import logger
from skyplane.utils.fn import do_parallel

console = Console()


def print_header():
    header = """ _____ _   ____   _______ _       ___   _   _  _____ 
/  ___| | / /\ \ / / ___ \ |     / _ \ | \ | ||  ___|
\ `--.| |/ /  \ V /| |_/ / |    / /_\ \|  \| || |__  
 `--. \    \   \ / |  __/| |    |  _  || . ` ||  __| 
/\__/ / |\  \  | | | |   | |____| | | || |\  || |___ 
\____/\_| \_/  \_/ \_|   \_____/\_| |_/\_| \_/\____/"""
    console.print(f"[bright_black]{header}[/bright_black]\n")


def print_stats_completed(total_runtime_s: float, throughput_gbits: Optional[float]):
    console.print(f"\n:white_check_mark: [bold green]Transfer completed successfully[/bold green]")
    runtime_line = f"[white]Transfer runtime:[/white] [bright_black]{total_runtime_s:.2f}s[/bright_black]"
    throughput_line = (
        f", [white]Throughput:[/white] [bright_black]{throughput_gbits:.2f}Gbps[/bright_black]" if throughput_gbits is not None else ""
    )
    console.print(f"{runtime_line}{throughput_line}")


def check_ulimit(hard_limit=1024 * 1024):
    # Get the current fs.file-max limit
    check_hard_limit = ["sysctl", "--values", "fs.file-max"]
    try:
        fs_hard_limit = subprocess.check_output(check_hard_limit)
    except subprocess.CalledProcessError:
        typer.secho(f"Failed to get fs.file-max limit", fg="yellow")
        return
    current_limit_hard = int(fs_hard_limit.decode("UTF-8"))

    # check/update fs.file-max limit
    if current_limit_hard < hard_limit:
        typer.secho(
            f"Warning: file limit is set to {current_limit_hard}, which is less than the recommended minimum of {hard_limit}",
            fg="red",
            err=True,
        )
        increase_ulimit = ["sudo", "sysctl", "-w", f"fs.file-max={hard_limit}"]
        typer.secho(f"Run the following command to increase the hard file limit to the recommended number ({hard_limit}):", fg="yellow")
        typer.secho(f"    {' '.join(increase_ulimit)}", fg="yellow")
    else:
        typer.secho(f"File limit greater than recommended minimum of {hard_limit}.", fg="blue")


def query_instances():
    instances = []
    query_jobs = []

    def catch_error(fn):
        def run():
            try:
                return fn()
            except Exception as e:
                logger.error(f"Error encountered during deprovision: {e}")
                return []

        return run

    if compute.AWSAuthentication().enabled():
        aws = compute.AWSCloudProvider()
        for region in aws.region_list():
            query_jobs.append(catch_error(partial(aws.get_matching_instances, region)))
    if compute.AzureAuthentication().enabled():
        query_jobs.append(catch_error(lambda: compute.AzureCloudProvider().get_matching_instances()))
    if compute.GCPAuthentication().enabled():
        query_jobs.append(catch_error(lambda: compute.GCPCloudProvider().get_matching_instances()))
    # query in parallel
    for instance_list in do_parallel(
        lambda f: f(), query_jobs, n=-1, return_args=False, spinner=True, desc="Querying clouds for instances"
    ):
        instances.extend(instance_list)
    return instances


def to_api_config(config: SkyplaneConfig):
    aws_config = AWSConfig(aws_enabled=config.aws_enabled)
    # todo: fix azure config support by collecting azure umi name and resource group and store in skyplane config
    gcp_config = GCPConfig(gcp_project_id=config.gcp_project_id, gcp_enabled=config.gcp_enabled)
    if not config.azure_resource_group or not config.azure_umi_name:
        typer.secho(
            "Azure resource group and UMI name not configured correctly. Please reinit Azure with `skyplane init --reinit-azure`.",
            fg=typer.colors.RED,
            err=True,
        )
        return aws_config, None, gcp_config
    azure_config = AzureConfig(
        config.azure_subscription_id,
        config.azure_resource_group,
        config.azure_principal_id,
        config.azure_umi_name,
        config.azure_client_id,
        config.azure_enabled,
    )
    return aws_config, azure_config, gcp_config
