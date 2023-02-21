from functools import partial
import sys
from typing import Optional

from rich.console import Console
from rich import print as rprint

import typer
import traceback as tb

from skyplane import compute
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


# pretty exception handler with rich
def register_exception_handler():
    def exception_handler(exception_type, exception, traceback, debug_hook=sys.excepthook):
        # write full traceback infomation with locals to log file
        logger.fs.error(f"Uncaught exception: {exception_type.__name__}: {exception}")
        logger.fs.error("Traceback:\n" + "".join(tb.format_exception(exception_type, exception, traceback)))
        rprint(f"[red][bold]Uncaught exception:[/bold] ({exception_type.__name__}) {exception}[/red]", file=sys.stderr)
        typer.secho(
            "Please check the log file for more information, and ensure to include it if reporting an issue on Github.",
            fg=typer.colors.YELLOW,
        )
        sys.exit(1)

    sys.excepthook = exception_handler
