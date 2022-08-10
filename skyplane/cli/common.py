import re
import subprocess
from functools import partial
from pathlib import Path

import typer
from rich.console import Console

from skyplane.compute.aws.aws_auth import AWSAuthentication
from skyplane.compute.aws.aws_cloud_provider import AWSCloudProvider
from skyplane.compute.azure.azure_auth import AzureAuthentication
from skyplane.compute.azure.azure_cloud_provider import AzureCloudProvider
from skyplane.compute.gcp.gcp_auth import GCPAuthentication
from skyplane.compute.gcp.gcp_cloud_provider import GCPCloudProvider
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
    console.print(f"[blue]{header}[/blue]\n")


def parse_path(path: str):
    def is_plausible_local_path(path_test: str):
        path_test = Path(path_test)
        if path_test.exists():
            return True
        if path_test.is_dir():
            return True
        if path_test.parent.exists():
            return True
        return False

    if path.startswith("s3://") or path.startswith("gs://"):
        provider, parsed = path[:2], path[5:]
        if len(parsed) == 0:
            typer.secho(f"Invalid path: '{path}'", fg="red", err=True)
            raise typer.Exit(code=1)
        bucket, *keys = parsed.split("/", 1)
        key = keys[0] if len(keys) > 0 else ""
        return provider, bucket, key
    elif (path.startswith("https://") or path.startswith("http://")) and "blob.core.windows.net" in path:
        # Azure blob storage
        regex = re.compile(r"https?://([^/]+).blob.core.windows.net/([^/]+)/?(.*)")
        match = regex.match(path)
        if match is None:
            raise ValueError(f"Invalid Azure path: {path}")
        account, container, blob_path = match.groups()
        return "azure", f"{account}/{container}", blob_path
    elif path.startswith("azure://"):
        bucket_name = path[8:]
        region = path[8:].split("-", 2)[-1]
        return "azure", bucket_name, region
    elif is_plausible_local_path(path):
        return "local", None, path
    raise ValueError(f"Parse error {path}")


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

    if AWSAuthentication().enabled():
        aws = AWSCloudProvider()
        for region in aws.region_list():
            query_jobs.append(catch_error(partial(aws.get_matching_instances, region)))
    if AzureAuthentication().enabled():
        query_jobs.append(catch_error(lambda: AzureCloudProvider().get_matching_instances()))
    if GCPAuthentication().enabled():
        query_jobs.append(catch_error(lambda: GCPCloudProvider().get_matching_instances()))
    # query in parallel
    for instance_list in do_parallel(
        lambda f: f(), query_jobs, n=-1, return_args=False, spinner=True, desc="Querying clouds for instances"
    ):
        instances.extend(instance_list)
    return instances
