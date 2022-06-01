import os
import re
import resource
import subprocess
from functools import partial
from pathlib import Path
from sys import platform

import typer

from skyplane.compute.aws.aws_auth import AWSAuthentication
from skyplane.compute.aws.aws_cloud_provider import AWSCloudProvider
from skyplane.compute.azure.azure_auth import AzureAuthentication
from skyplane.compute.azure.azure_cloud_provider import AzureCloudProvider
from skyplane.compute.gcp.gcp_auth import GCPAuthentication
from skyplane.compute.gcp.gcp_cloud_provider import GCPCloudProvider
from skyplane.utils import logger
from skyplane.utils.fn import do_parallel


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
            typer.secho(f"Invalid path: '{path}'", fg="red")
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


def check_ulimit(hard_limit=1024 * 1024, soft_limit=1024 * 1024):
    current_limit_soft, current_limit_hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    if current_limit_hard < hard_limit:
        typer.secho(
            f"Warning: hard file limit is set to {current_limit_hard}, which is less than the recommended minimum of {hard_limit}", fg="red"
        )
        increase_hard_limit = ["sudo", "sysctl", "-w", f"fs.file-max={hard_limit}"]
        typer.secho(f"Will run the following commands:")
        typer.secho(f"    {' '.join(increase_hard_limit)}", fg="yellow")
        if typer.confirm("sudo required; Do you want to increase the limit?", default=True):
            subprocess.check_output(increase_hard_limit)
            new_limit = resource.getrlimit(resource.RLIMIT_NOFILE)[0]
            if new_limit < soft_limit:
                typer.secho(
                    f"Failed to increase ulimit to {soft_limit}, please set manually with 'ulimit -n {soft_limit}'. Current limit is {new_limit}",
                    fg="red",
                )
                raise typer.Abort()
            else:
                typer.secho(f"Successfully increased ulimit to {new_limit}", fg="green")
    if current_limit_soft < soft_limit and (platform == "linux" or platform == "linux2"):
        increase_soft_limit = ["sudo", "prlimit", "--pid", str(os.getpid()), f"--nofile={soft_limit}:{hard_limit}"]
        logger.warning(
            f"Warning: soft file limit is set to {current_limit_soft}, increasing for process with `{' '.join(increase_soft_limit)}`"
        )
        subprocess.check_output(increase_soft_limit)


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
