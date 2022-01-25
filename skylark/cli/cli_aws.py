"""
AWS convenience interface
"""

from shlex import split
import subprocess
import sys
from typing import Optional
import questionary

import typer
from loguru import logger
from skylark.compute.aws.aws_cloud_provider import AWSCloudProvider
from skylark.compute.aws.aws_server import AWSServer
from skylark.utils.utils import do_parallel

app = typer.Typer(name="skylark-aws")

# config logger
logger.remove()
logger.add(sys.stderr, format="{function:>20}:{line:<3} | <level>{message}</level>", colorize=True, enqueue=True)


@app.command()
def vcpu_limits(quota_code="L-1216C47A"):
    """List the vCPU limits for each region."""

    def get_service_quota(region):
        service_quotas = AWSServer.get_boto3_client("service-quotas", region)
        response = service_quotas.get_service_quota(ServiceCode="ec2", QuotaCode=quota_code)
        return response["Quota"]["Value"]

    quotas = do_parallel(get_service_quota, AWSCloudProvider.region_list())
    for region, quota in quotas:
        typer.secho(f"{region}: {int(quota)}", fg="green")


@app.command()
def ssh(region: Optional[str] = None):
    aws = AWSCloudProvider()
    typer.secho("Querying AWS for instances", fg="green")
    instances = aws.get_matching_instances(region=None)
    if len(instances) == 0:
        typer.secho(f"No instancess found", fg="red")
        typer.Abort()

    instance_map = {f"{i.region()}, {i.public_ip()} ({i.instance_state()})": i for i in instances}
    choices = list(sorted(instance_map.keys()))
    instance_name: AWSServer = questionary.select("Select an instance", choices=choices).ask()
    if instance_name is not None and instance_name in instance_map:
        instance = instance_map[instance_name]
        proc = subprocess.Popen(split(f"ssh -i {str(instance.local_keyfile)} ubuntu@{instance.public_ip()}"))
        proc.wait()
    else:
        logger.secho(f"No instance selected", fg="red")


if __name__ == "__main__":
    app()
