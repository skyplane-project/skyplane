"""
AWS convenience interface
"""


import sys

import typer
from loguru import logger
from skylark.utils.utils import do_parallel
from skylark.compute.aws.aws_server import AWSServer
from skylark.compute.aws.aws_cloud_provider import AWSCloudProvider

app = typer.Typer(name="skylark")

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


if __name__ == "__main__":
    app()
