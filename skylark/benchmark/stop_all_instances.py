import argparse

from loguru import logger

from skylark.compute.aws.aws_cloud_provider import AWSCloudProvider
from skylark.compute.gcp.gcp_cloud_provider import GCPCloudProvider
from skylark.compute.server import Server
from skylark.utils import do_parallel


def stop_instance(instance: Server):
    instance.terminate_instance()
    logger.debug(f"Terminated instance {instance}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Stop all instances")
    parser.add_argument("--gcp-project", type=str, default=None, help="GCP project")
    args = parser.parse_args()

    aws = AWSCloudProvider()
    aws_instances = aws.get_matching_instances()
    gcp = GCPCloudProvider(gcp_project=args.gcp_project)
    gcp_instances = gcp.get_matching_instances()

    do_parallel(stop_instance, aws_instances + gcp_instances, progress_bar=True)
