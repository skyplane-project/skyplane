import argparse

from loguru import logger
from tqdm import tqdm

from skylark.compute.aws.aws_cloud_provider import AWSCloudProvider
from skylark.compute.gcp.gcp_cloud_provider import GCPCloudProvider
from skylark.compute.server import Server
from skylark.utils import do_parallel


def stop_instance(instance: Server):
    instance.terminate_instance()
    tqdm.write(f"Terminated instance {instance}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Stop all instances")
    parser.add_argument("--gcp-project", type=str, help="GCP project", required=True)
    args = parser.parse_args()

    logger.info("Getting matching AWS instances")
    aws = AWSCloudProvider()
    aws_instances = aws.get_matching_instances()
    logger.info("Getting matching GCP instances")
    gcp = GCPCloudProvider(gcp_project=args.gcp_project)
    gcp_instances = gcp.get_matching_instances()

    do_parallel(stop_instance, aws_instances + gcp_instances, progress_bar=True)
