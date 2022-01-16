import argparse

from loguru import logger
from tqdm import tqdm

from skylark.compute.aws.aws_cloud_provider import AWSCloudProvider
from skylark.compute.azure.azure_cloud_provider import AzureCloudProvider
from skylark.compute.gcp.gcp_cloud_provider import GCPCloudProvider
from skylark.compute.server import Server
from skylark.utils.utils import do_parallel


def stop_instance(instance: Server):
    instance.terminate_instance()
    tqdm.write(f"Terminated instance {instance}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Stop all instances")
    parser.add_argument("--disable-aws", action="store_true", help="Disables AWS operations if present")
    parser.add_argument("--gcp-project", type=str, help="GCP project", default=None)
    parser.add_argument("--azure-subscription", type=str, help="Microsoft Azure Subscription", default=None)
    args = parser.parse_args()

    instances = []

    if not args.disable_aws:
        logger.info("Getting matching AWS instances")
        aws = AWSCloudProvider()
        for _, instance_list in do_parallel(aws.get_matching_instances, aws.region_list(), progress_bar=True):
            instances += instance_list

    if args.gcp_project:
        logger.info("Getting matching GCP instances")
        gcp = GCPCloudProvider(gcp_project=args.gcp_project)
        instances += gcp.get_matching_instances()

    if args.azure_subscription:
        logger.info("Getting matching Azure instances")
        azure = AzureCloudProvider(azure_subscription=args.azure_subscription)
        instances += azure.get_matching_instances()

    do_parallel(stop_instance, instances, progress_bar=True)
