"""
AWS convenience interface
"""


import atexit
from collections import Counter, defaultdict
import json
import os
from pathlib import Path
import sys
from typing import List, Optional

import azure.core.exceptions
from azure.identity import DefaultAzureCredential
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.network import NetworkManagementClient
from azure.mgmt.compute import ComputeManagementClient
import typer
from loguru import logger

from skylark import GB, MB, print_header
from skylark.cli.cli_helper import (
    copy_local_local,
    copy_local_s3,
    copy_s3_local,
    deprovision_skylark_instances,
    ls_local,
    ls_s3,
    parse_path,
)
from skylark.utils.utils import do_parallel
from skylark.compute.aws.aws_server import AWSServer
from skylark.replicate.replication_plan import ReplicationJob, ReplicationTopology
from skylark.replicate.replicator_client import ReplicatorClient
from skylark.compute.aws.aws_cloud_provider import AWSCloudProvider
from skylark.compute.azure.azure_cloud_provider import AzureCloudProvider

app = typer.Typer(name="skylark")

# config logger
logger.remove()
logger.add(sys.stderr, format="{function:>20}:{line:<3} | <level>{message}</level>", colorize=True, enqueue=True)


@app.command()
def get_valid_skus(
    azure_subscription: str = typer.Option("", "--azure-subscription", help="Azure subscription ID"),
    regions: List[str] = typer.Option(AzureCloudProvider.region_list(), "--regions", "-r"),
    prefix: str = typer.Option("", "--prefix", help="Filter by prefix"),
    top_k: int = typer.Option(-1, "--top-k", help="Print top k entries"),
):
    credential = DefaultAzureCredential()
    client = ResourceManagementClient(credential, azure_subscription)

    # query azure API for each region to get available SKUs for each resource type
    def get_skus(region):
        client = ComputeManagementClient(credential, azure_subscription)
        valid_skus = []
        for sku in client.resource_skus.list(filter="location eq '{}'".format(region)):
            if len(sku.restrictions) == 0 and (not prefix or sku.name.startswith(prefix)):
                valid_skus.append(sku.name)
        return set(valid_skus)
    
    result = do_parallel(get_skus, regions, progress_bar=True, leave_pbar=False, desc="Query SKUs")
    
    sku_regions = defaultdict(set)
    for region, skus in result:
        for sku in skus:
            sku_regions[sku].add(region)
    
    # print top-k entries (if not -1)
    sorted_top_keys = sorted(sku_regions.keys(), key=lambda x: len(sku_regions[x]), reverse=True)
    if top_k > 0:
        sorted_top_keys = sorted_top_keys[:top_k]
    for sku in sorted_top_keys:
        typer.secho(f"{sku} in {len(sku_regions[sku])} regions: {', '.join(sorted(sku_regions[sku]))}")
        


if __name__ == "__main__":
    app()
