"""
AWS convenience interface
"""


from collections import defaultdict
from typing import List

import typer
from azure.identity import DefaultAzureCredential
from azure.mgmt.compute import ComputeManagementClient
from skylark.cli.cli_helper import load_config
from skylark.compute.azure.azure_cloud_provider import AzureCloudProvider
from skylark.utils.utils import do_parallel

app = typer.Typer(name="skylark-azure")


@app.command()
def get_valid_skus(
    azure_subscription: str = typer.Option("", "--azure-subscription", help="Azure subscription ID"),
    regions: List[str] = typer.Option(AzureCloudProvider.region_list(), "--regions", "-r"),
    prefix: str = typer.Option("", "--prefix", help="Filter by prefix"),
    top_k: int = typer.Option(-1, "--top-k", help="Print top k entries"),
):
    config = load_config()
    azure_subscription = azure_subscription or config.get("azure_subscription_id")
    typer.secho(f"Loaded from config file: azure_subscription={azure_subscription}", fg="blue")

    credential = DefaultAzureCredential()

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
        typer.secho(f"{sku} in {len(sku_regions[sku])} regions: {list(sorted(sku_regions[sku]))}")


if __name__ == "__main__":
    app()
