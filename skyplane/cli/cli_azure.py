"""
Azure convenience interface
"""

from collections import defaultdict
from typing import List

import typer

from skyplane.compute.azure.azure_auth import AzureAuthentication
from skyplane.compute.azure.azure_cloud_provider import AzureCloudProvider
from skyplane.utils.fn import do_parallel

app = typer.Typer(name="skyplane-azure")


@app.command()
def get_valid_skus(
    regions: List[str] = typer.Option(AzureCloudProvider.region_list(), "--regions", "-r"),
    prefix: str = typer.Option("", "--prefix", help="Filter by prefix"),
    top_k: int = typer.Option(-1, "--top-k", help="Print top k entries"),
):
    auth = AzureAuthentication()
    client = auth.get_compute_client()

    def get_skus(region):
        valid_skus = []
        for sku in client.resource_skus.list(filter="location eq '{}'".format(region)):
            if len(sku.restrictions) == 0 and (not prefix or sku.name.startswith(prefix)):
                valid_skus.append(sku.name)
        return set(valid_skus)

    result = do_parallel(get_skus, regions, spinner=True, desc="Query SKUs")

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
