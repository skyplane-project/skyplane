"""
AWS convenience interface
"""


from collections import defaultdict
from shlex import split
import subprocess
from typing import List, Optional
import questionary

import typer
from skylark.compute.azure.azure_auth import AzureAuthentication
from skylark.compute.azure.azure_cloud_provider import AzureCloudProvider
from skylark.compute.azure.azure_server import AzureServer
from skylark.utils.utils import do_parallel
from skylark.utils import logger

app = typer.Typer(name="skylark-azure")


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


@app.command()
def ssh(region: Optional[str] = None):
    azure = AzureCloudProvider()
    typer.secho("Querying Azure for instances", fg="green")
    instances = azure.get_matching_instances(region=region)
    if len(instances) == 0:
        typer.secho(f"No instances found", fg="red")
        raise typer.Abort()

    instance_map = {f"{i.region()}, {i.public_ip()} ({i.instance_state()})": i for i in instances}
    choices = list(sorted(instance_map.keys()))
    instance_name: AzureServer = questionary.select("Select an instance", choices=choices).ask()
    if instance_name is not None and instance_name in instance_map:
        instance = instance_map[instance_name]
        cmd = instance.get_ssh_cmd()
        logger.info(f"Running SSH command: {cmd}")
        logger.info("It may ask for a private key password, try `skylark`.")
        proc = subprocess.Popen(split(cmd))
        proc.wait()
    else:
        typer.secho(f"No instance selected", fg="red")
