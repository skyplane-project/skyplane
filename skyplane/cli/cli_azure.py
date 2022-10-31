"""
Azure convenience interface
"""

from collections import defaultdict
from typing import List
import subprocess
import json

import typer

from skyplane.compute.azure.azure_auth import AzureAuthentication
from skyplane.compute.azure.azure_cloud_provider import AzureCloudProvider
from skyplane.utils.fn import do_parallel
from skyplane.utils import logger
from skyplane import cloud_config
from rich import print as rprint

from skyplane.compute.azure.azure_auth import AzureAuthentication

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


@app.command()
def check(
    account: str = typer.Argument(..., help="Storage account name"),
    container: str = typer.Argument(..., help="Container name"),
    debug: bool = typer.Option(False, "--debug", help="Print debug info"),
):
    def check_assert(condition, msg, debug_msg=None):
        if not condition:
            rprint(f"[red][bold]:x: Check failed:[/bold] {msg}[/red]")
            if debug_msg:
                rprint(f"[red]{debug_msg}[/red]")
            raise typer.Exit(1)
        else:
            rprint(f"[green][bold]:heavy_check_mark: Check passed:[/bold] {msg}[/green]")

    def run_cmd(cmd):
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()
        if debug:
            rprint(f"[bright_black] $ {' '.join(cmd)} [bold]=> {process.returncode}[/bold][/bright_black]")
        return process.returncode, stdout.decode("utf-8"), stderr.decode("utf-8")

    hline = "=" * 80
    rprint(f"{hline}\n[bold]Checking Skyplane configuration...[/bold]\n{hline}")
    if debug:
        rprint(f"[bright_black]Skyplane config: {cloud_config}[/bright_black]")
    check_assert(cloud_config.azure_enabled, "Azure enabled in config")
    check_assert(cloud_config.azure_principal_id, "Azure principal ID set in config")
    check_assert(cloud_config.azure_subscription_id, "Azure subscription ID set in config")
    check_assert(cloud_config.azure_client_id, "Azure client ID set in config")

    rprint(f"\n{hline}\n[bold]Checking Azure CLI...[/bold]\n{hline}")
    # check that azure cli is installed
    retcode, stdout, stderr = run_cmd(["az", "--version"])
    check_assert(retcode == 0, "Azure CLI installed", debug_msg=stderr)
    if debug:
        rprint(f"[bright_black]Azure CLI version: {stdout.strip()}[/bright_black]")

    # check that azure cli is logged in
    retcode, stdout, stderr = run_cmd(["az", "account", "show"])
    check_assert(retcode == 0, "Azure CLI logged in", debug_msg=stderr)
    if debug:
        rprint(f"[bright_black]Azure CLI logged in as {stdout}[/bright_black]")

    # check that azure cli has correct subscription
    retcode, stdout, stderr = run_cmd(["az", "account", "show", "--query", "id"])
    if debug:
        rprint(f"[bright_black]Azure CLI subscription: {stdout.strip()}[/bright_black]")
    if debug:
        rprint(f"[bright_black]Skyplane subscription: {cloud_config.azure_subscription_id}[/bright_black]")
    check_assert(retcode == 0, "Azure CLI has subscription set", debug_msg=stderr)
    check_assert(stdout.replace('"', "").strip() == cloud_config.azure_subscription_id, "Azure CLI has correct subscription set")

    # check Azure UMIs
    rprint(f"\n{hline}\n[bold]Checking Azure UMIs...[/bold]\n{hline}")
    # list all UMIs with CLI
    cli = f"az identity list --resource-group skyplane"
    retcode, stdout, stderr = run_cmd(cli.split())
    check_assert(retcode == 0, "Azure CLI UMIs listed", debug_msg=stderr)
    if debug:
        rprint(f"[bright_black]Azure CLI UMIs: {stdout}[/bright_black]")
    # check Skyplane UMI is in list
    parsed = json.loads(stdout)
    matched_umi_idx = [i for i, umi in enumerate(parsed) if umi["name"] == "skyplane_umi"]
    check_assert(len(matched_umi_idx) == 1, f"Skyplane UMI exists")
    umi = parsed[matched_umi_idx[0]]
    if debug:
        rprint(f"[bright_black]Skyplane UMI: {umi}[/bright_black]")
    check_assert(umi["clientId"] == cloud_config.azure_client_id, "Skyplane UMI has correct client ID")
    check_assert(umi["principalId"] == cloud_config.azure_principal_id, "Skyplane UMI has correct principal ID")
    if debug:
        rprint(f"[bright_black]Skyplane UMI tenant ID: {umi['tenantId']}[/bright_black]")

    # check that Python SDK
    auth = AzureAuthentication(cloud_config)
    rprint(f"\n{hline}\n[bold]Checking Azure Python SDK...[/bold]\n{hline}")
    cred = auth.credential
    if debug:
        rprint(f"[bright_black]Azure Python SDK credential: {cred}[/bright_black]")
    check_assert(cred, "Azure Python SDK credential created")
    from azure.identity import DefaultAzureCredential

    check_assert(isinstance(cred, DefaultAzureCredential), "Azure Python SDK credential is DefaultAzureCredential")
    token = auth.get_token("https://storage.azure.com/.default")
    check_assert(token, "Azure Python SDK token created")

    # check that storage management client works
    rprint(f"\n{hline}\n[bold]Checking Azure storage management client...[/bold]\n{hline}")
    storage_client = auth.get_storage_management_client()
    if debug:
        rprint(f"[bright_black]Azure Python SDK storage client: {storage_client}[/bright_black]")
    check_assert(storage_client, "Azure Python SDK storage client created")
    from azure.mgmt.storage import StorageManagementClient

    check_assert(isinstance(storage_client, StorageManagementClient), "Azure Python SDK storage client is StorageManagementClient")
    storage_accounts = list(storage_client.storage_accounts.list())
    if debug:
        rprint(f"[bright_black]Azure Python SDK storage accounts: {[account.name for account in storage_accounts]}[/bright_black]")
    check_assert(storage_accounts, "Azure Python SDK storage accounts listed")
    account_idx = [i for i, a in enumerate(storage_accounts) if a.name == account]
    check_assert(len(account_idx) == 1, "Skyplane storage account exists")
    account_details = storage_accounts[account_idx[0]]
    if debug:
        rprint(f"[bright_black]Skyplane storage account: {account_details}[/bright_black]")
    account_subscription = account_details.id.split("/")[2]
    if debug:
        rprint(f"[bright_black]Skyplane storage account subscription: {account_subscription}[/bright_black]")

    # check UMI has access to storage account via Python SDK
    rprint(f"\n{hline}\n[bold]Checking Azure storage account access via UMI...[/bold]\n{hline}")
    # list UMI roles
    cli = f"az role assignment list --assignee {cloud_config.azure_principal_id} --all"
    retcode, stdout, stderr = run_cmd(cli.split())
    check_assert(retcode == 0, "Azure CLI UMI roles listed", debug_msg=stderr)
    if debug:
        rprint(f"[bright_black]Azure CLI UMI roles: {stdout}[/bright_black]")
    roles = json.loads(stdout)
    role_idx = [i for i, r in enumerate(roles) if r["scope"] == f"/subscriptions/{account_subscription}"]
    check_assert(len(role_idx) >= 1, "Skyplane storage account role assigned to UMI")
    role_names = [roles[i]["roleDefinitionName"] for i in role_idx]
    rprint(f"[bright_black]Skyplane storage account roles: {role_names}[/bright_black]")
    check_assert("Storage Blob Data Contributor" in role_names, "Skyplane storage account has Blob Data Contributor role assigned to UMI")
    check_assert("Storage Account Contributor" in role_names, "Skyplane storage account has Account Contributor role assigned to UMI")

    # check access to container via Python SDK
    rprint(f"\n{hline}\n[bold]Checking Azure container access[/bold]\n{hline}")
    container_client = auth.get_container_client(account, container)
    if debug:
        rprint(f"[bright_black]Azure Python SDK container client: {container_client}[/bright_black]")
    check_assert(container_client, "Azure Python SDK container client created")
    from azure.storage.blob import ContainerClient

    check_assert(isinstance(container_client, ContainerClient), "Azure Python SDK container client is ContainerClient")

    # check skyplane AzureBlobInterface
    rprint(f"\n{hline}\n[bold]Checking Skyplane AzureBlobInterface[/bold]\n{hline}")
    from skyplane.obj_store.azure_blob_interface import AzureBlobInterface

    iface = AzureBlobInterface(account, container)
    print(iface.container_client.get_container_properties())
