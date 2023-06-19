"""
Cloud convenience interface
"""

import json
import subprocess
import time
from collections import defaultdict

import typer
from rich import print as rprint
from typing import List

from skyplane import compute
from skyplane.config_paths import cloud_config
from skyplane.obj_store.s3_interface import S3Interface
from skyplane.utils import logger
from skyplane.utils.definitions import GB
from skyplane.utils.fn import do_parallel
from skyplane.utils.timer import Timer

app = typer.Typer(name="skyplane-cloud")

# Common utils


def check_assert(condition, msg, debug_msg=None):
    if not condition:
        rprint(f"[red][bold]:x: Check failed:[/bold] {msg}[/red]")
        if debug_msg:
            rprint(f"[red]{debug_msg}[/red]")
        raise typer.Exit(1)
    else:
        rprint(f"[green][bold]:heavy_check_mark: Check passed:[/bold] {msg}[/green]")


def run_cmd(cmd, debug: bool):
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()
    if debug:
        rprint(f"[bright_black] $ {' '.join(cmd)} [bold]=> {process.returncode}[/bold][/bright_black]")
    return process.returncode, stdout.decode("utf-8"), stderr.decode("utf-8")


# AWS CLI tools


@app.command()
def aws_vcpu_limits(quota_code="L-1216C47A"):
    """List the vCPU limits for each region."""
    aws_auth = compute.AWSAuthentication()

    def get_service_quota(region):
        service_quotas = aws_auth.get_boto3_client("service-quotas", region)
        try:
            response = service_quotas.get_service_quota(ServiceCode="ec2", QuotaCode=quota_code)
        except Exception as e:
            logger.exception(e, print_traceback=False)
            logger.error(f"Failed to get service quota for {quota_code} in {region}")
            return -1
        return response["Quota"]["Value"]

    quotas = do_parallel(get_service_quota, compute.AWSCloudProvider.region_list())
    for region, quota in quotas:
        typer.secho(f"{region}: {int(quota)}", fg="green")


@app.command()
def aws_datasync(src_bucket: str, dst_bucket: str, path: str):
    aws_auth = compute.AWSAuthentication()
    src_region = S3Interface(src_bucket).aws_region
    dst_region = S3Interface(dst_bucket).aws_region

    iam_client = aws_auth.get_boto3_client("iam", "us-east-1")
    try:
        response = iam_client.get_role(RoleName="datasync-role")
        typer.secho("IAM role exists datasync-role", fg="green")
    except iam_client.exceptions.NoSuchEntityException:
        typer.secho("Creating datasync-role", fg="green")
        policy = {
            "Version": "2012-10-17",
            "Statement": [{"Effect": "Allow", "Principal": {"Service": "datasync.amazonaws.com"}, "Action": "sts:AssumeRole"}],
        }
        response = iam_client.create_role(RoleName="datasync-role", AssumeRolePolicyDocument=json.dumps(policy))
    iam_client.attach_role_policy(RoleName="datasync-role", PolicyArn="arn:aws:iam::aws:policy/AWSDataSyncFullAccess")
    # attach s3:ListBucket to datasync-role
    iam_client.attach_role_policy(RoleName="datasync-role", PolicyArn="arn:aws:iam::aws:policy/AmazonS3FullAccess")

    iam_arn = response["Role"]["Arn"]
    typer.secho(f"IAM role ARN: {iam_arn}", fg="green")

    # wait for role to be ready
    typer.secho("Waiting for IAM role to be ready", fg="green")
    iam_client.get_waiter("role_exists").wait(RoleName="datasync-role")

    ds_client_src = aws_auth.get_boto3_client("datasync", src_region)
    src_response = ds_client_src.create_location_s3(
        S3BucketArn=f"arn:aws:s3:::{src_bucket}", Subdirectory=path, S3Config={"BucketAccessRoleArn": iam_arn}
    )
    src_s3_arn = src_response["LocationArn"]
    ds_client_dst = aws_auth.get_boto3_client("datasync", dst_region)
    dst_response = ds_client_dst.create_location_s3(
        S3BucketArn=f"arn:aws:s3:::{dst_bucket}", Subdirectory=path, S3Config={"BucketAccessRoleArn": iam_arn}
    )
    dst_s3_arn = dst_response["LocationArn"]

    try:
        create_task_response = ds_client_dst.create_task(
            SourceLocationArn=src_s3_arn,
            DestinationLocationArn=dst_s3_arn,
            Name=f"{src_bucket}-{dst_bucket}-{path}",
            Options={"BytesPerSecond": -1, "OverwriteMode": "ALWAYS", "TransferMode": "ALL", "VerifyMode": "NONE"},
        )
        task_arn = create_task_response["TaskArn"]
    except ds_client_dst.exceptions.InvalidRequestException:
        typer.secho(f"Region not supported: {src_region} to {dst_region}", fg="red", err=True)
        raise typer.Abort()

    with Timer() as t:
        exec_response = ds_client_dst.start_task_execution(TaskArn=task_arn)
        task_execution_arn = exec_response["TaskExecutionArn"]

        def exit():
            task_execution_response = ds_client_dst.describe_task_execution(TaskExecutionArn=task_execution_arn)
            if task_execution_response["Status"] != "SUCCESS":
                ds_client_dst.cancel_task_execution(TaskExecutionArn=task_execution_arn)
                typer.secho("Cancelling task", fg="red", err=True)

        last_status = None
        try:
            while last_status != "SUCCESS":
                task_execution_response = ds_client_dst.describe_task_execution(TaskExecutionArn=task_execution_arn)
                last_status = task_execution_response["Status"]
                metadata_fields = ["EstimatedBytesToTransfer", "BytesWritten", "Result"]
                metadata = {k: v for k, v in task_execution_response.items() if k in metadata_fields}
                typer.secho(f"{int(t.elapsed)}s\tStatus: {last_status}, {metadata}", fg="green")
                time.sleep(5)
                if (int(t.elapsed) > 300) and last_status == "LAUNCHING":
                    typer.secho(
                        "The process might have errored out. Try deleting the objects if they exist already and restart the transfer.",
                        fg="red",
                        err=True,
                    )
        except KeyboardInterrupt:
            if last_status != "SUCCESS":
                exit()

    task_execution_response = ds_client_dst.describe_task_execution(TaskExecutionArn=task_execution_arn)
    transfer_size_gb = task_execution_response["BytesTransferred"] / GB
    transfer_duration_s = task_execution_response["Result"]["TransferDuration"] / 1000
    gbps = transfer_size_gb * 8 / transfer_duration_s
    typer.secho(f"DataSync response: {task_execution_response}", fg="green")
    typer.secho(
        json.dumps(dict(transfer_size_gb=transfer_size_gb, transfer_duration_s=transfer_duration_s, gbps=gbps, total_runtime_s=t.elapsed)),
        fg="white",
    )


@app.command()
def azure_check(
    account: str = typer.Argument(..., help="Storage account name"),
    container: str = typer.Argument(..., help="Container name"),
    debug: bool = typer.Option(False, "--debug", help="Print debug info"),
):
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
    retcode, stdout, stderr = run_cmd(["az", "--version"], debug=debug)
    check_assert(retcode == 0, "Azure CLI installed", debug_msg=stderr)
    if debug:
        rprint(f"[bright_black]Azure CLI version: {stdout.strip()}[/bright_black]")

    # check that azure cli is logged in
    retcode, stdout, stderr = run_cmd(["az", "account", "show"], debug=debug)
    check_assert(retcode == 0, "Azure CLI logged in", debug_msg=stderr)
    if debug:
        rprint(f"[bright_black]Azure CLI logged in as {stdout}[/bright_black]")

    # check that azure cli has correct subscription
    retcode, stdout, stderr = run_cmd(["az", "account", "show", "--query", "id"], debug=debug)
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
    retcode, stdout, stderr = run_cmd(cli.split(), debug=debug)
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
    auth = compute.AzureAuthentication(cloud_config)
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
    retcode, stdout, stderr = run_cmd(cli.split(), debug=debug)
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


@app.command()
def gcp_check(
    bucket: str = typer.Argument(..., help="GCP bucket to check access for"),
    debug: bool = typer.Option(False, "--debug", help="Print debug info"),
):
    hline = "=" * 80
    rprint(f"{hline}\n[bold]Checking Skyplane configuration...[/bold]\n{hline}")
    if debug:
        rprint(f"[bright_black]Skyplane config: {cloud_config}[/bright_black]")
    check_assert(cloud_config.gcp_project_id, "GCP project ID set")
    check_assert(cloud_config.gcp_enabled, "GCP enabled")
    if debug:
        rprint(f"[bright_black]GCP project ID: {cloud_config.gcp_project_id}[/bright_black]")

    # check that GCloud CLI works
    rprint(f"\n{hline}\n[bold]Checking GCloud CLI...[/bold]\n{hline}")
    cli = "gcloud auth list"
    retcode, stdout, stderr = run_cmd(cli.split(), debug=debug)
    check_assert(retcode == 0, "GCloud CLI authenticated", debug_msg=stderr)
    if debug:
        rprint(f"[bright_black]GCloud CLI auth list: {stdout}[/bright_black]")
    check_assert("ACTIVE" in stdout, "GCloud CLI authenticated")

    # check that GCP Python SDK works
    rprint(f"\n{hline}\n[bold]Checking GCP Python SDK...[/bold]\n{hline}")
    auth = compute.GCPAuthentication(cloud_config)
    if debug:
        rprint(f"[bright_black]GCP Python SDK auth: {auth}[/bright_black]")
    check_assert(auth, "GCP Python SDK auth created")
    cred = auth.credentials
    sa_cred = auth.service_account_credentials
    if debug:
        rprint(f"[bright_black]GCP Python SDK credentials: {cred}[/bright_black]")
        rprint(f"[bright_black]GCP Python SDK service account credentials: {sa_cred}[/bright_black]")
    check_assert(cred, "GCP Python SDK credentials created")
    check_assert(sa_cred, "GCP Python SDK service account credentials created")


@app.command()
def azure_get_valid_skus(
    regions: List[str] = typer.Option(compute.AzureCloudProvider.region_list(), "--regions", "-r"),
    prefix: str = typer.Option("", "--prefix", help="Filter by prefix"),
    top_k: int = typer.Option(-1, "--top-k", help="Print top k entries"),
):
    auth = compute.AzureAuthentication()
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
