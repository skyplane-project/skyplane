import json
import os
from pathlib import Path
import subprocess
import traceback

import boto3
import typer
from rich.progress import Progress, SpinnerColumn, TextColumn

from skyplane import SkyplaneConfig, gcp_config_path, aws_config_path
from skyplane.compute.aws.aws_auth import AWSAuthentication
from skyplane.compute.azure.azure_auth import AzureAuthentication
from skyplane.compute.gcp.gcp_auth import GCPAuthentication

from skyplane.compute.azure.azure_server import AzureServer


def load_aws_config(config: SkyplaneConfig, non_interactive: bool = False) -> SkyplaneConfig:
    if non_interactive or typer.confirm("    Do you want to configure AWS support in Skyplane?", default=True):
        # get AWS credentials from boto3
        session = boto3.Session()
        credentials_session = session.get_credentials()
        if credentials_session is None:
            config.aws_enabled = False
        else:
            credentials_frozen = credentials_session.get_frozen_credentials()
            if credentials_frozen.access_key is None or credentials_frozen.secret_key is None:
                config.aws_enabled = False
            else:
                config.aws_enabled = True

        auth = AWSAuthentication(config=config)
        if config.aws_enabled:
            typer.secho(
                f"    Loaded AWS credentials from the AWS CLI [IAM access key ID: ...{credentials_frozen.access_key[-6:]}]", fg="blue"
            )
            config.aws_enabled = True
            auth.save_region_config(config)
            typer.secho(f"    AWS region config file saved to {aws_config_path}", fg="blue")
            return config
        else:
            typer.secho(
                "    AWS credentials not found in boto3 session, please use the AWS CLI to set them via `aws configure`", fg="red", err=True
            )
            typer.secho("    https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html", fg="red", err=True)
            typer.secho("    Disabling AWS support", fg="blue")
            if auth is not None:
                auth.clear_region_config()
            return config
    else:
        config.aws_enabled = False
        typer.secho("    Disabling AWS support", fg="blue")
        return config


def load_azure_config(config: SkyplaneConfig, force_init: bool = False, non_interactive: bool = False) -> SkyplaneConfig:
    def clear_azure_config(config, verbose=True):
        if verbose:
            typer.secho("    Disabling Azure support", fg="blue")
        config.azure_subscription_id = None
        config.azure_tenant_id = None
        config.azure_client_id = None
        config.azure_client_secret = None
        config.azure_enabled = False
        return config

    def make_role_cmds(principal_id, subscription_id):
        roles = ["Contributor", "Storage Blob Data Contributor", "Storage Account Contributor"]
        return [
            "az role assignment create --role".split(" ")
            + [role]
            + f"--assignee-object-id {principal_id} --assignee-principal-type ServicePrincipal".split(" ")
            + f"--subscription {subscription_id}".split(" ")
            for role in roles
        ]

    if non_interactive or typer.confirm("    Do you want to configure Azure support in Skyplane?", default=True):
        if force_init:
            typer.secho("    Azure credentials will be re-initialized", fg="red", err=True)
            clear_azure_config(config, verbose=False)
        if (
            config.azure_enabled
            and config.azure_subscription_id
            and config.azure_tenant_id
            and config.azure_client_id
            and config.azure_client_secret
        ):
            typer.secho("    Azure credentials already configured! To reconfigure Azure, run `skyplane init --reinit-azure`.", fg="blue")
            return config

        # load credentials from environment variables or input
        inferred_subscription_id = (
            os.environ.get("AZURE_SUBSCRIPTION_ID") or config.azure_subscription_id or AzureAuthentication.infer_subscription_id()
        )
        defaults = {
            "tenant_id": os.environ.get("AZURE_TENANT_ID") or config.azure_tenant_id,
            "client_id": os.environ.get("AZURE_CLIENT_ID") or config.azure_client_id,
            "client_secret": os.environ.get("AZURE_CLIENT_SECRET") or config.azure_client_secret,
            "subscription_id": inferred_subscription_id,
            "resource_group": os.environ.get("AZURE_RESOURCE_GROUP") or AzureServer.resource_group_name,
        }
        create_rg_cmd = "az group create -l westus2 -n skyplane"
        out, err = subprocess.Popen(create_rg_cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
        if err:
            typer.secho(f"    Error running command: {create_rg_cmd}", fg="red", err=True)
            typer.secho(f"    stdout: {out.decode('utf-8')}", fg="red", err=True)
            typer.secho(f"    stderr: {err.decode('utf-8')}", fg="red", err=True)
            return clear_azure_config(config)

        config.azure_subscription_id = (
            typer.prompt("    Which Azure subscription ID do you want to use?", default=defaults["subscription_id"])
            if not non_interactive
            else defaults["subscription_id"]
        )
        if not config.azure_subscription_id:
            typer.secho("    Invalid Azure subscription ID", fg="red", err=True)
            return clear_azure_config(config)

        change_subscription_cmd = f"az account set --subscription {config.azure_subscription_id}"
        create_umi_cmd = f"az identity create -g skyplane -n skyplane_umi"
        typer.secho(f"    I will run the following commands to create an Azure managed identity:", fg="blue")
        typer.secho(f"        $ {change_subscription_cmd}", fg="yellow")
        typer.secho(f"        $ {create_umi_cmd}", fg="yellow")

        with Progress(
            TextColumn("    "), SpinnerColumn(), TextColumn("Creating Skyplane managed identity{task.description}"), transient=True
        ) as progress:
            progress.add_task("", total=None)
            out, err = subprocess.Popen(change_subscription_cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
            if out or err:
                typer.secho(f"    Error running command: {change_subscription_cmd}", fg="red", err=True)
                typer.secho(f"    stdout: {out.decode('utf-8')}", fg="red", err=True)
                typer.secho(f"    stderr: {err.decode('utf-8')}", fg="red", err=True)
                return clear_azure_config(config)

            out, err = subprocess.Popen(create_umi_cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
            try:
                identity_json = json.loads(out.decode("utf-8"))
            except:
                typer.secho(f"    Error running command: {create_umi_cmd}", fg="red", err=True)
                typer.secho(f"    stdout: {out.decode('utf-8')}", fg="red", err=True)
                typer.secho(f"    stderr: {err.decode('utf-8')}", fg="red", err=True)
                return clear_azure_config(config)
            config.azure_client_id = identity_json["clientId"]
            config.azure_principal_id = identity_json["principalId"]

        if not config.azure_client_id or not config.azure_principal_id or not config.azure_subscription_id:
            typer.secho("    Azure credentials not configured correctly, disabling Azure support.", fg="red", err=True)
            return clear_azure_config(config)

        # authorize new managed identity with Storage Blob Data Contributor and Storage Account Contributor roles to the subscription
        role_cmds = make_role_cmds(config.azure_principal_id, config.azure_subscription_id)
        typer.secho(
            f"    I will run the following commands to authorize the newly created Skyplane managed identity to access your storage accounts:",
            fg="blue",
        )
        for role_cmd in role_cmds:
            typer.secho(f"        $ {' '.join(role_cmd)}", fg="yellow")

        with Progress(
            TextColumn("    "),
            SpinnerColumn(),
            TextColumn("Authorizing managed identity to access storage accounts{task.description}"),
            transient=True,
        ) as progress:
            progress.add_task("", total=None)
            for role_cmd in role_cmds:
                out, err = subprocess.Popen(role_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
                if err:
                    typer.secho(f"    Error running command: {role_cmd}", fg="red", err=True)
                    typer.secho(f"    stdout: {out.decode('utf-8')}", fg="red", err=True)
                    typer.secho(f"    stderr: {err.decode('utf-8')}", fg="red", err=True)
                    return clear_azure_config(config)
        typer.secho(
            f"    Azure managed identity created successfully! To delete it, run `az identity delete -n skyplane_umi -g skyplane`.",
            fg="green",
        )

        config.azure_enabled = True
        auth = AzureAuthentication(config=config)
        with Progress(
            TextColumn("    "),
            SpinnerColumn(),
            TextColumn("Querying Azure for available regions and VM SKUs{task.description}"),
            transient=True,
        ) as progress:
            progress.add_task("", total=None)
            auth.save_region_config()
        return config
    else:
        return clear_azure_config(config)


def check_gcp_service(gcp_auth: GCPAuthentication, non_interactive: bool = False):
    services = {"iam": "IAM", "compute": "Compute Engine", "storage": "Storage", "cloudresourcemanager": "Cloud Resource Manager"}
    for service, name in services.items():
        if not gcp_auth.check_api_enabled(service):
            typer.secho(f"    GCP {name} API not enabled", fg="red", err=True)
            if non_interactive or typer.confirm(f"    Do you want to enable the {name} API?", default=True):
                gcp_auth.enable_api(service)
                typer.secho(f"    Enabled GCP {name} API", fg="blue")
            else:
                return False
    return True


def load_gcp_config(config: SkyplaneConfig, force_init: bool = False, non_interactive: bool = False) -> SkyplaneConfig:
    def disable_gcp_support():
        typer.secho("    Disabling Google Cloud support", fg="blue")
        config.gcp_enabled = False
        config.gcp_project_id = None
        GCPAuthentication.clear_region_config()
        return config

    if non_interactive or typer.confirm("    Do you want to configure GCP support in Skyplane?", default=True):
        if force_init:
            typer.secho("    GCP credentials will be re-initialized", fg="red", err=True)
            config.gcp_project_id = None
        elif not Path(gcp_config_path).is_file():
            typer.secho("    GCP region config missing! GCP will be reconfigured.", fg="red", err=True)
            config.gcp_project_id = None

        if config.gcp_project_id is not None:
            typer.secho("    GCP already configured! To reconfigure GCP, run `skyplane init --reinit-gcp`.", fg="blue")
            config.gcp_enabled = True
            return config

        # check if GCP is enabled
        inferred_cred, inferred_project = GCPAuthentication.get_adc_credential()
        if inferred_cred is None or inferred_project is None:
            typer.secho("    Default GCP credentials are not set up yet. Run `gcloud auth application-default login`.", fg="red", err=True)
            typer.secho("    https://cloud.google.com/docs/authentication/getting-started", fg="red", err=True)
            return disable_gcp_support()
        else:
            typer.secho("    GCP credentials found in GCP CLI", fg="blue")
            if non_interactive or typer.confirm("    GCP credentials found, do you want to enable GCP support in Skyplane?", default=True):
                if not non_interactive:
                    config.gcp_project_id = typer.prompt("    Enter the GCP project ID", default=inferred_project)
                else:
                    config.gcp_project_id = inferred_project
                assert config.gcp_project_id is not None, "GCP project ID must not be None"
                config.gcp_enabled = True
                auth = GCPAuthentication(config=config)
                typer.secho(f"    Using GCP service account {auth.service_account_name}", fg="blue")
                if not check_gcp_service(auth, non_interactive):
                    return disable_gcp_support()
                try:
                    auth.save_region_config()
                except Exception as e:
                    typer.secho(f"    Error saving GCP region config", fg="red", err=True)
                    typer.secho(f"    {e}\n{traceback.format_exc()}", fg="red", err=True)
                    return disable_gcp_support()
                return config
            else:
                return disable_gcp_support()
    else:
        return disable_gcp_support()
