import json
import os
import shutil
import subprocess
import traceback
from pathlib import Path

import questionary
import typer
from rich.progress import Progress, SpinnerColumn, TextColumn
from typing import List

from skyplane import compute
from skyplane.config import SkyplaneConfig
from skyplane.config_paths import aws_config_path, gcp_config_path


def load_aws_config(config: SkyplaneConfig, non_interactive: bool = False) -> SkyplaneConfig:
    try:
        import boto3
    except ImportError:
        config.aws_enabled = False
        typer.secho("    AWS support disabled because boto3 is not installed. Run `pip install skyplane[aws].`", fg="red", err=True)
        return config
    if non_interactive or typer.confirm("    Do you want to configure AWS support in Skyplane?", default=True):
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

        auth = compute.AWSAuthentication(config=config)
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
        config.azure_client_id = None
        config.azure_principal_id = None
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

    def run_az_cmd(cmd: List[str]):
        out, err = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
        if err:
            typer.secho(f"    Error running command: {cmd}", fg="red", err=True)
            typer.secho(f"    stdout: {out.decode('utf-8')}", fg="red", err=True)
            typer.secho(f"    stderr: {err.decode('utf-8')}", fg="red", err=True)
            return False, out, err
        return True, out, err

    if non_interactive or typer.confirm("    Do you want to configure Azure support in Skyplane?", default=True):
        if force_init:
            typer.secho("    Azure credentials will be re-initialized", fg="red", err=True)
            clear_azure_config(config, verbose=False)
        if config.azure_enabled and config.azure_subscription_id and config.azure_principal_id and config.azure_client_id:
            typer.secho("    Azure credentials already configured! To reconfigure Azure, run `skyplane init --reinit-azure`.", fg="blue")
            return config

        # check if az cli is installed
        if not shutil.which("az"):
            typer.secho(
                "    Azure CLI not found, please install it from https://docs.microsoft.com/en-us/cli/azure/install-azure-cli",
                fg="red",
                err=True,
            )
            return clear_azure_config(config)

        # load credentials from environment variables or input
        defaults = {
            "client_id": os.environ.get("AZURE_CLIENT_ID") or config.azure_client_id,
            "subscription_id": os.environ.get("AZURE_SUBSCRIPTION_ID")
            or config.azure_subscription_id
            or compute.AzureAuthentication.infer_subscription_id(),
            "resource_group": os.environ.get("AZURE_RESOURCE_GROUP") or compute.AzureServer.resource_group_name,
        }

        # check if the az CLI is installed
        out, err = subprocess.Popen("az --version".split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
        if not out.decode("utf-8").startswith("azure-cli"):
            typer.secho(
                "    Azure CLI not found, please install it from https://docs.microsoft.com/en-us/cli/azure/install-azure-cli",
                fg="red",
                err=True,
            )
            return clear_azure_config(config)

        # query list of subscriptions
        success, out, err = run_az_cmd("az account list -o json --all".split(" "))
        if not success:
            typer.secho("    Error listing Azure subscriptions", fg="red", err=True)
            return clear_azure_config(config)
        subscriptions = {}
        for sub in json.loads(out):
            if sub["state"] == "Enabled":
                subscriptions[sub["name"]] = sub["id"]
        defaults["subscription_name"] = (
            next((n for n, i in subscriptions.items() if i == defaults["subscription_id"]), None) if defaults["subscription_id"] else None
        )

        # select subscription to launch Skyplane VMs in
        if non_interactive:
            config.azure_subscription_id = defaults["subscription_id"]
        else:
            choices = {f"{name} ({id})": id for name, id in subscriptions.items()}
            default_choice = f"{defaults['subscription_name']} ({defaults['subscription_id']})" if defaults["subscription_id"] else None
            selected_choice = questionary.select(
                "Select Azure subscription to launch Skyplane VMs in:",
                choices=list(sorted(choices.keys())),
                default=default_choice,
                qmark="    ?",
                pointer="    > ",
            ).ask()
            if selected_choice is None:
                typer.secho("    No subscription selected, disabling Azure support", fg="blue")
                return clear_azure_config(config)
            config.azure_subscription_id = choices[selected_choice]

        if not config.azure_subscription_id:
            typer.secho("    Invalid Azure subscription ID", fg="red", err=True)
            return clear_azure_config(config)

        # ask user which subscriptions Skyplane should be able to read/write to
        # in order to move data to or from that storage account, they must select it
        if non_interactive:
            authorize_subscriptions_ids = [config.azure_subscription_id]
        else:
            choices = {f"{name} ({id})": id for name, id in subscriptions.items()}
            default_choice = f"{defaults['subscription_name']} ({defaults['subscription_id']})" if defaults["subscription_id"] else None
            authorize_subscription_strs = questionary.checkbox(
                "Select which Azure subscriptions that Skyplane should be able to read/write data to",
                choices=list(sorted(choices.keys())),
                default=default_choice,
                qmark="    ?",
                pointer="    > ",
            ).ask()
            if not authorize_subscription_strs:
                typer.secho(
                    "    Note: Skyplane will not be able to read/write data to any Azure subscriptions so you will not be able to use Azure storage.",
                    fg="red",
                )
                authorize_subscriptions_ids = []
            else:
                authorize_subscriptions_ids = [choices[s] for s in authorize_subscription_strs]

        change_subscription_cmd = f"az account set --subscription {config.azure_subscription_id}"
        create_rg_cmd = f"az group create -l westus2 -n {compute.AzureServer.resource_group_name}"
        create_umi_cmd = f"az identity create -g skyplane -n skyplane_umi"
        typer.secho(f"    I will run the following commands to create an Azure managed identity:", fg="blue")
        typer.secho(f"        $ {change_subscription_cmd}", fg="yellow")
        typer.secho(f"        $ {create_rg_cmd}", fg="yellow")
        typer.secho(f"        $ {create_umi_cmd}", fg="yellow")

        with Progress(
            TextColumn("    "), SpinnerColumn(), TextColumn("Creating Skyplane managed identity{task.description}"), transient=True
        ) as progress:
            progress.add_task("", total=None)
            cmd_success, out, err = run_az_cmd(change_subscription_cmd.split())
            if not cmd_success:
                return clear_azure_config(config)
            cmd_success, out, err = run_az_cmd(create_rg_cmd.split())
            if not cmd_success:
                return clear_azure_config(config)
            cmd_success, out, err = run_az_cmd(create_umi_cmd.split())
            if not cmd_success:
                return clear_azure_config(config)
            else:
                identity_json = json.loads(out.decode("utf-8"))
                config.azure_client_id = identity_json["clientId"]
                config.azure_principal_id = identity_json["principalId"]

        if not config.azure_client_id or not config.azure_principal_id or not config.azure_subscription_id:
            typer.secho("    Azure credentials not configured correctly, disabling Azure support.", fg="red", err=True)
            return clear_azure_config(config)

        # authorize new managed identity with Storage Blob Data Contributor and Storage Account Contributor roles to the subscription
        role_cmds = []
        for subscription_id in authorize_subscriptions_ids:
            role_cmds.extend(make_role_cmds(config.azure_principal_id, subscription_id))

        if role_cmds:
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
                    cmd_success, out, err = run_az_cmd(role_cmd)
                    if not cmd_success:
                        return clear_azure_config(config)
        typer.secho(
            f"    Azure managed identity created successfully! To delete it, run `az identity delete -n skyplane_umi -g skyplane`.",
            fg="green",
        )

        config.azure_enabled = True
        auth = compute.AzureAuthentication(config=config)
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


def check_gcp_service(gcp_auth: compute.GCPAuthentication, non_interactive: bool = False):
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
        compute.GCPAuthentication.clear_region_config()
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
        inferred_cred, inferred_project = compute.GCPAuthentication.get_adc_credential()
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
                auth = compute.GCPAuthentication(config=config)
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
