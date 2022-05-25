import logging
from pathlib import Path

import boto3
import typer

from skyplane import SkyplaneConfig, gcp_config_path
from skyplane.compute.aws.aws_auth import AWSAuthentication
from skyplane.compute.azure.azure_auth import AzureAuthentication
from skyplane.compute.gcp.gcp_auth import GCPAuthentication


def load_aws_config(config: SkyplaneConfig) -> SkyplaneConfig:
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
        typer.secho(f"    Loaded AWS credentials from the AWS CLI [IAM access key ID: ...{credentials_frozen.access_key[-6:]}]", fg="blue")
        config.aws_enabled = True
        auth.save_region_config(config)
        return config
    else:
        typer.secho("    AWS credentials not found in boto3 session, please use the AWS CLI to set them via `aws configure`", fg="red")
        typer.secho("    https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html", fg="red")
        typer.secho("    Disabling AWS support", fg="blue")
        auth.clear_region_config()
        return config


def load_azure_config(config: SkyplaneConfig, force_init: bool = False) -> SkyplaneConfig:
    if force_init:
        typer.secho("    Azure credentials will be re-initialized", fg="red")
        config.azure_subscription_id = None

    if config.azure_subscription_id:
        typer.secho("    Azure credentials already configured! To reconfigure Azure, run `skyplane init --reinit-azure`.", fg="blue")
        return config

    # check if Azure is enabled
    logging.disable(logging.WARNING)  # disable Azure logging, we have our own
    auth = AzureAuthentication(config=config)
    try:
        auth.credential.get_token("https://management.azure.com/")
        azure_enabled = True
    except:
        azure_enabled = False
    logging.disable(logging.NOTSET)  # reenable logging
    if not azure_enabled:
        typer.secho("    No local Azure credentials! Run `az login` to set them up.", fg="red")
        typer.secho("    https://docs.microsoft.com/en-us/azure/developer/python/azure-sdk-authenticate", fg="red")
        typer.secho("    Disabling Azure support", fg="blue")
        config.azure_enabled = False
        auth.save_region_config(config)
        return config
    typer.secho("    Azure credentials found in Azure CLI", fg="blue")
    inferred_subscription_id = AzureAuthentication.infer_subscription_id()
    if typer.confirm("    Azure credentials found, do you want to enable Azure support in Skyplane?", default=True):
        config.azure_subscription_id = typer.prompt("    Enter the Azure subscription ID:", default=inferred_subscription_id)
        config.azure_enabled = True
    else:
        config.azure_subscription_id = None
        typer.secho("    Disabling Azure support", fg="blue")
        config.azure_enabled = False
    auth.save_region_config(config)
    return config


def load_gcp_config(config: SkyplaneConfig, force_init: bool = False) -> SkyplaneConfig:
    if force_init:
        typer.secho("    GCP credentials will be re-initialized", fg="red")
        config.gcp_project_id = None
    elif not Path(gcp_config_path).is_file():
        typer.secho("    GCP region config missing! GCP will be reconfigured.", fg="red")
        config.gcp_project_id = None

    if config.gcp_project_id is not None:
        typer.secho("    GCP already configured! To reconfigure GCP, run `skyplane init --reinit-gcp`.", fg="blue")
        config.gcp_enabled = True
        return config

    # check if GCP is enabled
    inferred_cred, inferred_project = GCPAuthentication.get_adc_credential()
    if inferred_cred is None or inferred_project is None:
        typer.secho(
            "    Default GCP credentials are not set up yet. Run `gcloud auth application-default login`.",
            fg="red",
        )
        typer.secho("    https://cloud.google.com/docs/authentication/getting-started", fg="red")
        typer.secho("    Disabling GCP support", fg="blue")
        config.gcp_enabled = False
        GCPAuthentication.clear_region_config()
        return config
    else:
        typer.secho("    GCP credentials found in GCP CLI", fg="blue")
        if typer.confirm("    GCP credentials found, do you want to enable GCP support in Skyplane?", default=True):
            config.gcp_project_id = typer.prompt("    Enter the GCP project ID", default=inferred_project)
            assert config.gcp_project_id is not None, "GCP project ID must not be None"
            config.gcp_enabled = True
            auth = GCPAuthentication(config=config)
            auth.save_region_config()
            return config
        else:
            config.gcp_project_id = None
            typer.secho("    Disabling GCP support", fg="blue")
            config.gcp_enabled = False
            GCPAuthentication.clear_region_config()
            return config
