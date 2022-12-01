import subprocess
from shlex import split
from typing import Optional

import typer
from rich.prompt import IntPrompt

import skyplane.cli.cli_cloud
import skyplane.cli.cli_config
import skyplane.cli.experiments
from skyplane import compute
from skyplane.cli.cli_init import init
from skyplane.cli.cli_transfer import cp, sync
from skyplane.cli.impl.common import query_instances
from skyplane.utils import logger
from skyplane.utils.fn import do_parallel

app = typer.Typer(name="skyplane")
app.command(
    name="cp",
    help="Copy files between any two cloud object stores",
)(cp)
app.command(
    name="sync",
    help="Sync files between any two cloud object stores",
)(sync)
app.command(
    name="init",
    help="Initialize the Skyplane CLI with your cloud credentials",
)(init)
app.add_typer(skyplane.cli.experiments.app, name="experiments")
app.add_typer(skyplane.cli.cli_cloud.app, name="cloud")
app.add_typer(skyplane.cli.cli_config.app, name="config")


@app.command()
def deprovision(
    all: bool = typer.Option(False, "--all", "-a", help="Deprovision all resources including networks."),
    filter_client_id: Optional[str] = typer.Option(None, help="Only deprovision instances with this client ID under the instance tag."),
):
    """Deprovision all resources created by skyplane."""
    instances = query_instances()
    if filter_client_id:
        instances = [instance for instance in instances if instance.tags().get("skyplaneclientid") == filter_client_id]

    if instances:
        typer.secho(f"Deprovisioning {len(instances)} instances", fg="yellow", bold=True)
        do_parallel(lambda instance: instance.terminate_instance(), instances, desc="Deprovisioning", spinner=True, spinner_persist=True)
    else:
        typer.secho("No instances to deprovision", fg="yellow", bold=True)

    if all:
        if compute.AWSAuthentication().enabled():
            aws = compute.AWSCloudProvider()
            aws.teardown_global()
        if compute.GCPAuthentication().enabled():
            gcp = compute.GCPCloudProvider()
            gcp.teardown_global()
        if compute.AzureAuthentication().enabled():
            azure = compute.AzureCloudProvider()
            azure.teardown_global()


@app.command()
def ssh():
    """SSH into a running gateway."""
    instances = query_instances()
    if len(instances) == 0:
        typer.secho(f"No instances found", fg="red", err=True)
        raise typer.Abort()

    instance_map = {f"{i.region_tag}, {i.public_ip()} ({i.instance_state()})": i for i in instances}
    choices = list(sorted(instance_map.keys()))

    # ask for selection
    typer.secho("Select an instance:", fg="yellow", bold=True)
    for i, choice in enumerate(choices):
        typer.secho(f"{i+1}) {choice}", fg="yellow")
    choice = IntPrompt.ask("Enter an instance number", choices=list([str(i) for i in range(1, len(choices) + 1)]), show_choices=False)
    instance = instance_map[choices[choice - 1]]

    # ssh
    cmd = instance.get_ssh_cmd()
    logger.info(f"Running SSH command: {cmd}")
    logger.info("It may ask for a private key password, try `skyplane`.")
    proc = subprocess.Popen(split(cmd))
    proc.wait()


typer_click_object = typer.main.get_command(app)

if __name__ == "__main__":
    app()
