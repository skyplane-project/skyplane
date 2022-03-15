import subprocess
from shlex import split
from typing import Optional

import questionary
import typer

from skylark.utils import logger
from skylark.compute.gcp.gcp_cloud_provider import GCPCloudProvider
from skylark.compute.gcp.gcp_server import GCPServer

app = typer.Typer(name="skylark-gcp")


@app.command()
def ssh(region: Optional[str] = None):
    gcp = GCPCloudProvider()
    typer.secho("Querying GCP for instances", fg="green")
    instances = gcp.get_matching_instances(region=region)
    if len(instances) == 0:
        typer.secho(f"No instances found", fg="red")
        raise typer.Abort()

    instance_map = {f"{i.region()}, {i.public_ip()} ({i.instance_state()})": i for i in instances}
    choices = list(sorted(instance_map.keys()))
    instance_name: GCPServer = questionary.select("Select an instance", choices=choices).ask()
    if instance_name is not None and instance_name in instance_map:
        cmd = instance_map[instance_name].get_ssh_cmd()
        typer.secho(cmd, fg="green")
        proc = subprocess.Popen(split(cmd))
        proc.wait()
    else:
        typer.secho(f"No instance selected", fg="red")
