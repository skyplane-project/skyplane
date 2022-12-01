"""
Config interface:
* skyplane config list
* skyplane config get <key>
* skyplane config set <key> <value>

Available keys:
* autoconfirm (bool): Don't ask for confirmation when running a transfer
"""

import typer

from skyplane.cli.impl.common import console
from skyplane.api.usage import UsageClient
from skyplane.config_paths import config_path, cloud_config

app = typer.Typer(name="skyplane-config")


@app.command()
def list():
    """List all available config keys"""
    for key in cloud_config.valid_flags():
        value = cloud_config.get_flag(key)
        console.print(f"[bold][blue]{key}[/blue] = [italic][green]{value}[/italic][/green][/bold]")


@app.command()
def get(key: str):
    """Get a config value."""
    try:
        value = cloud_config.get_flag(key)
        console.print(f"[bold][blue]{key}[/blue] = [italic][green]{value}[/italic][/green]")
    except KeyError:
        console.print(f"[red][bold]{key}[/bold] is not a valid config key[/red]")


@app.command()
def set(key: str, value: str):
    """Set a config value."""
    try:
        old = cloud_config.get_flag(key)
    except KeyError:
        old = None
    if key == "usage_stats":
        UsageClient.set_usage_stats_via_config(value, cloud_config)
    else:
        try:
            cloud_config.set_flag(key, value)
        except KeyError:
            console.print(f"[red][bold]{key}[/bold] is not a valid config key[/red]")
            raise typer.Exit(code=1)
    new = cloud_config.get_flag(key)
    cloud_config.to_config_file(config_path)
    console.print(f"[bold][blue]{key}[/blue] = [italic][green]{new}[/italic][/green][/bold] [bright_black](was {old})[/bright_black]")
