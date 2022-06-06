"""
Config interface:
* skyplane config get <key>
* skyplane config set <key> <value>

Available keys:
* autoconfirm (bool): Don't ask for confirmation when running a transfer
"""

import typer

from skyplane import cloud_config, config_path
from skyplane.cli.common import console

app = typer.Typer(name="skyplane-config")


@app.command()
def get(key: str):
    """Get a config value."""
    try:
        console.print(f"[bold][blue]{key}[/blue] = [green]{cloud_config.get_flag(key)}[/green][/bold]")
    except KeyError:
        console.print(f"[red][bold]{key}[/bold] is not a valid config key[/red]")


@app.command()
def set(key: str, value: str):
    """Set a config value."""
    try:
        old = cloud_config.get_flag(key)
    except KeyError:
        old = None
    cloud_config.set_flag(key, value)
    new = cloud_config.get_flag(key)
    cloud_config.to_config_file(config_path)
    console.print(f"[bold][blue]{key}[/blue] = [green]{new}[/green][/bold] [bright_black](was {old})[/bright_black]")
