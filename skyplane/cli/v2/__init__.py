import typer

from skyplane.cli.v2.cp import cp, sync

app = typer.Typer(name="skyplane-v2")
app.command(
    name="cp",
    help="Copy files between any two cloud object stores",
)(cp)
app.command(
    name="sync",
    help="Sync files between any two cloud object stores",
)(sync)


__all__ = ["app"]
