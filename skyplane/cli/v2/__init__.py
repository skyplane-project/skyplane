import typer

from skyplane.cli.v2.cp import cp

app = typer.Typer(name="skyplane-v2")
app.command(
    name="cp",
    help="Copy files between any two cloud object stores",
)(cp)

__all__ = ["app"]
