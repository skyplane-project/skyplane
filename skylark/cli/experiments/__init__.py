import typer
from skylark.cli.experiments.profile import latency_grid, throughput_grid

app = typer.Typer(name="experiments")
app.command()(latency_grid)
app.command()(throughput_grid)
