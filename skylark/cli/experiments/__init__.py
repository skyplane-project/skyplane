import typer
from skylark.cli.experiments.throughput import throughput_grid
from skylark.cli.experiments.util import get_max_throughput, util_grid_throughput, util_grid_cost

app = typer.Typer(name="experiments")
app.command()(throughput_grid)
app.command()(get_max_throughput)
app.command()(util_grid_throughput)
app.command()(util_grid_cost)
