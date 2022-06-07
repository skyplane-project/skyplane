import typer

from skyplane.cli.experiments.cli_profile import latency_grid, throughput_grid
from skyplane.cli.experiments.cli_query import get_max_throughput, util_grid_throughput, util_grid_cost, dump_full_util_cost_grid

app = typer.Typer(name="experiments")
app.command()(latency_grid)
app.command()(throughput_grid)
app.command()(get_max_throughput)
app.command()(util_grid_throughput)
app.command()(util_grid_cost)
app.command()(dump_full_util_cost_grid)
