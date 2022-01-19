"""
AWS convenience interface
"""


from pathlib import Path
import sys
from collections import defaultdict
from typing import List

import cvxpy as cp
import typer
from loguru import logger
from skylark.cli.cli_helper import load_config
from skylark.cli.experiments import throughput
from skylark.compute.azure.azure_cloud_provider import AzureCloudProvider
from skylark.replicate.solver import ThroughputSolverILP
from skylark.utils.utils import do_parallel
from skylark import skylark_root

app = typer.Typer(name="skylark-solver")

# config logger
logger.remove()
logger.add(sys.stderr, format="{function:>20}:{line:<3} | <level>{message}</level>", colorize=True, enqueue=True)


@app.command()
def solve_throughput(
    src: str = typer.Argument(..., help="Source region, in format of provider:region."),
    dst: str = typer.Argument(..., help="Destination region, in format of provider:region."),
    required_throughput_gbits: float = typer.Argument(..., help="Required throughput in gbps."),
    max_instances: int = typer.Option(1, help="Max number of instances per overlay region."),
    throughput_grid: Path = typer.Option(
        skylark_root / "profiles" / "throughput_mini.csv", "--throughput-grid", help="Throughput grid file"
    ),
    solver_verbose: bool = False,
):
    # build problem and solve
    tput = ThroughputSolverILP(throughput_grid)
    solution = tput.solve(
        src,
        dst,
        required_throughput_gbits=required_throughput_gbits,
        cost_limit=None,
        solver=cp.CBC,
        solver_verbose=solver_verbose,
    )

    # save results
    tput.print_solution(solution)
    if solution["feasible"]:
        g = tput.plot_graphviz(solution)
        if g is not None:
            g.render(filename="/tmp/throughput_graph.gv")


if __name__ == "__main__":
    app()
