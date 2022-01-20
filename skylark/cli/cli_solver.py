"""
AWS convenience interface
"""

import argparse
from pathlib import Path
import sys

import cvxpy as cp

# import typer
from loguru import logger
from skylark.replicate.solver import ThroughputSolverILP
from skylark.utils.utils import do_parallel
from skylark import skylark_root

# app = typer.Typer(name="skylark-solver")

# config logger
logger.remove()
logger.add(sys.stderr, format="{function:>20}:{line:<3} | <level>{message}</level>", colorize=True, enqueue=True)


# @app.command()
# def solve_throughput(
#     src: str = typer.Argument(..., help="Source region, in format of provider:region."),
#     dst: str = typer.Argument(..., help="Destination region, in format of provider:region."),
#     required_throughput_gbits: float = typer.Argument(..., help="Required throughput in gbps."),
#     max_instances: int = typer.Option(1, help="Max number of instances per overlay region."),
#     throughput_grid: Path = typer.Option(
#         skylark_root / "profiles" / "throughput_mini.csv", "--throughput-grid", help="Throughput grid file"
#     ),
#     solver_verbose: bool = False,
# ):
def solve_throughput(
    src: str,
    dst: str,
    gbyte_to_transfer: float,
    required_throughput_gbits: float,
    max_instances: int,
    throughput_grid: Path = skylark_root / "profiles" / "throughput_mini.csv",
    solver_verbose: bool = False,
):
    # build problem and solve
    tput = ThroughputSolverILP(throughput_grid)
    solution = tput.solve_min_cost(
        src,
        dst,
        required_throughput_gbits=required_throughput_gbits,
        gbyte_to_transfer=gbyte_to_transfer,
        instance_limit=max_instances,
        solver=cp.CBC,
        solver_verbose=solver_verbose,
    )

    # save results
    tput.print_solution(solution)
    if solution["feasible"]:
        g = tput.plot_graphviz(solution)
        if g is not None:
            g.render(filename="/tmp/throughput_graph.gv", quiet_view=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--src", type=str, required=True)
    parser.add_argument("--dst", type=str, required=True)
    parser.add_argument("--gbyte-to-transfer", type=float, default=1)
    parser.add_argument("--min-throughput", type=float, default=1)
    parser.add_argument("--num-instances", type=int, default=1)
    args = parser.parse_args()
    solve_throughput(
        args.src,
        args.dst,
        args.gbyte_to_transfer,
        args.min_throughput,
        args.num_instances,
    )
