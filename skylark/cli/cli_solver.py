"""
AWS convenience interface
"""

import argparse
from pathlib import Path
import sys

import cvxpy as cp

import typer
from loguru import logger
from skylark.replicate.solver import ThroughputProblem, ThroughputSolverILP
from skylark.utils.utils import do_parallel
from skylark import skylark_root

app = typer.Typer(name="skylark-solver")

# config logger
logger.remove()
logger.add(sys.stderr, format="{function:>20}:{line:<3} | <level>{message}</level>", colorize=True, enqueue=True)


def choose_solver():
    try:
        import gurobipy as _grb

        return cp.GUROBI
    except ImportError:
        try:
            import cylp as _cylp

            logger.warning("Gurobi not installed, using CoinOR instead.")
            return cp.CBC
        except ImportError:
            logger.warning("Gurobi and CoinOR not installed, using GLPK instead.")
            return cp.GLPK


@app.command()
def solve_throughput(
    src: str = typer.Argument(..., help="Source region, in format of provider:region."),
    dst: str = typer.Argument(..., help="Destination region, in format of provider:region."),
    required_throughput_gbits: float = typer.Argument(..., help="Required throughput in gbps."),
    gbyte_to_transfer: float = typer.Option(1, help="Gigabytes to transfer"),
    max_instances: int = typer.Option(1, help="Max number of instances per overlay region."),
    throughput_grid: Path = typer.Option(skylark_root / "profiles" / "throughput.csv", "--throughput-grid", help="Throughput grid file"),
    solver_verbose: bool = False,
):

    # build problem and solve
    tput = ThroughputSolverILP(throughput_grid)
    problem = ThroughputProblem(
        src,
        dst,
        required_throughput_gbits,
        gbyte_to_transfer,
        max_instances,
    )
    solution = tput.solve_min_cost(
        problem,
        solver=choose_solver(),
        solver_verbose=solver_verbose,
        save_lp_path=skylark_root / "data" / "throughput_solver.lp",
    )

    # save results
    tput.print_solution(solution)
    if solution.is_feasible:
        g = tput.plot_graphviz(solution)
        if g is not None:
            try:
                g.render(filename="/tmp/throughput_graph.gv", quiet_view=True, format="png")
            except FileNotFoundError as e:
                logger.error(f"Could not render graph: {e}")
