"""
AWS convenience interface
"""

from pathlib import Path

import cvxpy as cp

import typer
from skylark.utils import logger
from skylark.replicate.solver import ThroughputProblem, ThroughputSolverILP
from skylark import skylark_root

app = typer.Typer(name="skylark-solver")


def choose_solver():
    try:
        import gurobipy as _grb  # pytype: disable=import-error

        return cp.GUROBI
    except ImportError:
        try:
            import cylp as _cylp  # pytype: disable=import-error

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
    max_instances: int = typer.Option(1, "--max-instances", "-n", help="Max number of instances per overlay region."),
    instance_cost_multiplier: float = typer.Option(1, help="Instance cost multiplier."),
    throughput_grid: Path = typer.Option(skylark_root / "profiles" / "throughput.csv", "--throughput-grid", help="Throughput grid file"),
    solver_verbose: bool = False,
    out: Path = typer.Option(None, "--out", "-o", help="Output file for path."),
    visualize: bool = False,
):

    # build problem and solve
    tput = ThroughputSolverILP(throughput_grid)
    problem = ThroughputProblem(
        src, dst, required_throughput_gbits, gbyte_to_transfer, max_instances, const_instance_cost_multipler=instance_cost_multiplier
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
        if visualize:
            g = tput.plot_graphviz(solution)
            if g is not None:
                try:
                    for f in Path("/tmp/").glob("throughput_graph.gv*"):
                        f.unlink()
                    g.render(filename="/tmp/throughput_graph.gv", quiet_view=True, format="pdf")
                    g.render(filename="/tmp/throughput_graph.gv", format="png")
                except FileNotFoundError as e:
                    logger.error(f"Could not render graph: {e}")
        replication_topo = tput.to_replication_topology(solution)
        if out:
            with open(out, "w") as f:
                f.write(replication_topo.to_json())
        if visualize:
            g_rt = replication_topo.to_graphviz()
            if g_rt is not None:
                try:
                    for f in Path("/tmp/").glob("replication_topo.gv*"):
                        f.unlink()
                    g_rt.render(filename="/tmp/replication_topo.gv", quiet_view=True, format="pdf")
                    g_rt.render(filename="/tmp/replication_topo.gv", format="png")
                except FileNotFoundError as e:
                    logger.error(f"Could not render graph: {e}")
