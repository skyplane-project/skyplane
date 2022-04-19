"""
Optimal solver using ILP formulation.
"""

import json
from pathlib import Path

import typer
from skylark.utils import logger
from skylark.replicate.solver import ThroughputProblem, ThroughputSolverILP
from skylark import skylark_root
from skylark.utils.utils import Timer

app = typer.Typer(name="skylark-solver")


@app.command()
def solve_throughput(
    src: str = typer.Argument(..., help="Source region, in format of provider:region."),
    dst: str = typer.Argument(..., help="Destination region, in format of provider:region."),
    required_throughput_gbits: float = typer.Argument(..., help="Required throughput in gbps."),
    gbyte_to_transfer: float = typer.Option(1, help="Gigabytes to transfer"),
    max_instances: int = typer.Option(1, help="Max number of instances per overlay region."),
    throughput_grid: Path = typer.Option(skylark_root / "profiles" / "throughput.csv", "--throughput-grid", help="Throughput grid file"),
    solver_verbose: bool = False,
    out: Path = typer.Option(None, "--out", "-o", help="Output file for path."),
    visualize: bool = False,
):

    # build problem and solve
    tput = ThroughputSolverILP(throughput_grid)
    problem = ThroughputProblem(
        src=src,
        dst=dst,
        required_throughput_gbits=required_throughput_gbits,
        gbyte_to_transfer=gbyte_to_transfer,
        instance_limit=max_instances,
    )
    with Timer("Solve throughput problem"):
        solution = tput.solve_min_cost(
            problem,
            solver=ThroughputSolverILP.choose_solver(),
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
        replication_topo, connection_scale_factor = tput.to_replication_topology(solution)
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
    else:
        raise typer.Exit(f"Solution is infeasible.")

    # print json summarizing solution
    print(json.dumps(problem.to_summary_dict()))
    print(json.dumps(solution.to_summary_dict()))
