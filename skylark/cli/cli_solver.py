"""
AWS convenience interface
"""

from pathlib import Path

import typer
import numpy as np
from skylark.utils import logger
from skylark.replicate.solver import ThroughputProblem, ThroughputSolverILP, ThroughputSolution
from skylark import skylark_root
from skylark.utils.utils import Timer
from skylark import GB

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
    #print(vars(solution))
    print(solution.var_edge_flow_gigabits)
    print(solution.var_conn)
    print(solution.var_instances_per_region)
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

@app.command()
def solve_single_hop(
    src: str = typer.Argument(..., help="Source region, in format of provider:region."),
    dst: str = typer.Argument(..., help="Destination region, in format of provider:region."),
    throughput_grid: Path = typer.Option(skylark_root / "profiles" / "throughput.csv", "--throughput-grid", help="Throughput grid file"),
    out: Path = typer.Option(None, "--out", "-o", help="Output file for path."),
):
    tput = ThroughputSolverILP(throughput_grid)
    selected_region = None
    selected_region_num = -1
    throughput = tput.get_path_throughput(src, dst)
    for i, region in enumerate(tput.get_regions()):
        if region == src:
            src_region = i
        elif region == dst:
            dst_region = i
        else:
            curr_throughput = min(tput.get_path_throughput(src, region), tput.get_path_throughput(region, dst))
            if curr_throughput > throughput:
                print(region, curr_throughput)
                selected_region = region
                selected_region_num = i
                throughput = curr_throughput

    regions = tput.get_regions()
    edge_flow_gigabits = np.zeros((len(regions), len(regions)))
    instances_per_region = np.zeros((len(regions)))
    conn = np.zeros((len(regions), len(regions)))

    # direct transfer is optimal
    if selected_region == None:
        edge_flow_gigabits[src_region, dst_region] = 1
        conn[src_region, dst_region] = 1
   
    else:
        edge_flow_gigabits[src_region, selected_region_num] = 1
        edge_flow_gigabits[selected_region_num, dst_region] = 1

        conn[src_region, selected_region_num] = 1
        conn[selected_region_num, dst_region] = 1

        instances_per_region[selected_region_num] = 1
    
    sol = ThroughputSolution(None, True)
    sol.var_edge_flow_gigabits = edge_flow_gigabits
    sol.var_conn = conn
    sol.var_instances_per_region = instances_per_region

    replication_topo = tput.to_replication_topology(sol, scale_to_capacity=False)

    #print(top)
    if out:
        with open(out, "w") as f: 
            f.write(replication_topo.to_json())

