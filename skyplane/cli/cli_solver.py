"""
Optimal solver using ILP formulation.
"""

import json
from pathlib import Path

import numpy as np
import typer

from skyplane import GB
from skyplane import skyplane_root
from skyplane.replicate.solver import GBIT_PER_GBYTE, ThroughputProblem, ThroughputSolution
from skyplane.utils import logger
from skyplane.utils.timer import Timer

app = typer.Typer(name="skyplane-solver")


@app.command()
def solve_throughput(
    src: str = typer.Argument(..., help="Source region, in format of provider:region."),
    dst: str = typer.Argument(..., help="Destination region, in format of provider:region."),
    required_throughput_gbits: float = typer.Argument(..., help="Required throughput in gbps."),
    gbyte_to_transfer: float = typer.Option(1, help="Gigabytes to transfer"),
    max_instances: int = typer.Option(1, help="Max number of instances per overlay region."),
    throughput_grid: Path = typer.Option(skyplane_root / "profiles" / "throughput.csv", "--throughput-grid", help="Throughput grid file"),
    solver_verbose: bool = False,
    out: Path = typer.Option(None, "--out", "-o", help="Output file for path."),
    visualize: bool = False,
):
    from skyplane.replicate.solver_ilp import ThroughputSolverILP

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
            save_lp_path=skyplane_root / "data" / "throughput_solver.lp",
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
                    g.render(filename="/tmp/throughput_graph.gv", format="png")
                    g.render(filename="/tmp/throughput_graph.gv", quiet_view=True, format="pdf")
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
                    g_rt.render(filename="/tmp/replication_topo.gv", format="png")
                    g_rt.render(filename="/tmp/replication_topo.gv", quiet_view=True, format="pdf")
                except FileNotFoundError as e:
                    logger.error(f"Could not render graph: {e}")
    else:
        typer.secho("Solution is infeasible", fg="red", err=True)
        raise typer.Exit(1)

    # print json summarizing solution
    print(json.dumps(problem.to_summary_dict()))
    print(json.dumps(solution.to_summary_dict()))


@app.command()
def solve_single_hop(
    src: str = typer.Argument(..., help="Source region, in format of provider:region."),
    dst: str = typer.Argument(..., help="Destination region, in format of provider:region."),
    throughput_grid: Path = typer.Option(skyplane_root / "profiles" / "throughput.csv", "--throughput-grid", help="Throughput grid file"),
    gbyte_to_transfer: float = typer.Option(1, help="Gigabytes to transfer"),
    out: Path = typer.Option(None, "--out", "-o", help="Output file for path."),
):
    from skyplane.replicate.solver_ilp import ThroughputSolverILP

    tput = ThroughputSolverILP(throughput_grid)
    p = ThroughputProblem(src, dst, 1, gbyte_to_transfer, 1, const_throughput_grid_gbits=tput.get_throughput_grid())
    src_region, dst_region, selected_region = None, None, None
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
                selected_region = region
                selected_region_num = i
                throughput = curr_throughput
    assert src_region is not None and dst_region is not None, "Source and destination regions not found in throughput grid."

    regions = tput.get_regions()
    edge_flow_gigabits = np.zeros((len(regions), len(regions)))
    instances_per_region = np.zeros((len(regions)))
    conn = np.zeros((len(regions), len(regions)))

    # direct transfer is optimal
    if selected_region == None:
        transfer_size_gbit = p.gbyte_to_transfer * GBIT_PER_GBYTE
        runtime_s = transfer_size_gbit / p.required_throughput_gbits
        cost_egress = gbyte_to_transfer * tput.get_path_cost(src, dst)

        per_instance_cost: float = p.cost_per_instance_hr / 3600 * (runtime_s + p.instance_provision_time_s)
        instance_cost = 2 * per_instance_cost
        cost = cost_egress + instance_cost * p.instance_cost_multiplier

        edge_flow_gigabits[src_region, dst_region] = 1
        conn[src_region, dst_region] = 1

    else:
        transfer_size_gbit = p.gbyte_to_transfer * GBIT_PER_GBYTE
        runtime_s = transfer_size_gbit / p.required_throughput_gbits
        cost_egress = gbyte_to_transfer * tput.get_path_cost(src, selected_region) + gbyte_to_transfer * tput.get_path_cost(
            selected_region, dst
        )

        per_instance_cost: float = p.cost_per_instance_hr / 3600 * (runtime_s + p.instance_provision_time_s)
        instance_cost = 3 * per_instance_cost
        cost = cost_egress + instance_cost * p.instance_cost_multiplier

        edge_flow_gigabits[src_region, selected_region_num] = 1
        edge_flow_gigabits[selected_region_num, dst_region] = 1

        conn[src_region, selected_region_num] = 1
        conn[selected_region_num, dst_region] = 1

        instances_per_region[src_region] = 1
        instances_per_region[selected_region_num] = 1
        instances_per_region[dst_region] = 1

    prob = ThroughputProblem(src, dst, -1, gbyte_to_transfer, 1)

    sol = ThroughputSolution(prob, True)
    sol.var_edge_flow_gigabits = edge_flow_gigabits
    sol.var_conn = conn
    sol.var_instances_per_region = instances_per_region
    sol.throughput_achieved_gbits = [throughput / GB]
    sol.cost_total = cost
    sol.cost_egress = cost_egress
    sol.cost_instance = instance_cost
    sol.transfer_runtime_s = runtime_s
    sol.problem = p

    replication_topo, _ = tput.to_replication_topology(sol, scale_to_capacity=False)

    tput.print_solution(sol)

    if out:
        with open(out, "w") as f:
            f.write(replication_topo.to_json())
