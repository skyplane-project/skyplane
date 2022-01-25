from dataclasses import dataclass
import os
import shutil
from typing import Optional

import cvxpy as cp
import cvxpy.transforms as ct
import graphviz as gv
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from loguru import logger

from skylark import GB, skylark_root
from skylark.compute.cloud_providers import CloudProvider

GBIT_PER_GBYTE = 8


@dataclass
class ThroughputProblem:
    src: str
    dst: str
    required_throughput_gbits: float
    gbyte_to_transfer: float
    instance_limit: int
    const_throughput_grid_gbits: Optional[np.ndarray] = None  # if not set, load from profiles
    const_cost_per_gb_grid: Optional[np.ndarray] = None  # if not set, load from profiles
    aws_instance_throughput_limit: float = 5
    gcp_instance_throughput_limit: float = 8
    azure_instance_throughput_limit: float = 12.5
    benchmarked_throughput_connections = 64
    cost_per_instance_hr = 1.54  # m5.8xlarge


@dataclass
class ThroughputSolution:
    problem: ThroughputProblem
    is_feasible: bool

    # solution variables
    var_edge_flow_gigabits: Optional[np.ndarray] = None
    var_conn: Optional[np.ndarray] = None
    var_instances_per_region: Optional[np.ndarray] = None

    # solution values
    throughput_achieved_gbits: Optional[np.ndarray] = None
    cost_egress_by_edge: Optional[np.ndarray] = None
    cost_egress: Optional[float] = None
    cost_instance: Optional[float] = None
    cost_total: Optional[float] = None
    transfer_runtime_s: Optional[float] = None


class ThroughputSolver:
    def __init__(self, df_path, default_throughput=0.0):
        self.df = pd.read_csv(df_path).set_index(["src_region", "dst_region"])
        self.default_throughput = default_throughput

    def get_path_throughput(self, src, dst):
        if src == dst:
            return self.default_throughput
        elif (src, dst) not in self.df.index:
            return None
        return self.df.loc[(src, dst), "throughput_sent"]

    def get_path_cost(self, src, dst):
        return CloudProvider.get_transfer_cost(src, dst)

    def get_regions(self):
        return list(sorted(set(list(self.df.index.levels[0].unique()) + list(self.df.index.levels[1].unique()))))

    def get_throughput_grid(self):
        regions = self.get_regions()
        data_grid = np.zeros((len(regions), len(regions)))
        for i, src in enumerate(regions):
            for j, dst in enumerate(regions):
                data_grid[i, j] = self.get_path_throughput(src, dst) if self.get_path_throughput(src, dst) is not None else 0
        data_grid = data_grid / GB
        return data_grid.round(4)

    def get_cost_grid(self):
        regions = self.get_regions()
        data_grid = np.zeros((len(regions), len(regions)))
        for i, src in enumerate(regions):
            for j, dst in enumerate(regions):
                cost = self.get_path_cost(src, dst)
                assert cost is not None and cost >= 0, f"Cost for {src} -> {dst} is {cost}"
                data_grid[i, j] = cost
        return data_grid.round(2)

    def plot_throughput_grid(self, data_grid, title="Throughput (Gbps)"):
        for i in range(data_grid.shape[0]):
            for j in range(data_grid.shape[1]):
                if i <= j:
                    data_grid[i, j] = np.nan

        regions = self.get_regions()
        fig, ax = plt.subplots(1, 1, figsize=(9, 9))
        ax.imshow(data_grid)
        ax.set_title(title)
        ax.set_xticks(np.arange(len(regions)))
        ax.set_yticks(np.arange(len(regions)))
        ax.set_xticklabels(regions)
        ax.set_yticklabels(regions)

        for tick in ax.get_xticklabels():
            tick.set_rotation(90)

        # compute mean point of non nan values
        mean_point = np.nanmean(data_grid)
        for i, row in enumerate(data_grid):
            for j, col in enumerate(row):
                if i > j:
                    ax.text(j, i, round(col, 1), ha="center", va="center", color="white" if col < mean_point else "black")

        fig.patch.set_facecolor("white")
        fig.subplots_adjust(hspace=0.6)
        ax.figure.colorbar(ax.images[0], ax=ax)
        return fig, ax


class ThroughputSolverILP(ThroughputSolver):
    def solve_min_cost(
        self,
        p: ThroughputProblem,
        solver=cp.GLPK,
        solver_verbose=False,
        save_lp_path=None,
    ):
        regions = self.get_regions()
        sources = [regions.index(p.src)]
        sinks = [regions.index(p.dst)]

        # define constants
        if p.const_throughput_grid_gbits is None:
            p.const_throughput_grid_gbits = self.get_throughput_grid()
        if p.const_cost_per_gb_grid is None:
            p.const_cost_per_gb_grid = self.get_cost_grid()

        # define variables
        edge_flow_gigabits = cp.Variable(
            (len(regions), len(regions)), name="edge_flow_gigabits"
        )  # total flow including multiple connections
        conn = cp.Variable((len(regions), len(regions)), name="conn")
        instances_per_region = cp.Variable((len(regions)), name="instances_per_region")
        node_flow_in = cp.sum(edge_flow_gigabits, axis=0)
        node_flow_out = cp.sum(edge_flow_gigabits, axis=1)

        constraints = []

        # instance limit
        constraints.append(instances_per_region <= p.instance_limit)
        constraints.append(instances_per_region >= 0)

        # connection limits
        constraints.append(conn >= 0)
        constraints.append(cp.sum(conn, axis=1) <= instances_per_region * p.benchmarked_throughput_connections)

        # flow capacity constraint
        adjusted_edge_capacity_gigabits = cp.multiply(p.const_throughput_grid_gbits, conn / p.benchmarked_throughput_connections)
        constraints.append(edge_flow_gigabits <= adjusted_edge_capacity_gigabits)

        # flow conservation
        for v in range(len(regions)):
            f_in, f_out = node_flow_in[v], node_flow_out[v]
            if v in sources:
                constraints.append(f_in == 0)
                constraints.append(f_out == p.required_throughput_gbits)
            elif v in sinks:
                constraints.append(f_in == p.required_throughput_gbits)
            else:
                constraints.append(f_in == f_out)

        # non-negative flow constraint
        constraints.append(edge_flow_gigabits >= 0)

        # instance throughput constraints (egress)
        for idx, r in enumerate(regions):
            provider = r.split(":")[0]
            f_out = node_flow_out[idx]
            if provider == "aws":
                provider_instance_limit = p.aws_instance_throughput_limit
            elif provider == "gcp":
                provider_instance_limit = p.gcp_instance_throughput_limit
            elif provider == "azure":
                provider_instance_limit = p.azure_instance_throughput_limit
            else:
                raise ValueError(f"Unknown provider {provider}")
            constraints.append(f_out <= provider_instance_limit * instances_per_region[idx])

        # define objective
        transfer_size_gbit = p.gbyte_to_transfer * GBIT_PER_GBYTE
        assert p.required_throughput_gbits > 0
        runtime_s = transfer_size_gbit / p.required_throughput_gbits  # gbit * s / gbit = s
        cost_per_edge = cp.multiply(edge_flow_gigabits * runtime_s, p.const_cost_per_gb_grid / GBIT_PER_GBYTE)  #  gbit/s * $/gbit = $/s
        cost_egress = cp.sum(cost_per_edge)

        # instance cost
        per_instance_cost: float = p.cost_per_instance_hr / 3600 * runtime_s
        instance_cost = cp.sum(instances_per_region) * per_instance_cost
        total_cost = cost_egress + instance_cost
        prob = cp.Problem(cp.Minimize(total_cost), constraints)

        if solver == cp.GUROBI or solver == "gurobi":
            solver_options = {}
            solver_options["Threads"] = 1
            if save_lp_path:
                solver_options["ResultFile"] = str(save_lp_path)
            prob.solve(verbose=True, qcp=True, solver=cp.GUROBI)
        else:
            prob.solve(solver=solver, verbose=solver_verbose)

        if prob.status == "optimal":
            return ThroughputSolution(
                problem=p,
                is_feasible=True,
                var_edge_flow_gigabits=edge_flow_gigabits.value.round(6),
                var_conn=conn.value.round(6),
                var_instances_per_region=instances_per_region.value,
                throughput_achieved_gbits=[node_flow_in[i].value for i in sinks],
                cost_egress_by_edge=cost_per_edge.value,
                cost_egress=cost_egress.value,
                cost_instance=instance_cost.value,
                cost_total=total_cost.value,
                transfer_runtime_s=runtime_s,
            )
        else:
            logger.warning(f"Solver status: {prob.status}")
            return ThroughputSolution(problem=p, is_feasible=False)

    def print_solution(self, solution: ThroughputSolution):
        if solution.is_feasible:
            regions = self.get_regions()
            logger.debug(
                f"Total cost: ${solution.cost_total:.4f} (egress: ${solution.cost_egress:.4f}, instance: ${solution.cost_instance:.4f})"
            )
            logger.debug(f"Total throughput: [{', '.join(str(round(t, 2)) for t in solution.throughput_achieved_gbits)}] Gbps")
            logger.debug(f"Total runtime: {solution.transfer_runtime_s:.2f}s")
            region_inst_count = {regions[i]: solution.var_instances_per_region[i].round(3) for i in range(len(regions))}
            logger.debug("Instance regions: [{}]".format(", ".join(f"{r}={c}" for r, c in region_inst_count.items() if c > 0)))
            logger.debug("Flow matrix:")
            for i, src in enumerate(regions):
                for j, dst in enumerate(regions):
                    if solution.var_edge_flow_gigabits[i, j] > 0:
                        gb_sent = solution.transfer_runtime_s * solution.var_edge_flow_gigabits[i, j] * GBIT_PER_GBYTE
                        s = f"\t{src} -> {dst}: {solution.var_edge_flow_gigabits[i, j]:.2f} Gbps with {solution.var_conn[i, j]:.1f} connections, "
                        s += f"{gb_sent:.1f}GB (link capacity = {solution.problem.const_throughput_grid_gbits[i, j]:.2f} Gbps)"
                        logger.debug(s)
        else:
            logger.debug("No feasible solution")

    def plot_graphviz(self, solution: ThroughputSolution) -> gv.Digraph:
        # if dot is not installed
        has_dot = shutil.which("dot") is not None
        if not has_dot:
            logger.error("Graphviz is not installed. Please install it to plot the solution (sudo apt install graphviz).")
            return None

        regions = self.get_regions()
        g = gv.Digraph(name="throughput_graph")
        g.attr(rankdir="LR")

        label = f"{solution.problem.src} to {solution.problem.dst}\n"
        label += f"[{', '.join(str(round(t, 2)) for t in solution.throughput_achieved_gbits)}] Gbps, ${solution.cost_total:.4f}\n"
        label += f"Average: ${solution.cost_total / solution.transfer_runtime_s:.4f}/GB"
        g.attr(label=label)
        g.attr(labelloc="t")
        for i, src in enumerate(regions):
            for j, dst in enumerate(regions):
                if solution.var_edge_flow_gigabits[i, j] > 0:
                    link_cost = self.get_path_cost(src, dst)
                    gb_sent = solution.transfer_runtime_s * solution.var_edge_flow_gigabits[i, j] * GBIT_PER_GBYTE
                    label = f"{solution.var_edge_flow_gigabits[i, j]:.2f} Gbps (of {solution.problem.const_throughput_grid_gbits[i, j]:.2f}Gbps), "
                    label += f"${link_cost:.4f}/GB w/ {gb_sent:.1f}GB sent w/ {solution.var_conn[i, j]:.1f}c"
                    g.edge(src.replace(":", "/"), dst.replace(":", "/"), label=label)
        return g
