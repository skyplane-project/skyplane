import argparse
import shutil
from tabnanny import verbose

import cvxpy as cp
import graphviz as gv
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from loguru import logger

from skylark import GB, skylark_root
from skylark.compute.cloud_providers import CloudProvider

GBIT_PER_GBYTE = 8


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
        return data_grid

    def get_cost_grid(self):
        regions = self.get_regions()
        data_grid = np.zeros((len(regions), len(regions)))
        for i, src in enumerate(regions):
            for j, dst in enumerate(regions):
                cost = self.get_path_cost(src, dst)
                assert cost is not None and cost >= 0, f"Cost for {src} -> {dst} is {cost}"
                data_grid[i, j] = cost
        return data_grid

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
        src,
        dst,
        required_throughput_gbits,
        gbyte_to_transfer,
        instance_limit=1,
        aws_instance_throughput_limit=5.0,
        gcp_instance_throughput_limit=8.0,
        azure_instance_throughput_limit=16.0,
        solver=cp.GLPK,
        solver_verbose=False,
    ):
        logger.info(f"Solving {src} -> {dst} with tput = {required_throughput_gbits} Gbps")
        regions = self.get_regions()
        src_idx = regions.index(src)
        dst_idx = regions.index(dst)
        sources = [src_idx]
        sinks = [dst_idx]

        # define constants
        edge_cost_per_gigabyte, edge_capacity_gigabits = self.get_cost_grid(), self.get_throughput_grid()

        # define variables
        edge_flow_gigabits = cp.Variable((len(regions), len(regions)), boolean=False, name="edge_flow_gigabits")
        inv_n_instances = cp.Variable(name="n_instances")  # integer=True
        n_instances = cp.inv_pos(inv_n_instances)
        node_flow_in = cp.sum(edge_flow_gigabits, axis=0)
        node_flow_out = cp.sum(edge_flow_gigabits, axis=1)
        req_tput_instance = required_throughput_gbits * inv_n_instances

        constraints = []

        # instance limit
        constraints.append(inv_n_instances >= 1.0 / instance_limit)
        constraints.append(inv_n_instances <= 1)
        # constraints.append(n_instances >= 1)

        # flow capacity constraint
        constraints.append(edge_flow_gigabits <= edge_capacity_gigabits)

        # flow conservation
        for v in range(len(regions)):
            f_in, f_out = node_flow_in[v], node_flow_out[v]
            if v in sources:
                constraints.append(f_in == 0)
                constraints.append(f_out == req_tput_instance)
            elif v in sinks:
                constraints.append(f_in == req_tput_instance)
            else:
                constraints.append(f_in == f_out)

        # non-negative flow constraint
        constraints.append(edge_flow_gigabits >= 0)

        # instance throughput constraints (egress)
        for idx, r in enumerate(regions):
            provider = r.split(":")[0]
            f_out = node_flow_out[idx]
            if provider == "aws":
                constraints.append(f_out <= aws_instance_throughput_limit)
            elif provider == "gcp":
                constraints.append(f_out <= gcp_instance_throughput_limit)
            elif provider == "azure":
                constraints.append(f_out <= azure_instance_throughput_limit)
            else:
                raise ValueError(f"Unknown provider {provider}")

        assert required_throughput_gbits > 0

        # define objective
        transfer_size_gbit = gbyte_to_transfer * GBIT_PER_GBYTE
        runtime_s = transfer_size_gbit / required_throughput_gbits  # gbit * s / gbit = s
        edge_cost_per_gigabit = edge_cost_per_gigabyte / GBIT_PER_GBYTE  # $ / (GB * 8) = $/gbit
        cost_per_second = cp.sum(cp.multiply(edge_flow_gigabits, edge_cost_per_gigabit))  #  gbit/s * $/gbit = $/s
        total_cost = cost_per_second * runtime_s  # + 0.01 * n_instances

        prob = cp.Problem(cp.Minimize(total_cost), constraints)
        # prob.solve(solver=solver, verbose=solver_verbose)
        prob.solve(verbose=True, qcp=True, solver=cp.GUROBI)
        if prob.status == "optimal":
            solution = cp.pos(edge_flow_gigabits).value
            return dict(
                src=src,
                dst=dst,
                gbyte_to_transfer=gbyte_to_transfer,
                n_instances=n_instances.value,
                solution=solution,
                cost=total_cost.value,
                throughput=node_flow_in[sinks[0]].value * n_instances.value,
                feasible=True,
            )
        else:
            return dict(feasible=None)

    def print_solution(self, solution):
        if solution["feasible"]:
            sol = solution["solution"]
            cost = solution["cost"]
            throughput = solution["throughput"]
            n_instances = solution["n_instances"]
            regions = self.get_regions()

            logger.debug(f"Total cost: ${cost:.4f}")
            logger.debug(f"Total throughput: {throughput:.2f} Gbps")
            logger.debug(f"Number of instances: {n_instances:.2f}")
            logger.debug("Flow matrix:")
            for i, src in enumerate(regions):
                for j, dst in enumerate(regions):
                    if sol[i, j] > 0:
                        logger.debug(f"\t{src} -> {dst}: {sol[i, j]:.2f} Gbps")
        else:
            logger.debug("No feasible solution")

    def plot_graphviz(self, solution) -> gv.Digraph:
        # if dot is not installed
        has_dot = shutil.which("dot") is not None
        if not has_dot:
            logger.error("Graphviz is not installed. Please install it to plot the solution (sudo apt install graphviz).")
            return None
        regions = self.get_regions()
        g = gv.Digraph(name="throughput_graph")
        g.attr(rankdir="LR")
        g.attr(
            label=f"{solution['src']} to {solution['dst']}\n{solution['throughput']:.2f} Gbps, ${solution['cost']:.4f}, {solution['n_instances']:.2f} instances"
        )
        g.attr(labelloc="t")
        for i, src in enumerate(regions):
            for j, dst in enumerate(regions):
                if solution["solution"][i, j] > 0:
                    link_cost = self.get_path_cost(src, dst)
                    g.edge(
                        src.replace(":", "/"),
                        dst.replace(":", "/"),
                        label=f"{solution['solution'][i, j]:.2f} Gbps, ${link_cost:.4f}/GB",
                    )
        return g
