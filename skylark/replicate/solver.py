import argparse
import shutil

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
    def solve(
        self,
        src,
        dst,
        required_throughput_gbits=None,
        cost_limit=None,
        gbyte_to_transfer=1.0,
        instance_limit=1,
        aws_instance_throughput_limit=5.0,
        gcp_instance_throughput_limit=8.0,
        azure_instance_throughput_limit=16.0,
        solver=cp.GLPK,
        solver_verbose=False,
    ):
        regions = self.get_regions()
        src_idx = regions.index(src)
        dst_idx = regions.index(dst)

        # define constants
        edge_cost_per_gigabyte, edge_capacity_gigabits = self.get_cost_grid(), self.get_throughput_grid()
        edge_cost_per_gigabit = edge_cost_per_gigabyte / GBIT_PER_GBYTE

        # define variables
        edge_flow_gigabits = cp.Variable((len(regions), len(regions)), boolean=False, name="edge_flow_gigabits")
        total_throughput_out = cp.sum(edge_flow_gigabits[src_idx, :])
        total_throughput_in = cp.sum(edge_flow_gigabits[:, dst_idx])

        # constraints
        constraints = [total_throughput_out == total_throughput_in]
        for u in range(len(regions)):
            for v in range(len(regions)):
                # capacity constraints
                constraints.append(edge_flow_gigabits[u, v] <= edge_capacity_gigabits[u, v])
                # skew symmetry
                constraints.append(edge_flow_gigabits[u, v] == -1 * edge_flow_gigabits[v, u])
            # flow conservation
            if u != src_idx and u != dst_idx:
                constraints.append(cp.sum(edge_flow_gigabits[u, :]) == 0)

        # instance throughput constraints
        for idx, r in enumerate(regions):
            provider = r.split(":")[0]
            if provider == "aws":
                constraints.append(cp.sum(edge_flow_gigabits[idx, :]) <= aws_instance_throughput_limit)
            elif provider == "gcp":
                constraints.append(cp.sum(edge_flow_gigabits[idx, :]) <= gcp_instance_throughput_limit)
            elif provider == "azure":
                constraints.append(cp.sum(edge_flow_gigabits[idx, :]) <= azure_instance_throughput_limit)
            else:
                raise ValueError(f"Unknown provider {provider}")

        if required_throughput_gbits is not None and cost_limit is None:  # min cost
            assert required_throughput_gbits > 0
            cost_per_second = cp.sum(cp.multiply(edge_cost_per_gigabit, cp.pos(edge_flow_gigabits)))  # $/gbit * gbit/s = $/s
            transfer_runtime = (GBIT_PER_GBYTE * gbyte_to_transfer) / required_throughput_gbits  # gbit / (gbit/s) = s
            total_cost = cost_per_second * transfer_runtime

            objective = cp.Minimize(total_cost)
            constraints.append(total_throughput_out * instance_limit == required_throughput_gbits)
            constraints.append(total_throughput_in * instance_limit == required_throughput_gbits)
        elif cost_limit is not None and required_throughput_gbits is None:  # max throughput
            raise NotImplementedError("Max throughput not implemented")
            # assert cost_limit > 0
            # cost_per_second = cp.sum(cp.multiply(edge_cost_per_gigabit, cp.pos(edge_flow_gigabits)))  # $/gbit * gbit/s = $/s
            # transfer_runtime = (GBIT_PER_GBYTE * gbyte_to_transfer) / cp.inv_pos(total_throughput_out)  # gbit / (gbit/s) = s
            # total_cost = cost_per_second * transfer_runtime

            # objective = cp.Maximize(total_throughput_out)
            # constraints.append(total_cost <= cost_limit)
        else:  # min cost and max throughput
            raise NotImplementedError()

        prob = cp.Problem(objective, constraints)
        prob.solve(solver=solver, verbose=solver_verbose)
        if prob.status == "optimal":
            solution = cp.pos(edge_flow_gigabits).value
            return dict(
                src=src,
                dst=dst,
                gbyte_to_transfer=gbyte_to_transfer,
                solution=solution,
                cost=total_cost.value,
                throughput=total_throughput_out.value,
                feasible=True,
            )
        else:
            return dict(feasible=None)

    def print_solution(self, solution):
        if solution["feasible"]:
            sol = solution["solution"]
            cost = solution["cost"]
            throughput = solution["throughput"]
            regions = self.get_regions()

            logger.debug(f"Total cost: ${cost:.4f}")
            logger.debug(f"Total throughput: {throughput:.2f} Gbps")
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
        g.attr(label=f"{solution['src']} to {solution['dst']}\n{solution['throughput']:.2f} Gbps, ${solution['cost']:.4f}")
        g.attr(labelloc="t")
        for i, src in enumerate(regions):
            for j, dst in enumerate(regions):
                if solution["solution"][i, j] > 0:
                    link_cost = self.get_path_cost(src, dst)
                    g.edge(
                        src.replace(":", "/"),
                        dst.replace(":", "/"),
                        label=f"{solution['solution'][i, j]:.2f} Gbps, ${link_cost:.2f}/GB",
                    )
        return g


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--cost-path", type=str, default=str(skylark_root / "data" / "throughput" / "df_throughput_agg.csv"))
    parser.add_argument("--src", type=str, required=True)
    parser.add_argument("--dst", type=str, required=True)
    parser.add_argument("--max-cost", type=float, default=None)
    parser.add_argument("--min-throughput", type=float, default=None)
    args = parser.parse_args()

    tput = ThroughputSolverILP(args.cost_path)
    solution = tput.solve(
        args.src,
        args.dst,
        required_throughput_gbits=args.min_throughput,
        cost_limit=args.max_cost,
        solver=cp.GUROBI,
        solver_verbose=False,
    )
    tput.print_solution(solution)
    if solution["feasible"]:
        tput.plot_graphviz(solution).render(filename="/tmp/throughput_graph.gv", view=True)
