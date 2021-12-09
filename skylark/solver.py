import argparse

from cvxpy.expressions import constants
import pandas as pd
import numpy as np
import cvxpy as cp
import matplotlib.pyplot as plt
from loguru import logger
from skylark import skylark_root


class ThroughputSolver:
    def __init__(self, df_path, default_throughput=0.0):
        self.df = pd.read_csv(df_path).drop(columns="Unnamed: 0").set_index(["src", "dst"])
        self.default_throughput = default_throughput

    def get_path_throughput(self, src, dst):
        if src == dst:
            return self.default_throughput
        elif (src, dst) not in self.df.index:
            return None
        return self.df.loc[(src, dst), "throughput_sent"]

    def get_path_cost(self, src, dst):
        src_provider = src.split(":")[0]
        dst_provider = dst.split(":")[0]
        if src == dst:
            return 0
        elif src_provider == dst_provider:
            return 0.02
        else:
            return 0.09

    def get_regions(self):
        return list(sorted(set(list(self.df.index.levels[0].unique()) + list(self.df.index.levels[1].unique()))))

    def get_throughput_grid(self):
        regions = self.get_regions()
        data_grid = np.zeros((len(regions), len(regions)))
        for i, src in enumerate(regions):
            for j, dst in enumerate(regions):
                data_grid[i, j] = self.get_path_throughput(src, dst) if self.get_path_throughput(src, dst) is not None else 0
        data_grid = data_grid / 1e9
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

    def max_two_hop_throughput(self):
        regions = self.get_regions()
        data_grid = np.zeros((len(regions), len(regions)))
        for i, src in enumerate(regions):
            for j, dst in enumerate(regions):
                data_grid[i, j] = self.get_path_throughput(src, dst) if self.get_path_throughput(src, dst) is not None else 0
                for inter in regions:
                    tp_a = self.get_path_throughput(src, inter) if self.get_path_throughput(src, inter) is not None else 0
                    tp_b = self.get_path_throughput(inter, dst) if self.get_path_throughput(inter, dst) is not None else 0
                    tp = min(tp_a, tp_b)
                    data_grid[i, j] = max(data_grid[i, j], tp)
        data_grid = data_grid / 1e9
        return data_grid

    def max_three_hop_throughput(self):
        regions = self.get_regions()
        data_grid = np.zeros((len(regions), len(regions)))
        for i, src in enumerate(regions):
            for j, dst in enumerate(regions):
                data_grid[i, j] = self.get_path_throughput(src, dst) if self.get_path_throughput(src, dst) is not None else 0
                for inter_a in regions:
                    tp_a = self.get_path_throughput(src, inter_a) if self.get_path_throughput(src, inter_a) is not None else 0
                    for inter_b in regions:
                        tp_b = self.get_path_throughput(inter_a, inter_b) if self.get_path_throughput(inter_a, inter_b) is not None else 0
                        tp_c = self.get_path_throughput(inter_b, dst) if self.get_path_throughput(inter_b, dst) is not None else 0
                        tp = min(tp_a, tp_b, tp_c)
                        data_grid[i, j] = max(data_grid[i, j], tp)
        data_grid = data_grid / 1e9
        return data_grid


class ThroughputSolverILP(ThroughputSolver):
    def get_path_cost(self, src, dst):
        return 0.12 if src != dst else 0.0

    def get_cost_matrices(self):
        regions = self.get_regions()
        cost_matrix = np.zeros((len(regions), len(regions)))
        capacity_matrix = np.zeros((len(regions), len(regions)))
        for i, src in enumerate(regions):
            for j, dst in enumerate(regions):
                cost_matrix[i, j] = self.get_path_cost(src, dst)
                capacity_matrix[i, j] = self.get_path_throughput(src, dst) if self.get_path_throughput(src, dst) is not None else 0.0
        return cost_matrix, capacity_matrix / 1e9

    def solve(self, src, dst, required_throughput=None, cost_limit=None, solver=cp.GLPK, solver_verbose=False):
        regions = self.get_regions()
        src_idx = regions.index(src)
        dst_idx = regions.index(dst)

        edge_cost, edge_capacity = self.get_cost_matrices()
        edge_flow = cp.Variable((len(regions), len(regions)), boolean=False, name="edge_flow")
        total_cost_matrix = cp.multiply(edge_cost, cp.pos(edge_flow))
        total_cost = cp.sum(total_cost_matrix)
        total_throughput_out = cp.sum(edge_flow[src_idx, :])
        total_throughput_in = cp.sum(edge_flow[:, dst_idx])

        # constraints
        constraints = []
        constraints.append(total_throughput_out == total_throughput_in)
        for u in range(len(regions)):
            for v in range(len(regions)):
                # capacity constraints
                constraints.append(edge_flow[u, v] <= edge_capacity[u, v])
                # skew symmetry
                constraints.append(edge_flow[u, v] == -1 * edge_flow[v, u])
            # flow conservation
            if u != src_idx and u != dst_idx:
                constraints.append(cp.sum(edge_flow[u, :]) == 0)

        if required_throughput is not None and cost_limit is None:  # min cost
            logger.info("Solving for minimum cost")
            objective = cp.Minimize(total_cost)
            constraints.append(total_throughput_out >= required_throughput)
            constraints.append(total_throughput_in >= required_throughput)
        elif cost_limit is not None and required_throughput is None:  # max throughput
            logger.info("Solving for maximum throughput")
            objective = cp.Maximize(total_throughput_out)
            constraints.append(total_cost <= cost_limit)
        else:  # min cost and max throughput
            raise NotImplementedError()

        prob = cp.Problem(objective, constraints)
        prob.solve(solver=solver, verbose=solver_verbose)
        if prob.status == "optimal":
            print(cp.pos(edge_flow).value)
            print(total_cost_matrix.value)
            return dict(
                solution=cp.pos(edge_flow).value, cost_per_gb=total_cost.value, throughput=total_throughput_out.value, feasible=True
            )
        else:
            return dict(feasible=None)

    def print_solution(self, solution):
        if solution["feasible"]:
            sol = solution["solution"]
            cost = solution["cost_per_gb"]
            throughput = solution["throughput"]
            regions = self.get_regions()

            logger.debug(f"Total cost: ${cost:.4f}/GB")
            logger.debug(f"Total throughput: {throughput:.4f} Gbps")
            logger.debug("Flow matrix:")
            for i, src in enumerate(regions):
                for j, dst in enumerate(regions):
                    if sol[i, j] > 0:
                        logger.debug(f"\t{src} -> {dst}: {sol[i, j]:.2f} Gbps")
        else:
            logger.debug("No feasible solution")


if __name__ == "__main__":
    # argparse above arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("--cost_path", type=str, default=str(skylark_root / "data" / "throughput" / "df_throughput_agg_test.csv"))
    parser.add_argument("--src", type=str, required=True)
    parser.add_argument("--dst", type=str, required=True)
    parser.add_argument("--cost_limit", type=float, default=None)
    parser.add_argument("--min_throughput", type=float, default=None)
    args = parser.parse_args()

    tput = ThroughputSolverILP(args.cost_path)
    edge_cost, edge_capacity = tput.get_cost_matrices()
    print("Regions:")
    print(tput.get_regions())
    print("\nEdge cost:")
    print(edge_cost)
    print("\nEdge capacity:")
    print(edge_capacity)
    print()

    solution = tput.solve(
        args.src, args.dst, required_throughput=args.min_throughput, cost_limit=args.cost_limit, solver=cp.GUROBI, solver_verbose=False
    )
    tput.print_solution(solution)


# def test_all_pairs_solver():
#     tput = ThroughputSolver(skylark_root / 'data' / 'throughput' / 'df_throughput_agg.csv')
#     def make_symmetric(mat):
#         x, y = mat.shape
#         for i in range(x):
#             for j in range(y):
#                 mat[i, j] = max(mat[i, j], mat[j, i])
#         return mat

#     wan = make_symmetric(tput.get_throughput_grid())
#     two_hop = make_symmetric(tput.max_two_hop_throughput())
#     for i in range(len(tput.get_regions())):
#         wan[i, i] = 1.
#         two_hop[i, i] = 0.
#     speedup = two_hop / wan
#     fig, ax = tput.plot_throughput_grid(speedup, title="Throughput speedup factor")
#     fig.savefig(str(skylark_root / 'data' / 'throughput' / 'df_throughput_grid_speedup_1x.png'), bbox_inches='tight', dpi=300)

#     wan = make_symmetric(tput.get_throughput_grid())
#     three_hop = make_symmetric(tput.max_three_hop_throughput())
#     for i in range(len(tput.get_regions())):
#         wan[i, i] = 1.
#         three_hop[i, i] = 0.
#     speedup = three_hop / wan
#     fig, ax = tput.plot_throughput_grid(speedup, title="Throughput speedup factor")
#     fig.savefig(str(skylark_root / 'data' / 'throughput' / 'df_throughput_grid_speedup_2x.png'), bbox_inches='tight', dpi=300)
