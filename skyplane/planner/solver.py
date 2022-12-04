import functools
import shutil
from collections import namedtuple
from dataclasses import dataclass

import numpy as np
import pandas as pd
from typing import Dict, List, Optional, Tuple

from skyplane import compute
from skyplane.planner.topology import ReplicationTopology
from skyplane.utils import logger
from skyplane.utils.definitions import GB

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
    # provider bandwidth limits (egress, ingress)
    aws_instance_throughput_limit: Tuple[float, float] = (5, 10)
    gcp_instance_throughput_limit: Tuple[float, float] = (7, 16)  # limited to 12.5 gbps due to CPU limit
    azure_instance_throughput_limit: Tuple[float, float] = (16, 16)  # limited to 12.5 gbps due to CPU limit
    # benchmarked_throughput_connections is the number of connections that the iperf3 throughput grid was run at,
    # we assume throughput is linear up to this connection limit
    benchmarked_throughput_connections = 64
    cost_per_instance_hr = 0.54  # based on m5.8xlarge spot
    instance_cost_multiplier = 1.0
    instance_provision_time_s = 0.0

    def to_summary_dict(self):
        """Simple summary of the problem"""
        return {
            "src": self.src,
            "dst": self.dst,
            "required_throughput_gbits": self.required_throughput_gbits,
            "gbyte_to_transfer": self.gbyte_to_transfer,
            "instance_limit": self.instance_limit,
            "aws_instance_throughput_limit": self.aws_instance_throughput_limit,
            "gcp_instance_throughput_limit": self.gcp_instance_throughput_limit,
            "azure_instance_throughput_limit": self.azure_instance_throughput_limit,
            "benchmarked_throughput_connections": self.benchmarked_throughput_connections,
            "cost_per_instance_hr": self.cost_per_instance_hr,
            "instance_cost_multiplier": self.instance_cost_multiplier,
            "instance_provision_time_s": self.instance_provision_time_s,
        }


@dataclass
class ThroughputSolution:
    problem: ThroughputProblem
    is_feasible: bool
    extra_data: Optional[Dict] = None

    # solution variables
    var_edge_flow_gigabits: Optional[np.ndarray] = None
    var_conn: Optional[np.ndarray] = None
    var_instances_per_region: Optional[np.ndarray] = None

    # solution values
    throughput_achieved_gbits: Optional[List[float]] = None
    cost_egress_by_edge: Optional[np.ndarray] = None
    cost_egress: Optional[float] = None
    cost_instance: Optional[float] = None
    cost_total: Optional[float] = None
    transfer_runtime_s: Optional[float] = None

    # baseline
    baseline_throughput_achieved_gbits: Optional[float] = None
    baseline_cost_egress: Optional[float] = None
    baseline_cost_instance: Optional[float] = None
    baseline_cost_total: Optional[float] = None

    def to_summary_dict(self):
        """Print simple summary of solution."""
        if self.is_feasible:
            return {
                "is_feasible": self.is_feasible,
                "solution": {
                    "throughput_achieved_gbits": self.throughput_achieved_gbits,
                    "cost_egress": self.cost_egress,
                    "cost_instance": self.cost_instance,
                    "cost_total": self.cost_total,
                    "transfer_runtime_s": self.transfer_runtime_s,
                },
                "baseline": {
                    "throughput_achieved_gbits": self.baseline_throughput_achieved_gbits,
                    "cost_egress": self.baseline_cost_egress,
                    "cost_instance": self.baseline_cost_instance,
                    "cost_total": self.baseline_cost_total,
                },
            }
        else:
            return {"is_feasible": self.is_feasible}


class ThroughputSolver:
    def __init__(self, df_path, default_throughput=0.0):
        self.df = pd.read_csv(df_path).set_index(["src_region", "dst_region", "src_tier", "dst_tier"]).sort_index()
        self.default_throughput = default_throughput

    @functools.lru_cache(maxsize=None)
    def get_path_throughput(self, src, dst, src_tier="PREMIUM", dst_tier="PREMIUM"):
        if src == dst:
            return self.default_throughput
        elif (src, dst, src_tier, dst_tier) not in self.df.index:
            return None
        return self.df.loc[(src, dst, src_tier, dst_tier), "throughput_sent"].values[0]

    @functools.lru_cache(maxsize=None)
    def get_path_cost(self, src, dst, src_tier="PREMIUM", dst_tier="PREMIUM"):
        assert src_tier == "PREMIUM" and dst_tier == "PREMIUM"
        return compute.CloudProvider.get_transfer_cost(src, dst)

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

    def get_baseline_throughput_and_cost(self, p: ThroughputProblem) -> Tuple[float, float, float]:
        src, dst = p.src, p.dst
        throughput = max(p.instance_limit * self.get_path_throughput(src, dst) / GB, 1e-6)
        transfer_s = p.gbyte_to_transfer * GBIT_PER_GBYTE / throughput
        instance_cost = p.cost_per_instance_hr * p.instance_limit * transfer_s / 3600
        egress_cost = p.gbyte_to_transfer * self.get_path_cost(src, dst)
        return throughput, egress_cost, instance_cost

    def plot_throughput_grid(self, data_grid, title="Throughput (Gbps)"):
        import matplotlib.pyplot as plt

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

    def print_solution(self, solution: ThroughputSolution):
        if solution.is_feasible:
            regions = self.get_regions()
            logger.debug(
                f"Total cost: ${solution.cost_total:.4f} (egress: ${solution.cost_egress:.4f}, instance: ${solution.cost_instance:.4f})"
            )
            logger.debug(f"Total throughput: [{', '.join(str(round(t, 2)) for t in solution.throughput_achieved_gbits)}] Gbps")
            logger.debug(f"Total runtime: {solution.transfer_runtime_s:.2f}s")
            region_inst_count = {regions[i]: int(solution.var_instances_per_region[i]) for i in range(len(regions))}
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

    def plot_graphviz(self, solution: ThroughputSolution):
        import graphviz as gv

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
                    label = f"{solution.var_edge_flow_gigabits[i, j]:.2f} Gbps (of {solution.problem.const_throughput_grid_gbits[i, j]:.2f}Gbps), "
                    label += f"\n${link_cost:.4f}/GB over {solution.var_conn[i, j]:.1f}c"
                    src_label = f"{src.replace(':', '/')}x{solution.var_instances_per_region[i]:.1f}"
                    dst_label = f"{dst.replace(':', '/')}x{solution.var_instances_per_region[j]:.1f}"
                    g.edge(src_label, dst_label, label=label)
        return g

    def to_replication_topology(self, solution: ThroughputSolution, scale_to_capacity=True) -> Tuple[ReplicationTopology, float]:
        regions = self.get_regions()
        Edge = namedtuple("Edge", ["src_region", "src_instance_idx", "dst_region", "dst_instance_idx", "connections"])

        # compute connections to target per instance
        ninst = solution.var_instances_per_region
        average_egress_conns = [0 if ninst[i] == 0 else np.ceil(solution.var_conn[i, :].sum() / ninst[i]) for i in range(len(regions))]
        average_ingress_conns = [0 if ninst[i] == 0 else np.ceil(solution.var_conn[:, i].sum() / ninst[i]) for i in range(len(regions))]

        # first assign source instances to destination regions
        src_edges: List[Edge] = []
        n_instances = {}
        for i, src in enumerate(regions):
            src_instance_idx, src_instance_connections = 0, 0
            for j, dst in enumerate(regions):
                if solution.var_edge_flow_gigabits[i, j] > 0:
                    connections_to_allocate = np.rint(solution.var_conn[i, j]).astype(int)
                    while connections_to_allocate > 0:
                        # if this edge would exceed the instance connection limit, partially add connections to
                        # current instance and increment instance
                        if connections_to_allocate + src_instance_connections > average_egress_conns[i]:
                            partial_conn = average_egress_conns[i] - src_instance_connections
                            connections_to_allocate -= partial_conn
                            assert connections_to_allocate >= 0, f"connections_to_allocate = {connections_to_allocate}"
                            assert partial_conn >= 0, f"partial_conn = {partial_conn}"
                            if partial_conn > 0:
                                src_edges.append(Edge(src, src_instance_idx, dst, None, partial_conn))
                                logger.fs.warning(
                                    f"{src}:{src_instance_idx}:{src_instance_connections}c -> {dst} (partial): {partial_conn}c of {connections_to_allocate}c remaining"
                                )
                            src_instance_idx += 1
                            src_instance_connections = 0
                        else:
                            partial_conn = connections_to_allocate
                            connections_to_allocate = 0
                            src_edges.append(Edge(src, src_instance_idx, dst, None, partial_conn))
                            logger.fs.warning(
                                f"{src}:{src_instance_idx}:{src_instance_connections}c -> {dst}: {partial_conn}c of {connections_to_allocate}c remaining"
                            )
                            src_instance_connections += partial_conn
            n_instances[i] = src_instance_idx + 1

        # assign destination instances (currently None) to Edges
        dst_edges = []
        dsts_instance_idx = {i: 0 for i in regions}
        dsts_instance_conn = {i: 0 for i in regions}
        for e in src_edges:
            connections_to_allocate = np.rint(e.connections).astype(int)
            while connections_to_allocate > 0:
                if connections_to_allocate + dsts_instance_conn[e.dst_region] > average_ingress_conns[regions.index(e.dst_region)]:
                    partial_conn = average_ingress_conns[regions.index(e.dst_region)] - dsts_instance_conn[e.dst_region]
                    connections_to_allocate = connections_to_allocate - partial_conn
                    if partial_conn > 0:
                        dst_edges.append(
                            Edge(e.src_region, e.src_instance_idx, e.dst_region, dsts_instance_idx[e.dst_region], partial_conn)
                        )
                        logger.fs.warning(
                            f"{e.src_region}:{e.src_instance_idx}:{dsts_instance_conn[e.dst_region]}c -> {e.dst_region}:{dsts_instance_idx[e.dst_region]}:{dsts_instance_conn[e.dst_region]}c (partial): {partial_conn}c of {connections_to_allocate}c remaining"
                        )
                    dsts_instance_idx[e.dst_region] += 1
                    dsts_instance_conn[e.dst_region] = 0
                else:
                    dst_edges.append(
                        Edge(e.src_region, e.src_instance_idx, e.dst_region, dsts_instance_idx[e.dst_region], connections_to_allocate)
                    )
                    logger.fs.warning(
                        f"{e.src_region}:{e.src_instance_idx}:{dsts_instance_conn[e.dst_region]}c -> {e.dst_region}:{dsts_instance_idx[e.dst_region]}:{dsts_instance_conn[e.dst_region]}c: {connections_to_allocate}c remaining"
                    )
                    dsts_instance_conn[e.dst_region] += connections_to_allocate
                    connections_to_allocate = 0

        # scale connections up to saturate links
        if scale_to_capacity:
            conns_egress: Dict[Tuple[str, int], int] = {}
            conns_ingress: Dict[Tuple[str, int], int] = {}
            for e in dst_edges:
                conns_egress[(e.src_region, e.src_instance_idx)] = e.connections + conns_egress.get((e.src_region, e.src_instance_idx), 0)
                conns_ingress[(e.dst_region, e.dst_instance_idx)] = e.connections + conns_ingress.get((e.dst_region, e.dst_instance_idx), 0)
            bottleneck_capacity = max(list(conns_egress.values()) + list(conns_ingress.values()))
            scale_factor = 64 / bottleneck_capacity
            logger.fs.warning(f"Scaling connections by {scale_factor:.2f}x")
            dst_edges = [e._replace(connections=int(e.connections * scale_factor)) for e in dst_edges]
        else:
            scale_factor = 1.0

        # build ReplicationTopology
        obj_store_edges = set()
        replication_topology = ReplicationTopology()
        for e in dst_edges:
            if e.connections >= 1:
                # connect source to destination
                replication_topology.add_instance_instance_edge(
                    src_region=e.src_region,
                    src_instance=e.src_instance_idx,
                    dest_region=e.dst_region,
                    dest_instance=e.dst_instance_idx,
                    num_connections=e.connections,
                )

                # connect source instances to source gateway
                if e.src_region == solution.problem.src and ("src", e.src_region, e.src_instance_idx) not in obj_store_edges:
                    replication_topology.add_objstore_instance_edge(
                        src_region=e.src_region, dest_region=e.src_region, dest_instance=e.src_instance_idx
                    )
                    obj_store_edges.add(("src", e.src_region, e.src_instance_idx))

                # connect destination instances to destination gateway
                if e.dst_region == solution.problem.dst and ("dst", e.dst_region, e.dst_instance_idx) not in obj_store_edges:
                    replication_topology.add_instance_objstore_edge(
                        src_region=e.dst_region, src_instance=e.dst_instance_idx, dest_region=e.dst_region
                    )
                    obj_store_edges.add(("dst", e.dst_region, e.dst_instance_idx))

        return replication_topology, scale_factor
