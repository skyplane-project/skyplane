import functools
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from skyplane import GB
from skyplane.compute.cloud_providers import CloudProvider

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
