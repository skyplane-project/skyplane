import numpy as np
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

GBIT_PER_GBYTE = 8


@dataclass
class BroadcastProblem:
    src: str
    dsts: List[str]

    gbyte_to_transfer: float
    instance_limit: int  # max # of vms per region
    num_partitions: int

    required_time_budget: float = 10  # ILP specific, default to 10s

    const_throughput_grid_gbits: Optional[np.ndarray] = None  # if not set, load from profiles
    const_cost_per_gb_grid: Optional[np.ndarray] = None  # if not set, load from profiles

    # provider bandwidth limits (ingress, egress)
    aws_instance_throughput_limit: Tuple[float, float] = (10, 5)
    gcp_instance_throughput_limit: Tuple[float, float] = (16, 7)  # limited to 12.5 gbps due to CPU limit
    azure_instance_throughput_limit: Tuple[float, float] = (16, 16)  # limited to 12.5 gbps due to CPU limit

    # benchmarked_throughput_connections is the number of connections that the iperf3 throughput grid was run at,
    # we assume throughput is linear up to this connection limit
    benchmarked_throughput_connections = 64
    cost_per_instance_hr = 0.54  # based on m5.8xlarge spot
    instance_cost_multiplier = 1.0
    # instance_provision_time_s = 0.0

    def to_summary_dict(self):
        """Simple summary of the problem"""
        return {
            "src": self.src,
            "dsts": self.dsts,
            "gbyte_to_transfer": self.gbyte_to_transfer,
            "instance_limit": self.instance_limit,
            "num_partitions": self.num_partitions,
            "required_time_budget": self.required_time_budget,
            "aws_instance_throughput_limit": self.aws_instance_throughput_limit,
            "gcp_instance_throughput_limit": self.gcp_instance_throughput_limit,
            "azure_instance_throughput_limit": self.azure_instance_throughput_limit,
            "benchmarked_throughput_connections": self.benchmarked_throughput_connections,
            "cost_per_instance_hr": self.cost_per_instance_hr,
            "instance_cost_multiplier": self.instance_cost_multiplier
            # "instance_provision_time_s": self.instance_provision_time_s,
        }


@dataclass
class BroadcastSolution:
    problem: BroadcastProblem
    is_feasible: bool
    extra_data: Optional[Dict] = None

    var_edges: Optional[List] = None  # need to fix this, just for testing
    var_nodes: Optional[List] = None  # need to fix this, just for testing

    # solution variables
    var_edge_partitions: Optional[np.ndarray] = None  # each edge carries each partition or not
    var_node_transfer_partitions: Optional[np.ndarray] = None  # whether node transfers partition
    var_instances_per_region: Optional[np.ndarray] = None  # number of VMs per region
    var_flow: Optional[np.ndarray] = None  # enforce flow conservation, just used for checking

    # solution values
    cost_egress: Optional[float] = None
    cost_instance: Optional[float] = None
    cost_total: Optional[float] = None
    transfer_runtime_s: Optional[float] = None  # NOTE: might not be able to calculate here
    throughput_achieved_gbits: Optional[List[float]] = None  # NOTE: might not be able to calculate here

    def to_summary_dict(self):
        """Print simple summary of solution."""
        return {
            "is_feasible": self.is_feasible,
            "solution": {
                "cost_egress": self.cost_egress,
                "cost_instance": self.cost_instance,
                "cost_total": self.cost_total,
                "time_budget": self.problem.required_time_budget,
            },
        }
