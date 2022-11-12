import networkx as nx
import pandas as pd
import numpy as np
from skyplane.broadcast.bc_plan import BroadcastReplicationTopology
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

    # provider bandwidth limits (egress, ingress)
    aws_instance_throughput_limit: Tuple[float, float] = (5, 10)
    gcp_instance_throughput_limit: Tuple[float, float] = (7, 16)  # limited to 12.5 gbps due to CPU limit
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


class BroadcastSolver:
    def __init__(self, cost_grid_path, tp_grid_path):
        self.costs = pd.read_csv(cost_grid_path)
        self.throughput = pd.read_csv(tp_grid_path)
        self.G = self.make_nx_graph(self.costs, self.throughput)

    def make_nx_graph(self, cost, throughput):
        G = nx.DiGraph()
        for _, row in throughput.iterrows():
            if row["src_region"] == row["dst_region"]:
                continue
            G.add_edge(row["src_region"], row["dst_region"], cost=None, throughput=row["throughput_sent"] / 1e9)

        for _, row in cost.iterrows():
            if row["src"] in G and row["dest"] in G[row["src"]]:
                G[row["src"]][row["dest"]]["cost"] = row["cost"]
            else:
                continue

        return G

    def to_broadcast_replication_topology(self, solution: BroadcastSolution) -> BroadcastReplicationTopology:
        """
        Convert ILP solution to BroadcastReplicationTopology
        """
        v_result = solution.var_instances_per_region
        # instance cost: v_result.sum() * (solution.problem.cost_per_instance_hr / 3600) * solution.problem.required_time_budget
        result = solution.var_edge_partitions

        result_g = nx.DiGraph()  # solution nx graph
        for i in range(result.shape[0]):
            edge = solution.var_edges[i]
            partitions = [chunk_i for chunk_i in range(result.shape[1]) if result[i][chunk_i] > 0.5]

            if len(partitions) == 0:
                continue

            src_node, dst_node = edge[0], edge[1]
            result_g.add_edge(src_node, dst_node, partitions=partitions, cost=self.G[src_node][dst_node]["cost"])

        for i in range(len(v_result)):
            num_vms = int(v_result[i])
            print(f"node: {solution.var_nodes[i]}, vms: {num_vms}")
            node = solution.var_nodes[i]
            if node in result_g.nodes:
                result_g.nodes[node]["num_vms"] = num_vms

        # TODO: the generated topo itself is wrong, but the networkx graph contains all information needed to generate gateway programs
        print("solution (edge): ", result_g.edges.data())
        print("solution (node):", result_g.nodes.data())
        return self.get_topo_from_nxgraph(solution.problem, result_g)

    def get_topo_from_nxgraph(self, p: BroadcastProblem, solution_graph: nx.DiGraph) -> BroadcastReplicationTopology:
        """
        Convert solutions (i.e. networkx graph) to BroadcastReplicationTopology
        """
        num_partitions = p.num_partitions
        partition_ids = list(range(num_partitions))
        partition_size_in_GB = p.gbyte_to_transfer / num_partitions

        source_region = p.src
        dst_regions = p.dsts

        topo = BroadcastReplicationTopology(solution_graph)
        cost_egress = 0.0

        # adding edges from object store
        for i in range(solution_graph.nodes[source_region]["num_vms"]):
            topo.add_objstore_instance_edge(source_region, source_region, i, partition_ids)

        # adding edges between instances from networkx DiGraph solutions
        for edge in solution_graph.edges.data():
            s, d = edge[0], edge[1]
            partitions_on_edge = edge[-1]["partitions"]
            cost_egress += len(partitions_on_edge) * partition_size_in_GB * edge[-1]["cost"]

            print(solution_graph.nodes.data())
            s_num_instances = solution_graph.nodes[s]["num_vms"]
            d_num_instances = solution_graph.nodes[d]["num_vms"]

            # TODO: fix it, might be wrong; if # of src region gateways != # of dst region gateways, how add the edge?
            for i in range(s_num_instances):
                for j in range(d_num_instances):
                    topo.add_instance_instance_edge(s, i, d, j, 0, partitions_on_edge)  # set num_connections = 0 for now

        # adding edges to object store
        for dst_region in dst_regions:
            for i in range(solution_graph.nodes[dst_region]["num_vms"]):
                topo.add_instance_objstore_edge(dst_region, i, dst_region, partition_ids)

        # set networkx solution graph in topo
        topo.cost_per_gb = cost_egress / gbyte_to_transfer  # cost per gigabytes
        return topo

    def get_throughput_grid(self):
        return np.array([e[2] for e in self.G.edges(data="throughput")])

    def get_cost_grid(self):
        return np.array([e[2] for e in self.G.edges(data="cost")])
