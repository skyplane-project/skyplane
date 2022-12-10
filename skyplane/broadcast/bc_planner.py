import os
import subprocess
from pathlib import Path

from skyplane.api.client import tmp_log_dir
from skyplane.broadcast.bc_plan import BroadcastReplicationTopology
from skyplane.broadcast.bc_solver import BroadcastProblem, BroadcastSolution, GBIT_PER_GBYTE

from typing import List, Optional
from pprint import pprint
import networkx as nx
import pandas as pd
import numpy as np

from skyplane.utils import logger
from skyplane.broadcast import __root__
import functools
import colorama
from colorama import Fore, Style

class BroadcastPlanner:
    def __init__(
        self,
        src_provider: str,
        src_region,
        dst_providers: List[str],
        dst_regions: List[str],
        num_instances: int,
        num_connections: int,
        num_partitions: int,
        gbyte_to_transfer: float,
        aws_only: bool,
        gcp_only: bool,
        azure_only: bool,
        cost_grid_path: Optional[Path] = __root__ / "broadcast" / "profiles" / "cost.csv",
        tp_grid_path: Optional[Path] = __root__ / "broadcast" / "profiles" / "whole_throughput_11_28.csv",
    ):

        self.src_provider = src_provider
        self.src_region = src_region
        self.dst_providers = dst_providers
        self.dst_regions = dst_regions
        self.num_instances = num_instances
        self.num_connections = num_connections
        self.num_partitions = num_partitions
        self.gbyte_to_transfer = gbyte_to_transfer

        # need to input cost_grid and tp_grid
        self.costs = pd.read_csv(cost_grid_path)
        self.throughput = pd.read_csv(tp_grid_path)
        self.G = self.make_nx_graph(self.costs, self.throughput)

        if aws_only:
            self.G.remove_nodes_from([i for i in self.G.nodes if i.split(":")[0] == "gcp" or i.split(":")[0] == "azure"])
        elif gcp_only: 
            self.G.remove_nodes_from([i for i in self.G.nodes if i.split(":")[0] == "aws" or i.split(":")[0] == "azure"])
        elif azure_only:
            self.G.remove_nodes_from([i for i in self.G.nodes if i.split(":")[0] == "aws" or i.split(":")[0] == "gcp"])
        else:
            return 

    @functools.lru_cache(maxsize=None)
    def get_path_cost(self, src, dst, src_tier="PREMIUM", dst_tier="PREMIUM"):
        from skyplane.compute.cloud_provider import CloudProvider

        assert src_tier == "PREMIUM" and dst_tier == "PREMIUM"
        return CloudProvider.get_transfer_cost(src, dst)

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

        # update the cost using skyplane.compute tools [i.e. in utils.py] (need to build the skyplane repo first)
        for edge in G.edges.data():
            if edge[-1]["cost"] is None:
                edge[-1]["cost"] = self.get_path_cost(edge[0], edge[1])

        assert all([edge[-1]["cost"] is not None for edge in G.edges.data()])
        return G

    def get_throughput_grid(self):
        return np.array([e[2] for e in self.G.edges(data="throughput")])

    def get_cost_grid(self):
        return np.array([e[2] for e in self.G.edges(data="cost")])

    def get_topo_from_nxgraph(
        self, num_partitions: int, gbyte_to_transfer: float, solution_graph: nx.DiGraph
    ) -> BroadcastReplicationTopology:
        """
        Convert solutions (i.e. networkx graph) to BroadcastReplicationTopology
        """
        partition_ids = list(range(num_partitions))
        partition_ids = [str(id) for id in partition_ids]
        partition_size_in_GB = gbyte_to_transfer / num_partitions

        source_region = self.src_provider + ":" + self.src_region
        dst_regions = [f"{p}:{r}" for p, r in zip(self.dst_providers, self.dst_regions)]

        topo = BroadcastReplicationTopology(solution_graph, num_partitions)
        cost_egress = 0.0

        # adding edges from object store
        for i in range(solution_graph.nodes[source_region]["num_vms"]):
            topo.add_objstore_instance_edge(source_region, source_region, i, partition_ids)

        # adding edges between instances from networkx DiGraph solutions
        for edge in solution_graph.edges.data():
            s, d = edge[0], edge[1]
            partitions_on_edge = edge[-1]["partitions"]
            cost_egress += len(partitions_on_edge) * partition_size_in_GB * edge[-1]["cost"]

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

        tot_vm_price_per_s = 0 # total price per second 
        tot_vms = 0 # total number of vms 
        cost_map = {"gcp": 0.54, "aws": 0.54, "azure": 0.54} # cost per instance hours 

        for node in solution_graph.nodes:
            tot_vm_price_per_s += solution_graph.nodes[node]["num_vms"] * cost_map[node.split(":")[0]] / 3600
            tot_vms += solution_graph.nodes[node]["num_vms"]
 
        # set networkx solution graph in topo
        topo.cost_per_gb = cost_egress / gbyte_to_transfer  # cost per gigabytes
        topo.tot_vm_price_per_s = tot_vm_price_per_s
        topo.tot_vms = tot_vms
        topo.default_max_conn_per_vm = self.num_connections
        topo.nx_graph = solution_graph
        return topo

    def plan(self) -> BroadcastReplicationTopology:
        raise NotImplementedError


class BroadcastDirectPlanner(BroadcastPlanner):
    def __init__(
        self,
        src_provider: str,
        src_region,
        dst_providers: List[str],
        dst_regions: List[str],
        num_instances: int,
        num_connections: int,
        num_partitions: int,
        gbyte_to_transfer: float,
        aws_only: bool,
        gcp_only: bool,
        azure_only: bool,
    ):
        super().__init__(
            src_provider, src_region, dst_providers, dst_regions, num_instances, num_connections, num_partitions, gbyte_to_transfer, aws_only, gcp_only, azure_only
        )

    def plan(self) -> BroadcastReplicationTopology:
        direct_graph = nx.DiGraph()

        src = self.src_provider + ":" + self.src_region
        dsts = [f"{p}:{r}" for p, r in zip(self.dst_providers, self.dst_regions)]

        for dst in dsts:
            if src == dst:
                cost_of_edge = 0
            else:
                try:
                    cost_of_edge = self.G[src][dst]["cost"]
                except Exception:
                    raise ValueError(f"Missing cost edge {src}->{dst}")
            direct_graph.add_edge(src, dst, partitions=[str(i) for i in list(range(self.num_partitions))], cost=cost_of_edge)

        for node in direct_graph.nodes:
            direct_graph.nodes[node]["num_vms"] = self.num_instances
        return self.get_topo_from_nxgraph(self.num_partitions, self.gbyte_to_transfer, direct_graph)


class BroadcastMDSTPlanner(BroadcastPlanner):
    def __init__(
        self,
        src_provider: str,
        src_region,
        dst_providers: List[str],
        dst_regions: List[str],
        num_instances: int,
        num_connections: int,
        num_partitions: int,
        gbyte_to_transfer: float,
        aws_only: bool,
        gcp_only: bool,
        azure_only: bool,
    ):
        super().__init__(
            src_provider, src_region, dst_providers, dst_regions, num_instances, num_connections, num_partitions, gbyte_to_transfer, aws_only, gcp_only, azure_only
        )

    def plan(self) -> BroadcastReplicationTopology:
        src = self.src_provider + ":" + self.src_region
        dsts = [f"{p}:{r}" for p, r in zip(self.dst_providers, self.dst_regions)]

        h = self.G.copy()
        h.remove_edges_from(list(h.in_edges(src)) + list(nx.selfloop_edges(h)))

        DST_graph = nx.algorithms.tree.Edmonds(h.subgraph([src] + dsts))
        opt_DST = DST_graph.find_optimum(attr="cost", kind="min", preserve_attrs=True, style="arborescence")

        # Construct MDST graph
        MDST_graph = nx.DiGraph()
        for edge in list(opt_DST.edges()):
            s, d = edge[0], edge[1]
            cost_of_edge = self.G[s][d]["cost"]
            MDST_graph.add_edge(s, d, partitions=[str(i) for i in list(range(self.num_partitions))], cost=cost_of_edge)

        for node in MDST_graph.nodes:
            MDST_graph.nodes[node]["num_vms"] = self.num_instances

        return self.get_topo_from_nxgraph(self.num_partitions, self.gbyte_to_transfer, MDST_graph)


class BroadcastHSTPlanner(BroadcastPlanner):
    def __init__(
        self,
        src_provider: str,
        src_region,
        dst_providers: List[str],
        dst_regions: List[str],
        num_instances: int,
        num_connections: int,
        num_partitions: int,
        gbyte_to_transfer: float,
        aws_only: bool,
        gcp_only: bool,
        azure_only: bool,
    ):
        super().__init__(
            src_provider, src_region, dst_providers, dst_regions, num_instances, num_connections, num_partitions, gbyte_to_transfer, aws_only, gcp_only, azure_only
        )

    def plan(self, hop_limit=3000) -> BroadcastReplicationTopology:
        # TODO: not usable now
        source_v, dest_v = self.src_provider + ":" + self.src_region, [f"{p}:{r}" for p, r in zip(self.dst_providers, self.dst_regions)]

        h = self.G.copy()
        h.remove_edges_from(list(h.in_edges(source_v)) + list(nx.selfloop_edges(h)))

        nodes, edges = list(h.nodes), list(h.edges)
        num_nodes, num_edges = len(nodes), len(edges)
        id_to_name = {nodes.index(n) + 1: n for n in nodes}

        config_loc = tmp_log_dir / "write.set"
        write_loc = tmp_log_dir / "test.stplog"
        param_loc = tmp_log_dir / "test.stp"

        with open(config_loc, "w") as f:
            f.write('stp/logfile = "use_probname"')
            f.close()

        command = " ~/Documents/Packages/scipoptsuite-8.0.2/build/bin/applications/scipstp "
        command += f"-f {param_loc} -s {config_loc} -l {write_loc}"

        def construct_stp():
            section_begin = '33D32945 STP File, STP Format Version 1.0\n\nSECTION Comment\nName "Relay: cloud regions"\nCreator "S. Liu"\n'
            section_begin += f'Remark "Cloud region problem adapted from relay"\nEND\n\nSECTION Graph\n'
            section_begin += f"Nodes {num_nodes}\nEdges {num_edges}\nHopLimit {hop_limit}\n"

            Edge_info = []
            cnt = 0
            for edge in edges:
                s, d = nodes.index(edge[0]) + 1, nodes.index(edge[1]) + 1
                cost = h[edge[0]][edge[1]]["cost"]
                cnt += 1
                Edge_info.append(f"A {s} {d} {cost}\n")
                if cnt == num_edges:
                    Edge_info.append("END\n")

            s = nodes.index(source_v) + 1
            v = [nodes.index(i) + 1 for i in dest_v]
            terminal_info = [f"T {i}\n" for i in v]
            terminal_info.append("END\n\nEOF")
            section_terminal = f"""\nSECTION Terminals\nRoot {s}\nTerminals {len(dest_v)}\n"""

            with open(param_loc, "w") as f:
                f.write(section_begin)
                for edge in Edge_info:
                    f.write(edge.lstrip())
                f.write(section_terminal)
                for t in terminal_info:
                    f.write(t)
                f.close()
            return

        def read_result(loc):
            di_stree_graph = nx.DiGraph()
            with open(loc, "r") as f:
                lines = f.readlines()
                for line in lines:
                    if line.startswith("E") and len(line.split()) == 3:
                        l = line.split()
                        src_r, dst_r = id_to_name[int(l[1])], id_to_name[int(l[2])]
                        cost_of_edge = self.G[src_r][dst_r]["cost"]
                        di_stree_graph.add_edge(
                            src_r, dst_r, partitions=[str(i) for i in list(range(self.num_partitions))], cost=cost_of_edge
                        )

            for node in di_stree_graph.nodes:
                di_stree_graph.nodes[node]["num_vms"] = self.num_instances

            return di_stree_graph

        construct_stp()  # construct problem to a file
        process = subprocess.Popen(command, shell=True)  # run the steiner tree solver
        process.wait()
        solution_graph = read_result(loc=write_loc)

        os.remove(config_loc)
        os.remove(write_loc)
        os.remove(param_loc)
        return self.get_topo_from_nxgraph(self.num_partitions, self.gbyte_to_transfer, solution_graph)


class BroadcastILPSolverPlanner(BroadcastPlanner):
    def __init__(
        self,
        src_provider: str,
        src_region,
        dst_providers: List[str],
        dst_regions: List[str],
        max_instances: int,
        num_connections: int,
        num_partitions: int,
        gbyte_to_transfer: float,
        target_time: float,
        aws_only: bool,
        gcp_only: bool,
        azure_only: bool,

    ):
        super().__init__(
            src_provider,
            src_region,
            dst_providers,
            dst_regions,
            num_instances=max_instances,
            num_connections=num_connections,
            num_partitions=num_partitions,
            gbyte_to_transfer=gbyte_to_transfer,
            aws_only=aws_only, 
            gcp_only=gcp_only, 
            azure_only=azure_only
        )

        src = self.src_provider + ":" + self.src_region
        dsts = [f"{p}:{r}" for p, r in zip(self.dst_providers, self.dst_regions)]

        self.problem = BroadcastProblem(
            src=src,
            dsts=dsts,
            gbyte_to_transfer=gbyte_to_transfer,
            instance_limit=max_instances,
            num_partitions=num_partitions,
            required_time_budget=target_time,
        )

    @staticmethod
    def choose_solver():
        import cvxpy as cp

        try:
            import gurobipy as _grb  # pytype: disable=import-error

            return cp.GUROBI
        except ImportError:
            try:
                import cylp as _cylp  # pytype: disable=import-error

                logger.fs.warning("Gurobi not installed, using CoinOR instead.")
                return cp.CBC
            except ImportError:
                logger.fs.warning("Gurobi and CoinOR not installed, using GLPK instead.")
                return cp.GLPK

    def to_broadcast_replication_topology(self, solution: BroadcastSolution) -> BroadcastReplicationTopology:
        """
        Convert ILP solution to BroadcastReplicationTopology
        """
        v_result = solution.var_instances_per_region
        result = np.array(solution.var_edge_partitions)
        result_g = nx.DiGraph()  # solution nx graph
        for i in range(result.shape[0]):
            edge = solution.var_edges[i]
            partitions = [str(partition_i) for partition_i in range(result.shape[1]) if result[i][partition_i] > 0.5]

            if len(partitions) == 0:
                continue

            src_node, dst_node = edge[0], edge[1]
            result_g.add_edge(
                src_node,
                dst_node,
                partitions=partitions,
                throughput=self.G[src_node][dst_node]["throughput"],
                cost=self.G[src_node][dst_node]["cost"],
            )

        for i in range(len(v_result)):
            num_vms = int(v_result[i])
            node = solution.var_nodes[i]
            if node in result_g.nodes:
                result_g.nodes[node]["num_vms"] = num_vms

        # TODO: the generated topo itself is not used, but the networkx graph contains all information needed to generate gateway programs
        print(f"Solution # of edges: {len(result_g.edges)}, # of nodes: {len(result_g.nodes)}")
        print("solution (edge): ", result_g.edges.data())
        print()
        print("solution (node):", result_g.nodes.data())
        print()
        return self.get_topo_from_nxgraph(solution.problem.num_partitions, solution.problem.gbyte_to_transfer, result_g)

    def combine_partition_subgraphs(self, partition_g):
        bg = nx.DiGraph()
        for i in range(len(partition_g)):
            for edge in partition_g[i].edges:
                if edge[0] in bg and edge[1] in bg[edge[0]]:
                    bg[edge[0]][edge[1]]["partitions"].append(i)
                else:
                    e = self.G[edge[0].split(",")[0]][edge[1].split(",")[0]]
                    bg.add_edge(edge[0], edge[1], partitions=[i], cost=e["cost"], throughput=e["throughput"])

            for node in partition_g[i].nodes:
                if "num_vms" not in bg.nodes[node]:
                    bg.nodes[node]["num_vms"] = 0
                bg.nodes[node]["num_vms"] += partition_g[i].nodes[node]["num_vms"]

        return bg

    def total_broadcast_cost(self, bg):
        cost = 0
        for edge in bg.edges:
            cost += len(bg[edge[0]][edge[1]]["partitions"]) * bg[edge[0]][edge[1]]["cost"]
        return cost

    def create_topo_graph(self, p, v, g, edges, nodes):
        result = p
        v_result = v
        result_g = nx.DiGraph()

        for i in range(result.shape[0]):
            edge = edges[i]

            if result[i] == 0:
                continue
            for vm in [0]:  # range(int(v_result[nodes.index(edge[0])])): # multiple VMs
                result_g.add_edge(edge[0], edge[1], throughput=g[edge[0]][edge[1]]["throughput"], cost=g[edge[0]][edge[1]]["cost"])

        remove_nodes = []  # number of vms is one but the
        for i in range(len(v_result)):
            num_vms = int(v_result[i])
            if nodes[i] in result_g.nodes:
                result_g.nodes[nodes[i]]["num_vms"] = num_vms
            else:
                # print(f"Nodes: {nodes[i]}, number of vms: {num_vms}, not in result_g") --> why would this happen
                remove_nodes.append(nodes[i])

        return result_g

    def get_egress_ingress(self, g, nodes, edges, partition_size, p):

        partition_size *= 8  # need to convert to gbits?

        num_edges = len(edges)
        egress = []
        ingress = []
        for node in nodes:

            node_i = nodes.index(node)
            # egress
            i = np.zeros(num_edges)
            for e in g.edges:
                if e[0] == node:  # edge goes to dest
                    i[edges.index(e)] = 1
            egress.append(np.sum(i @ p) * partition_size)

            # ingress
            i = np.zeros(num_edges)
            for e in g.edges:
                if e[1] == node:
                    i[edges.index(e)] = 1
            ingress.append(np.sum(i @ p) * partition_size)

        return egress, ingress

    def solve_partition(
        self,
        g,
        cost,
        tp,
        nodes,
        edges,
        egress_limit,
        ingress_limit,
        existing_vms,  # total number of existing VMs per region
        existing_p,  # total number of partitions along each edge
        existing_egress,  # total egress for each region
        existing_ingress,  # total ingress for each region
        source_v,
        dest_v,
        partition_size_gb,
        instance_cost_s,
        max_vm_per_region,
        s,
        remaining_data_size_gb,  # how much remaining data needs to be sent
        filter_edge,  # whether to filter all except 1-hop
    ):
        import cvxpy as cp

        num_edges = len(edges)
        num_nodes = len(nodes)
        num_dest = len(dest_v)

        # indicator matrix (must be 2-D)
        p = cp.Variable((num_edges), boolean=True)  # whether edge is carrying partition
        n = cp.Variable((num_nodes), boolean=True)  # whether node transfers partition
        f = cp.Variable((num_nodes, num_nodes + 1), integer=True)  # enforce flow conservation

        v = cp.Variable((num_nodes), integer=True)  # number of VMs per region

        # optimization problem (minimize sum of costs)
        egress_cost = cp.sum(cost @ p) * partition_size_gb
        instance_cost = cp.sum(v) * instance_cost_s * s  # NOTE(sl): adding instance cost every time?
        obj = cp.Minimize(egress_cost + instance_cost)

        constraints = []

        # constraints on VM per region
        for i in range(num_nodes):
            constraints.append(v[i] <= max_vm_per_region - existing_vms[i])
            constraints.append(v[i] >= 0)

        # constraints to enforce flow between source/dest nodes
        for i in range(num_nodes):
            for j in range(num_nodes + 1):

                if i != j:

                    if j != num_nodes:
                        edge = (nodes[i], nodes[j])

                        constraints.append(f[i][j] <= p[edges.index(edge)] * num_dest)
                        # p = 0 -> f <= 0
                        # p = 1 -> f <= num_dest
                        constraints.append(f[i][j] >= (p[edges.index(edge)] - 1) * (num_dest + 1) + 1)
                        # p = 0 -> f >= -(num_dest)
                        # p = 1 -> f >= 1

                        constraints.append(f[i][j] == -f[j][i])

                        # capacity constraint for special node
                    else:
                        if nodes[i] in dest_v:  # only connected to destination nodes
                            constraints.append(f[i][j] <= 1)
                        else:
                            constraints.append(f[i][j] <= 0)
                else:
                    constraints.append(f[i][i] == 0)

            # flow conservation
            if nodes[i] != source_v and i != num_nodes + 1:
                constraints.append(cp.sum(f[i]) == 0)

        # source must have outgoing flow
        constraints.append(cp.sum(f[nodes.index(source_v), :]) == num_dest)

        # special node (connected to all destinations) must recieve all flow
        constraints.append(cp.sum(f[:, -1]) == num_dest)

        # node contained (or previously contained) if edge is contained
        for edge in edges:
            n0 = nodes.index(edge[0])
            n1 = nodes.index(edge[1])
            constraints.append(existing_vms[n0] + n[n0] >= cp.max(p[edges.index(edge)]))
            constraints.append(existing_vms[n1] + n[n1] >= cp.max(p[edges.index(edge)]))

        if filter_edge:
            # hop limit = 2: either source is source node, and/or dest is terminal node
            # all other edges must be 0
            # alternative: filter edges to matchi this
            for edge in edges:
                if edge[0] != source_v and edge[1] not in dest_v:
                    # cannot be in graph
                    constraints.append(p[edges.index(edge)] == 0)

        # throughput constraint
        # TODO: update edge constraints
        for edge_i in range(num_edges):
            # constraints.append(cp.sum(p[edge_i]*partition_size_gb*8) <= s*tp[edge_i])
            node_i = nodes.index(edge[0])
            constraints.append(
                cp.sum(p[edge_i] * partition_size_gb * 8) + cp.sum(existing_p[edge_i] * partition_size_gb * 8)
                <= s * tp[edge_i] * (v[node_i] + existing_vms[node_i])
            )

        # instance limits
        for node in nodes:

            node_i = nodes.index(node)
            # egress
            i = np.zeros(num_edges)
            for e in g.edges:
                if e[0] == node:  # edge goes to dest
                    i[edges.index(e)] = 1
            constraints.append(
                cp.sum(i @ p) * partition_size_gb * 8 + existing_egress[node_i]
                <= s * egress_limit[node_i] * (v[node_i] + existing_vms[node_i])
            )

            if node == source_v:
                # keep future solutions feasible by making sure source has enough remaining
                # egress capacity to send remaining data
                constraints.append(
                    cp.sum(i @ p) * partition_size_gb * 8 + existing_egress[node_i]
                    <= s * egress_limit[node_i] * (v[node_i] + existing_vms[node_i]) - remaining_data_size_gb * 8
                )

            # ingress
            #
            i = np.zeros(num_edges)
            for e in g.edges:
                if e[1] == node:  # edge goes to dest
                    i[edges.index(e)] = 1
            # keep future solutions feasible by making sure destinations have
            # enough remaining ingress to recieve the remaining data
            constraints.append(
                cp.sum(i @ p) * partition_size_gb * 8 + existing_ingress[node_i]
                <= s * ingress_limit[node_i] * (v[node_i] + existing_vms[node_i]) - remaining_data_size_gb * 8
            )

        prob = cp.Problem(obj, constraints)

        cost = prob.solve(solver=cp.GUROBI, verbose=False)

        if cost is None:
            print("No solution feasible")

        # NOTE: might not want to return the whole cost everytime, this looks wrong
        return cost, p, n, f, v, egress_cost.value, instance_cost.value

    def plan_iterative(
        self, 
        problem: BroadcastProblem, 
        solver=None,
        filter_node: bool = False,
        filter_edge: bool = False,
        solve_iterative: bool = False,
        solver_verbose: bool = False,
        save_lp_path: Optional[str] = None,
    ) -> BroadcastReplicationTopology:
        import cvxpy as cp

        if solver is None:
            solver = cp.GUROBI

        # get graph
        g = self.G

        # node-approximation
        if filter_node:
            src_dst_li = [problem.src] + problem.dsts
            sampled = [i for i in sample(list(self.G.nodes), 15) if i not in src_dst_li]
            g = g.subgraph(src_dst_li + sampled).copy()
            print(f"Filter node (only use): {src_dst_li + sampled}")

        cost = np.array([e[2] for e in g.edges(data="cost")])
        tp = np.array([e[2] for e in g.edges(data="throughput")])
        edges = list(g.edges)
        nodes = list(g.nodes)
        source_v = problem.src
        dest_v = problem.dsts
        partition_size_gb = problem.gbyte_to_transfer / problem.num_partitions

        dest_edges = [g.edges.index(e) for e in g.edges if e[1] == ""]
        num_edges = len(g.edges)
        num_nodes = len(nodes)
        num_partitions = problem.num_partitions
        num_dest = len(dest_v)
        instance_cost_s = problem.cost_per_instance_hr / 3600
        transfer_size_gb = problem.gbyte_to_transfer
        max_vm_per_region = problem.instance_limit

        print("Transfer size (GB):", transfer_size_gb)
        print("Instance cost per second:", instance_cost_s)
        print(f"Number partitions:", num_partitions)
        print(f"Max VM per region:", max_vm_per_region)
        print("Runtime:", problem.required_time_budget)

        total_v = np.zeros(num_nodes)
        total_p = np.zeros(num_edges)
        total_egress = np.zeros(num_nodes)
        total_ingress = np.zeros(num_nodes)
        total_cost, total_egress_cost, total_instance_cost = 0, 0, 0
        partition_g = []

        spare_tp = [0] * num_edges
        spare_egress_limit = [0] * num_nodes
        spare_ingress_limit = [0] * num_nodes
        sent_data_size = 0

        egress_limit = []
        ingress_limit = []
        for node in g.nodes:
            if "aws" in node:
                egress_limit_gbps, ingress_limit_gbps = problem.aws_instance_throughput_limit
            elif "gcp" in node:
                egress_limit_gbps, ingress_limit_gbps = problem.gcp_instance_throughput_limit
            elif "azure" in node:
                egress_limit_gbps, ingress_limit_gbps = problem.azure_instance_throughput_limit
            elif "cloudflare" in node:
                egress_limit_gbps, ingress_limit_gbps = 1, 1
            else:
                raise ValueError("node is not correct")

            egress_limit.append(egress_limit_gbps)
            ingress_limit.append(ingress_limit_gbps)

        partitions = range(num_partitions)
        for partition in partitions:
            print(f"Solving partition {partition}...")
            remaining_data_size_gb = partition_size_gb * len(partitions) - sent_data_size - partition_size_gb
            c_cost, c_p, c_n, c_f, c_v, egress_cost, instance_cost = self.solve_partition(
                g,
                cost,
                tp,
                nodes,
                edges,
                egress_limit,
                ingress_limit,
                existing_vms=total_v,
                existing_p=total_p,
                existing_egress=total_egress,
                existing_ingress=total_ingress,
                source_v=source_v,
                dest_v=dest_v,
                partition_size_gb=partition_size_gb,
                instance_cost_s=instance_cost_s,
                max_vm_per_region=max_vm_per_region,
                s=problem.required_time_budget,  # time budget
                remaining_data_size_gb=remaining_data_size_gb,
                filter_edge=filter_edge,
            )

            print("Cost: ", c_cost)
            # update state
            sent_data_size += partition_size_gb
            total_cost += c_cost
            total_egress_cost += egress_cost
            total_instance_cost += instance_cost
            total_v = total_v + np.array(c_v.value)
            total_p = total_p + np.array(c_p.value)
            total_egress, total_ingress = self.get_egress_ingress(g, nodes, edges, partition_size_gb, total_p)

            # append partition graph
            partition_g.append(self.create_topo_graph(np.array(c_p.value), np.array(c_v.value), g, edges, nodes))

        broadcast_g = self.combine_partition_subgraphs(partition_g)

        actual_tot_instance_cost = 0
        print()
        for node in broadcast_g.nodes:
            print(f"Node: {node}, num_vms: ", broadcast_g.nodes[node]["num_vms"])
            actual_tot_instance_cost += broadcast_g.nodes[node]["num_vms"] * problem.required_time_budget * instance_cost_s

        print("avg instance cost: ", problem.required_time_budget * instance_cost_s)
        actual_tot_cost = total_egress_cost + actual_tot_instance_cost

        print("Solver completes.\n")
        print(f"{Fore.BLUE}Time budget = {Fore.YELLOW}{problem.required_time_budget}s{Style.RESET_ALL}")
        print(
            f"{Fore.BLUE}Calculated tput = {Fore.YELLOW}{round(transfer_size_gb * 8 / problem.required_time_budget, 4)} Gbps{Style.RESET_ALL}\n"
        )
        print(f"{Fore.BLUE}Egress cost = {Fore.YELLOW}${round(total_egress_cost, 4)}{Style.RESET_ALL}")
        print(f"{Fore.BLUE}Actual instance cost = {Fore.YELLOW}${round(actual_tot_instance_cost, 4)}{Style.RESET_ALL}")
        print(f"{Fore.BLUE}Total cost = {Fore.YELLOW}${round(actual_tot_cost, 4)}{Style.RESET_ALL}\n")

        print(f"Solution (nodes): {broadcast_g.nodes.data()}\n")
        print(f"Solution (edges): {broadcast_g.edges.data()}\n")
        return self.get_topo_from_nxgraph(num_partitions, problem.gbyte_to_transfer, broadcast_g)

    def plan(
        self,
        solver=None,
        filter_node: bool = False,
        filter_edge: bool = False,
        solve_iterative: bool = False,
        solver_verbose: bool = False,
        save_lp_path: Optional[str] = None,
    ) -> BroadcastReplicationTopology:

        import cvxpy as cp

        if solver is None:
            solver = cp.GUROBI

        problem = self.problem

        if solve_iterative:
            return self.plan_iterative(problem, solver, filter_node, filter_edge, solver_verbose, save_lp_path)

        g = self.G

        # node-approximation
        if filter_node:
            src_dst_li = [problem.src] + problem.dsts
            sampled = [i for i in sample(list(self.G.nodes), 15) if i not in src_dst_li]
            g = g.subgraph(src_dst_li + sampled).copy()
            print(f"Filter node (only use): {src_dst_li + sampled}")

        cost = np.array([e[2] for e in g.edges(data="cost")])
        tp = np.array([e[2] for e in g.edges(data="throughput")])

        edges = list(g.edges)
        nodes = list(g.nodes)
        num_edges, num_nodes = len(edges), len(nodes)
        num_dest = len(problem.dsts)
        print(f"Num edges: {num_edges}, num nodes: {num_nodes}, num dest: {num_dest}, runtime budget: {problem.required_time_budget}s")

        partition_size_gb = problem.gbyte_to_transfer / problem.num_partitions
        partition_size_gbit = partition_size_gb * GBIT_PER_GBYTE
        print("Partition size (gbit): ", partition_size_gbit)

        # define variables
        p = cp.Variable((num_edges, problem.num_partitions), boolean=True)  # whether edge is carrying partition
        n = cp.Variable((num_nodes), boolean=True)  # whether node transfers partition
        f = cp.Variable((num_nodes * problem.num_partitions, num_nodes + 1), integer=True)  # enforce flow conservation
        v = cp.Variable((num_nodes), integer=True)  # number of VMs per region

        # define objective
        egress_cost = cp.sum(cost @ p) * partition_size_gb
        instance_cost = cp.sum(v) * (problem.cost_per_instance_hr / 3600) * problem.required_time_budget
        tot_cost = egress_cost + instance_cost
        obj = cp.Minimize(tot_cost)

        # define constants
        constraints = []

        # constraints on VM per region
        for i in range(num_nodes):
            constraints.append(v[i] <= problem.instance_limit)
            constraints.append(v[i] >= 0)

        # constraints to enforce flow between source/dest nodes
        for c in range(problem.num_partitions):

            for i in range(num_nodes):
                for j in range(num_nodes + 1):

                    if i != j:

                        if j != num_nodes:
                            edge = (nodes[i], nodes[j])

                            constraints.append(f[c * num_nodes + i][j] <= p[edges.index(edge)][c] * num_dest)
                            # p = 0 -> f <= 0
                            # p = 1 -> f <= num_dest
                            constraints.append(f[c * num_nodes + i][j] >= (p[edges.index(edge)][c] - 1) * (num_dest + 1) + 1)
                            # p = 0 -> f >= -(num_dest)
                            # p = 1 -> f >= 1

                            constraints.append(f[c * num_nodes + i][j] == -f[c * num_nodes + j][i])

                        # capacity constraint for special node
                        else:
                            if nodes[i] in problem.dsts:  # only connected to destination nodes
                                constraints.append(f[c * num_nodes + i][j] <= 1)
                            else:
                                constraints.append(f[c * num_nodes + i][j] <= 0)
                    else:
                        constraints.append(f[c * num_nodes + i][i] == 0)

                # flow conservation
                if nodes[i] != problem.src and i != num_nodes + 1:
                    constraints.append(cp.sum(f[c * num_nodes + i]) == 0)

            # source must have outgoing flow
            constraints.append(cp.sum(f[c * num_nodes + nodes.index(problem.src), :]) == num_dest)

            # special node (connected to all destinations) must recieve all flow
            constraints.append(cp.sum(f[c * num_nodes : (c + 1) * num_nodes, -1]) == num_dest)

        # node contained if edge is contained
        for edge in edges:
            constraints.append(n[nodes.index(edge[0])] >= cp.max(p[edges.index(edge)]))
            constraints.append(n[nodes.index(edge[1])] >= cp.max(p[edges.index(edge)]))

        # edge approximation
        if filter_edge:
            # hop limit = 2: either source is source node, and/or dest is terminal node
            # all other edges must be 0
            # alternative: filter edges to matchi this
            print("Filter edge")
            for edge in edges:
                if edge[0] != problem.src and edge[1] not in problem.dsts:
                    # cannot be in graph
                    constraints.append(p[edges.index(edge)] == 0)

        # throughput constraint
        for edge_i in range(num_edges):
            node_i = nodes.index(edge[0])
            constraints.append(cp.sum(p[edge_i] * partition_size_gbit) <= problem.required_time_budget * tp[edge_i] * v[node_i])

        # instance limits
        for node in nodes:
            region = node.split(":")[0]
            if region == "aws":
                ingress_limit_gbps, egress_limit_gbps = problem.aws_instance_throughput_limit
            elif region == "gcp":
                ingress_limit_gbps, egress_limit_gbps = problem.gcp_instance_throughput_limit
            elif region == "azure":
                ingress_limit_gbps, egress_limit_gbps = problem.azure_instance_throughput_limit
            elif region == "cloudflare":  # TODO: not supported yet in the tput / cost graph
                ingress_limit_gbps, egress_limit_gbps = 1, 1

            node_i = nodes.index(node)
            # egress
            i = np.zeros(num_edges)
            for e in g.edges:
                if e[0] == node:  # edge goes to dest
                    i[edges.index(e)] = 1

            constraints.append(cp.sum(i @ p) * partition_size_gbit <= problem.required_time_budget * egress_limit_gbps * v[node_i])

            # ingress
            i = np.zeros(num_edges)
            for e in g.edges:
                # edge goes to dest
                if e[1] == node:
                    i[edges.index(e)] = 1
            constraints.append(cp.sum(i @ p) * partition_size_gbit <= problem.required_time_budget * ingress_limit_gbps * v[node_i])

        print("Define problem done.")

        # solve
        prob = cp.Problem(obj, constraints)
        if solver == cp.GUROBI or solver == "gurobi":
            solver_options = {}
            solver_options["Threads"] = 1
            if save_lp_path:
                solver_options["ResultFile"] = str(save_lp_path)
            if not solver_verbose:
                solver_options["OutputFlag"] = 0
            cost = prob.solve(verbose=solver_verbose, qcp=True, solver=cp.GUROBI, reoptimize=True, **solver_options)
        elif solver == cp.CBC or solver == "cbc":
            solver_options = {}
            solver_options["maximumSeconds"] = 60
            solver_options["numberThreads"] = 1
            cost = prob.solve(verbose=solver_verbose, solver=cp.CBC, **solver_options)
        else:
            cost = prob.solve(solver=solver, verbose=solver_verbose)

        if prob.status == "optimal":
            solution = BroadcastSolution(
                problem=problem,
                is_feasible=True,
                var_edges=edges,
                var_nodes=nodes,
                var_edge_partitions=p.value,
                var_node_transfer_partitions=n.value,
                var_instances_per_region=v.value,
                var_flow=f.value,
                cost_egress=egress_cost.value,
                cost_instance=instance_cost.value,
                cost_total=tot_cost.value,
            )
        else:
            solution = BroadcastSolution(problem=problem, is_feasible=False, extra_data=dict(status=prob.status))

        print("ILP solution: ")
        # pprint(solution.to_summary_dict())
        return self.to_broadcast_replication_topology(solution)
