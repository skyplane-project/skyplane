import os
import subprocess
from pathlib import Path

from skyplane import __root__
from skyplane.api.client import tmp_log_dir
from skyplane.broadcast.bc_plan import BroadcastReplicationTopology
from skyplane.broadcast.bc_solver import BroadcastProblem, BroadcastSolution, GBIT_PER_GBYTE

from typing import List, Optional
from pprint import pprint
import networkx as nx
import pandas as pd
import numpy as np
import cvxpy as cp

from skyplane.utils import logger


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
        cost_grid_path: Optional[Path] = __root__ / "profiles" / "cost.csv",
        tp_grid_path: Optional[Path] = __root__ / "profiles" / "throughput.csv",
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

    def make_nx_graph(self, cost, throughput):
        """
        Create nx graph with cost and throughput information on the edge
        """
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

        source_region = self.src_region
        dst_regions = self.dst_regions

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
        topo.default_max_conn_per_vm = self.num_connections
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
    ):
        super().__init__(
            src_provider, src_region, dst_providers, dst_regions, num_instances, num_connections, num_partitions, gbyte_to_transfer
        )

    def plan(self) -> BroadcastReplicationTopology:
        direct_graph = nx.DiGraph()
        for dst in self.dst_regions:
            cost_of_edge = self.G[self.src_region][dst]["cost"]
            direct_graph.add_edge(self.src_region, dst, partitions=list(range(self.num_partitions)), cost=cost_of_edge)

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
    ):
        super().__init__(
            src_provider, src_region, dst_providers, dst_regions, num_instances, num_connections, num_partitions, gbyte_to_transfer
        )

    def plan(self) -> BroadcastReplicationTopology:
        h = self.G.copy()
        h.remove_edges_from(list(h.in_edges(self.src_region)) + list(nx.selfloop_edges(h)))

        DST_graph = nx.algorithms.tree.Edmonds(h.subgraph([self.src_region] + self.dst_regions))
        opt_DST = DST_graph.find_optimum(attr="cost", kind="min", preserve_attrs=True, style="arborescence")

        # Construct MDST graph
        MDST_graph = nx.DiGraph()
        for edge in list(opt_DST.edges()):
            s, d = edge[0], edge[1]
            cost_of_edge = self.G[s][d]["cost"]
            MDST_graph.add_edge(s, d, partitions=list(range(self.num_partitions)), cost=cost_of_edge)

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
    ):
        super().__init__(
            src_provider, src_region, dst_providers, dst_regions, num_instances, num_connections, num_partitions, gbyte_to_transfer
        )

    def plan(self, hop_limit=3000) -> BroadcastReplicationTopology:
        # TODO: not usable now
        source_v, dest_v = self.src_region, self.dst_regions

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
                        di_stree_graph.add_edge(src_r, dst_r, partitions=list(range(self.num_partitions)), cost=cost_of_edge)

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
        )
        self.problem = BroadcastProblem(
            src=src_region,
            dsts=dst_regions,
            gbyte_to_transfer=gbyte_to_transfer,
            instance_limit=max_instances,
            num_partitions=num_partitions,
            required_time_budget=target_time,
        )

    @staticmethod
    def choose_solver():
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
            partitions = [partition_i for partition_i in range(result.shape[1]) if result[i][partition_i] > 0.5]

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
        return self.get_topo_from_nxgraph(solution.problem.num_partitions, solution.problem.gbyte_to_transfer, result_g)

    def plan(self, solver=cp.GUROBI, solver_verbose=False, save_lp_path=None) -> BroadcastReplicationTopology:
        problem = self.problem

        # OPTION1: use the graph with only source and destination nodes
        # g = self.G.subgraph([problem.src] + problem.dsts).copy()
        # cost = np.array([e[2] for e in g.edges(data="cost")])
        # tp = np.array([e[2] for e in g.edges(data="throughput")])

        # OPTION2: use the entire graph
        g = self.G
        cost = self.get_cost_grid()
        tp = self.get_throughput_grid()

        edges = list(g.edges)
        nodes = list(g.nodes)
        num_edges, num_nodes = len(edges), len(nodes)
        num_dest = len(problem.dsts)
        partition_size_gb = problem.gbyte_to_transfer / problem.num_partitions
        partition_size_gbit = partition_size_gb * GBIT_PER_GBYTE

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
        pprint(solution.to_summary_dict())
        return self.to_broadcast_replication_topology(solution)
