import functools
from importlib.resources import path
from typing import List, Optional, Tuple
import numpy as np
import collections
import pandas as pd
from skyplane.planner.solver_ilp import ThroughputSolverILP
from skyplane.planner.solver import ThroughputProblem, BroadcastProblem, BroadcastSolution, GBIT_PER_GBYTE
from skyplane import compute
from skyplane.planner.topology import TopologyPlan
from skyplane.gateway.gateway_program import (
    GatewayProgram,
    GatewayMuxOr,
    GatewayMuxAnd,
    GatewayReadObjectStore,
    GatewayWriteObjectStore,
    GatewayReceive,
    GatewaySend,
)
import networkx as nx
from skyplane.api.transfer_job import TransferJob
from pathlib import Path
from importlib.resources import files
from random import sample


class Planner:
    def __init__(self, n_instances: int, n_connections: int, n_partitions: Optional[int] = 1):
        self.n_instances = n_instances
        self.n_connections = n_connections
        self.n_partitions = n_partitions

    def logical_plan(self) -> nx.DiGraph:
        # create logical plan in nx.DiGraph format
        raise NotImplementedError

    def plan(self, jobs: List[TransferJob]) -> TopologyPlan:
        # create physical plan in TopologyPlan format
        raise NotImplementedError

    def verify_job_src_dsts(self, jobs: List[TransferJob], multicast=False) -> Tuple[str, List[str]]:
        src_region_tag = jobs[0].src_iface.region_tag()

        if multicast:
            # multicast checking
            dst_region_tags = [iface.region_tag() for iface in jobs[0].dst_ifaces]

            # jobs must have same sources and destinations
            for job in jobs[1:]:
                assert job.src_iface.region_tag() == src_region_tag, "All jobs must have same source region"
                assert [iface.region_tag() for iface in job.dst_ifaces] == dst_region_tags, "Add jobs must have same destination set"
        else:
            # unicast checking
            for job in jobs:
                assert len(job.dst_ifaces) == 1, f"DirectPlanner only support single destination jobs, got {len(job.dst_ifaces)}"

            # jobs must have same sources and destinations
            dst_region_tag = jobs[0].dst_ifaces[0].region_tag()
            for job in jobs[1:]:
                assert job.src_iface.region_tag() == src_region_tag, "All jobs must have same source region"
                assert job.dst_ifaces[0].region_tag() == dst_region_tag, "All jobs must have same destination region"
            dst_region_tags = [dst_region_tag]

        return src_region_tag, dst_region_tags

    @functools.lru_cache(maxsize=None)
    def make_nx_graph(self, tp_grid_path: Optional[Path] = files("data") / "throughput.csv") -> nx.DiGraph:
        # create throughput / cost graph for all regions for planner
        G = nx.DiGraph()
        throughput = pd.read_csv(tp_grid_path)
        for _, row in throughput.iterrows():
            if row["src_region"] == row["dst_region"]:
                continue
            G.add_edge(row["src_region"], row["dst_region"], cost=None, throughput=row["throughput_sent"] / 1e9)

        for edge in G.edges.data():
            if edge[-1]["cost"] is None:
                edge[-1]["cost"] = compute.CloudProvider.get_transfer_cost(edge[0], edge[1])

        assert all([edge[-1]["cost"] is not None for edge in G.edges.data()])
        return G

    def add_src_or_overlay_operator(
        self,
        solution_graph: nx.DiGraph,
        gateway_program: GatewayProgram,
        region: str,
        partition_ids: List[int],
        partition_offset: int,
        plan: TopologyPlan,
        bucket_info: Optional[Tuple[str, str]] = None,
        dst_op: Optional[GatewayReceive] = None,
    ) -> bool:
        """
        :param solution_graph: nx.DiGraph of solution
        :param gateway_program: GatewayProgram of region to add operator to
        :param region: region to add operator to
        :param partition_ids: list of partition ids to add operator to
        :param partition_offset: offset of partition ids
        :param plan: TopologyPlan of solution [for getting gateway ids]
        :param bucket_info: tuple of (bucket_name, bucket_region) for object store
        :param dst_op: if None, then this is either the source node or a overlay node; otherwise, this is the destination overlay node
        """
        g = solution_graph
        # partition_ids are set of ids that follow the same path from the out edges of the region
        any_id = partition_ids[0] - partition_offset
        next_regions = set([edge[1] for edge in g.out_edges(region, data=True) if str(any_id) in edge[-1]["partitions"]])

        # if partition_ids does not have a next region, then we cannot add an operator
        if len(next_regions) == 0:
            print(
                f"Region {region}, any id: {any_id}, partition ids: {partition_ids}, has no next region to forward data to: {g.out_edges(region, data=True)}"
            )
            return

        # identify if this is a destination overlay node or not
        if dst_op is None:
            # source node or overlay node
            # TODO: add generate data locally operator
            if bucket_info is None:
                receive_op = GatewayReceive()
            else:
                receive_op = GatewayReadObjectStore(
                    bucket_name=bucket_info[0], bucket_region=bucket_info[1], num_connections=self.n_connections
                )
        else:
            # destination overlay node, dst_op is the parent node
            receive_op = dst_op

        # find set of regions to send to for all partitions in partition_ids
        
        region_to_id_map = {}
        for next_region in next_regions:
            region_to_id_map[next_region] = []
            for i in range(solution_graph.nodes[next_region]["num_vms"]):
                region_to_id_map[next_region].append(plan.get_region_gateways(next_region)[i].gateway_id)

        # use muxand or muxor for partition_id
        operation = "MUX_AND" if len(next_regions) > 1 else "MUX_OR"
        mux_op = GatewayMuxAnd() if len(next_regions) > 1 else GatewayMuxOr()

        # non-dst node: add receive_op into gateway program
        if dst_op is None:
            gateway_program.add_operator(op=receive_op, partition_id=tuple(partition_ids))

        # MUX_AND: send this partition to multiple regions
        if operation == "MUX_AND":
            if dst_op is not None and dst_op.op_type == "mux_and":
                mux_op = receive_op
            else:  # do not add any nested mux_and if dst_op parent is mux_and
                gateway_program.add_operator(op=mux_op, parent_handle=receive_op.handle, partition_id=tuple(partition_ids))

            for next_region, next_region_ids in region_to_id_map.items():
                send_ops = [
                    GatewaySend(target_gateway_id=id, region=next_region, num_connections=self.n_connections) for id in next_region_ids
                ]

                # if there is more than one region to forward data to, add MUX_OR
                if len(next_region_ids) > 1:
                    mux_or_op = GatewayMuxOr()
                    gateway_program.add_operator(op=mux_or_op, parent_handle=mux_op.handle, partition_id=tuple(partition_ids))
                    gateway_program.add_operators(ops=send_ops, parent_handle=mux_or_op.handle, partition_id=tuple(partition_ids))
                else:
                    # otherwise, the parent of send_op is mux_op ("MUX_AND")
                    assert len(send_ops) == 1
                    gateway_program.add_operator(op=send_ops[0], parent_handle=mux_op.handle, partition_id=tuple(partition_ids))
        else:
            # only send this partition to a single region
            assert len(region_to_id_map) == 1
            next_region = list(region_to_id_map.keys())[0]
            ids = [id for next_region_ids in region_to_id_map.values() for id in next_region_ids]
            send_ops = [GatewaySend(target_gateway_id=id, region=next_region, num_connections=self.n_connections) for id in ids]

            # if num of gateways > 1, then connect to MUX_OR
            if len(ids) > 1:
                gateway_program.add_operator(op=mux_op, parent_handle=receive_op.handle, partition_id=tuple(partition_ids))
                gateway_program.add_operators(ops=send_ops, parent_handle=mux_op.handle)
            else:
                gateway_program.add_operators(ops=send_ops, parent_handle=receive_op.handle, partition_id=tuple(partition_ids))

        return True

    def add_dst_operator(
        self,
        solution_graph,
        gateway_program: GatewayProgram,
        region: str,
        partition_ids: List[int],
        partition_offset: int,
        plan: TopologyPlan,
        obj_store: Tuple[str, str] = None,
    ):
        # operator that receives data
        receive_op = GatewayReceive()
        gateway_program.add_operator(receive_op, partition_id=tuple(partition_ids))

        # operator that writes to the object store
        write_op = GatewayWriteObjectStore(bucket_name=obj_store[0], bucket_region=obj_store[1], num_connections=self.n_connections)

        g = solution_graph

        # partition_ids are ids that follow the same path from the out edges of the region
        any_id = partition_ids[0] - partition_offset
        next_regions = set([edge[1] for edge in g.out_edges(region, data=True) if str(any_id) in edge[-1]["partitions"]])

        # if no regions to forward data to, write to the object store
        if len(next_regions) == 0:
            gateway_program.add_operator(write_op, parent_handle=receive_op.handle, partition_id=tuple(partition_ids))

        # otherwise, receive and write to the object store, then forward data to next regions
        else:
            mux_and_op = GatewayMuxAnd()
            # receive and write
            gateway_program.add_operator(mux_and_op, parent_handle=receive_op.handle, partition_id=tuple(partition_ids))
            gateway_program.add_operator(write_op, parent_handle=mux_and_op.handle, partition_id=tuple(partition_ids))

            # forward: destination nodes are also forwarders
            self.add_src_or_overlay_operator(
                solution_graph, gateway_program, region, partition_ids, partition_offset, plan, dst_op=mux_and_op
            )

    def logical_plan_to_topology_plan(self, jobs: List[TransferJob], solution_graph: nx.graph) -> TopologyPlan:
        """
        Given a logical plan, construct a gateway program for each region in the logical plan for the given jobs.
        """
        # get source and destination regions
        src_ifaces, dst_ifaces = [job.src_iface for job in jobs], [job.dst_ifaces for job in jobs]
        src_region_tag = src_ifaces[0].region_tag()
        dst_region_tags = [dst_iface.region_tag() for dst_iface in dst_ifaces[0]]

        # map from the node to the gateway program
        region_to_gateway_program = {region: GatewayProgram() for region in solution_graph.nodes}

        # construct TopologyPlan for all the regions in solution_graph
        overlay_region_tags = [node for node in solution_graph.nodes if node != src_region_tag and node not in dst_region_tags]
        plan = TopologyPlan(src_region_tag=src_region_tag, dest_region_tags=dst_region_tags, overlay_region_tags=overlay_region_tags)
        for node in solution_graph.nodes:
            plan.add_gateway(node)

        # iterate through all the jobs
        for i in range(len(src_ifaces)):
            src_bucket = src_ifaces[i].bucket()
            dst_buckets = {dst_iface[i].region_tag(): dst_iface[i].bucket() for dst_iface in dst_ifaces}

            # iterate through all the regions in the solution graph
            for node in solution_graph.nodes:
                node_gateway_program = region_to_gateway_program[node]
                partition_to_next_regions = {}

                # give each job a different partition offset i, so we can read/write to different buckets
                for j in range(i, i + self.n_partitions):
                    partition_to_next_regions[j] = set(
                        [edge[1] for edge in solution_graph.out_edges(node, data=True) if str(j) in edge[-1]["partitions"]]
                    )

                keys_per_set = collections.defaultdict(list)
                for key, value in partition_to_next_regions.items():
                    keys_per_set[frozenset(value)].append(key)

                list_of_partitions = list(keys_per_set.values())

                # source node: read from object store or generate random data, then forward data
                for partitions in list_of_partitions:
                    if node == src_region_tag:
                        self.add_src_or_overlay_operator(
                            solution_graph,
                            node_gateway_program,
                            node,
                            partitions,
                            partition_offset=i,
                            plan=plan,
                            bucket_info=(src_bucket, node),
                        )

                    # dst receive data, write to object store, forward data if needed
                    elif node in dst_region_tags:
                        dst_bucket = dst_buckets[node]
                        self.add_dst_operator(
                            solution_graph,
                            node_gateway_program,
                            node,
                            partitions,
                            partition_offset=i,
                            plan=plan,
                            obj_store=(dst_bucket, node),
                        )

                    # overlay node only forward data
                    else:
                        self.add_src_or_overlay_operator(
                            solution_graph, node_gateway_program, node, partitions, partition_offset=i, plan=plan, bucket_info=None
                        )
            region_to_gateway_program[node] = node_gateway_program
            assert len(region_to_gateway_program) > 0, f"Empty gateway program {node}"

        for node in solution_graph.nodes:
            plan.set_gateway_program(node, region_to_gateway_program[node])

        for edge in solution_graph.edges.data():
            src_region, dst_region = edge[0], edge[1]
            plan.cost_per_gb += compute.CloudProvider.get_transfer_cost(src_region, dst_region) * (
                len(edge[-1]["partitions"]) / self.n_partitions
            )

        return plan


class MulticastDirectPlanner(Planner):
    def __init__(self, n_instances: int, n_connections: int, n_partitions: Optional[int] = 1):
        super().__init__(n_instances, n_connections, n_partitions)

    def logical_plan(self, src_region: str, dst_regions: List[str]) -> nx.DiGraph:
        graph = nx.DiGraph()
        graph.add_node(src_region)
        for dst_region in dst_regions:
            graph.add_node(dst_region)
            graph.add_edge(src_region, dst_region, partitions=[str(i) for i in range(self.n_partitions)])

        for node in graph.nodes:
            graph.nodes[node]["num_vms"] = self.n_instances
        return graph

    def plan(self, jobs: List[TransferJob]) -> TopologyPlan:
        src_region_tag, dst_region_tags = self.verify_job_src_dsts(jobs)
        solution_graph = self.logical_plan(src_region_tag, dst_region_tags)
        return self.logical_plan_to_topology_plan(jobs, solution_graph)


class MulticastILPPlanner(Planner):
    def __init__(
        self,
        n_instances: int,
        n_connections: int,
        target_time: Optional[float] = 10000,
        n_partitions: Optional[int] = 1,
        aws_only: bool = False,
        gcp_only: bool = False,
        azure_only: bool = False,
    ):
        super().__init__(n_instances, n_connections, n_partitions)
        self.target_time = target_time
        self.aws_only = aws_only
        self.gcp_only = gcp_only
        self.azure_only = azure_only
        self.G = super().make_nx_graph()

    def multicast_solution_to_nxgraph(self, solution: BroadcastSolution) -> nx.DiGraph:
        """
        Convert ILP solution to logical plan in nx graph
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

    def logical_plan(
        self,
        src_region: str,
        dst_regions: List[str],
        gbyte_to_transfer: int = 1,
        filter_node: bool = False,
        filter_edge: bool = False,
        solver_verbose: bool = False,
        save_lp_path: Optional[str] = None,
        solver: Optional[str] = None,
    ) -> nx.DiGraph:
        import cvxpy as cp

        if solver is None:
            solver = cp.GUROBI

        problem = BroadcastProblem(
            src=src_region,
            dsts=dst_regions,
            gbyte_to_transfer=gbyte_to_transfer,
            instance_limit=self.n_instances,
            num_partitions=self.n_partitions,
            required_time_budget=self.target_time,
        )

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

        return self.multicast_solution_to_nxgraph(solution)

    def plan(self, jobs: List[TransferJob]) -> TopologyPlan:
        src_region_tag, dst_region_tags = self.verify_job_src_dsts(jobs, multicast=True)
        solution_graph = self.logical_plan(src_region_tag, dst_region_tags)
        return self.logical_plan_to_topology_plan(jobs, solution_graph)


class MulticastMDSTPlanner(Planner):
    def __init__(self, n_instances: int, n_connections: int, n_partitions: Optional[int] = 1):
        super().__init__(n_instances, n_connections, n_partitions)
        self.G = super().make_nx_graph()

    def logical_plan(self, src_region: str, dst_regions: List[str]) -> nx.DiGraph:
        h = self.G.copy()
        h.remove_edges_from(list(h.in_edges(src_region)) + list(nx.selfloop_edges(h)))

        DST_graph = nx.algorithms.tree.Edmonds(h.subgraph([src_region] + dst_regions))
        opt_DST = DST_graph.find_optimum(attr="cost", kind="min", preserve_attrs=True, style="arborescence")

        # Construct MDST graph
        MDST_graph = nx.DiGraph()
        for edge in list(opt_DST.edges()):
            s, d = edge[0], edge[1]
            MDST_graph.add_edge(s, d, partitions=[str(i) for i in list(range(self.num_partitions))])

        for node in MDST_graph.nodes:
            MDST_graph.nodes[node]["num_vms"] = self.n_instances

        return MDST_graph

    def plan(self, jobs: List[TransferJob]) -> TopologyPlan:
        src_region_tag, dst_region_tags = self.verify_job_src_dsts(jobs, multicast=True)
        solution_graph = self.logical_plan(src_region_tag, dst_region_tags)
        return self.logical_plan_to_topology_plan(jobs, solution_graph)


class UnicastDirectPlanner(Planner):
    def __init__(self, n_instances: int, n_connections: int, n_partitions: Optional[int] = 1):
        super().__init__(n_instances, n_connections, n_partitions)

    def logical_plan(self, src_region: str, dst_regions: List[str]) -> nx.DiGraph:
        graph = nx.DiGraph()
        graph.add_node(src_region)
        for dst_region in dst_regions:
            graph.add_node(dst_region)
            graph.add_edge(src_region, dst_region, partitions=[str(i) for i in range(self.n_partitions)])

        for node in graph.nodes:
            graph.nodes[node]["num_vms"] = self.n_instances

        return graph

    def plan(self, jobs: List[TransferJob]) -> TopologyPlan:
        src_region_tag, dst_region_tag = self.verify_job_src_dsts(jobs)
        solution_graph = self.logical_plan(src_region_tag, dst_region_tag)
        return self.logical_plan_to_topology_plan(jobs, solution_graph)


class UnicastILPPlanner(Planner):
    def __init__(self, n_instances: int, n_connections: int, required_throughput_gbits: float, n_partitions: Optional[int] = 1):
        super().__init__(n_instances, n_connections, n_partitions)
        self.solver_required_throughput_gbits = required_throughput_gbits

    def logical_plan(self, src_region: str, dst_regions: List[str]) -> nx.DiGraph:
        problem = ThroughputProblem(
            src=src_region,
            dst=dst_regions,
            required_throughput_gbits=self.solver_required_throughput_gbits,
            gbyte_to_transfer=1,
            instance_limit=self.n_instances,
        )

        with path("skyplane.data", "throughput.csv") as solver_throughput_grid:
            tput = ThroughputSolverILP(solver_throughput_grid)
        solution = tput.solve_min_cost(problem)

        if not solution.is_feasible:
            raise RuntimeError("No feasible solution found")

        return tput.to_replication_topology(solution)

    def plan(self, jobs: List[TransferJob]) -> TopologyPlan:
        src_region_tag, dst_region_tag = self.verify_job_src_dsts(jobs)
        solution_graph = self.logical_plan(src_region_tag, dst_region_tag)
        return self.logical_plan_to_topology_plan(jobs, solution_graph)


class UnicastRONSolverPlanner(Planner):
    def __init__(self, n_instances: int, n_connections: int, required_throughput_gbits: float, n_partitions: Optional[int] = 1):
        super().__init__(n_instances, n_connections, n_partitions)
        self.solver_required_throughput_gbits = required_throughput_gbits

    def logical_plan(self, src_region: str, dst_regions: List[str]) -> nx.DiGraph:
        raise NotImplementedError("RON solver not implemented yet")

    def plan(self, jobs: List[TransferJob]) -> TopologyPlan:
        raise NotImplementedError("RON solver not implemented yet")
