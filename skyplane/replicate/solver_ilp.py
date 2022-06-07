import shutil
from collections import namedtuple
from typing import Tuple, List, Dict, Optional

import cvxpy as cp
import graphviz as gv
import numpy as np

from skyplane.replicate.replication_plan import ReplicationTopology
from skyplane.replicate.solver import ThroughputSolver, ThroughputProblem, GBIT_PER_GBYTE, ThroughputSolution
from skyplane.utils import logger


class ThroughputSolverILP(ThroughputSolver):
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

    def solve_min_cost(self, p: ThroughputProblem, solver=cp.GLPK, solver_verbose=False, save_lp_path=None):
        regions = self.get_regions()
        sources = [regions.index(p.src)]
        sinks = [regions.index(p.dst)]

        # define constants
        if p.const_throughput_grid_gbits is None:
            p.const_throughput_grid_gbits = self.get_throughput_grid()
        if p.const_cost_per_gb_grid is None:
            p.const_cost_per_gb_grid = self.get_cost_grid()

        # define variables
        edge_flow_gigabits = cp.Variable((len(regions), len(regions)), name="edge_flow_gigabits")
        conn = cp.Variable((len(regions), len(regions)), name="conn")
        instances_per_region = cp.Variable((len(regions)), name="instances_per_region", integer=True)
        node_flow_in = cp.sum(edge_flow_gigabits, axis=0)
        node_flow_out = cp.sum(edge_flow_gigabits, axis=1)

        constraints = []

        # instance limit
        constraints.append(instances_per_region <= p.instance_limit)
        constraints.append(instances_per_region >= 0)

        # connection limits
        constraints.append(conn >= 0)
        constraints.append(cp.sum(conn, axis=1) <= instances_per_region * p.benchmarked_throughput_connections)  # egress
        for i in range(len(regions)):
            constraints.append(cp.sum(conn[:, i]) <= instances_per_region[i] * p.benchmarked_throughput_connections)  # ingress

        # flow capacity constraint
        adjusted_edge_capacity_gigabits = cp.multiply(p.const_throughput_grid_gbits, conn / p.benchmarked_throughput_connections)
        constraints.append(edge_flow_gigabits <= adjusted_edge_capacity_gigabits)

        # flow conservation
        for v in range(len(regions)):
            f_in, f_out = node_flow_in[v], node_flow_out[v]
            if v in sources:
                constraints.append(f_in == 0)
                constraints.append(f_out == p.required_throughput_gbits)
            elif v in sinks:
                constraints.append(f_in == p.required_throughput_gbits)
            else:
                constraints.append(f_in == f_out)

        # non-negative flow constraint
        constraints.append(edge_flow_gigabits >= 0)

        # instance throughput constraints
        for idx, r in enumerate(regions):
            provider = r.split(":")[0]
            if provider == "aws":
                egress_limit, ingress_limit = p.aws_instance_throughput_limit
            elif provider == "gcp":
                egress_limit, ingress_limit = p.gcp_instance_throughput_limit
            elif provider == "azure":
                egress_limit, ingress_limit = p.azure_instance_throughput_limit
            else:
                raise ValueError(f"Unknown provider {provider}")
            constraints.append(node_flow_in[idx] <= ingress_limit * instances_per_region[idx])
            constraints.append(node_flow_out[idx] <= egress_limit * instances_per_region[idx])

        # define objective
        transfer_size_gbit = p.gbyte_to_transfer * GBIT_PER_GBYTE
        assert p.required_throughput_gbits > 0
        runtime_s = transfer_size_gbit / p.required_throughput_gbits  # gbit * s / gbit = s
        cost_per_edge = cp.multiply(edge_flow_gigabits * runtime_s, p.const_cost_per_gb_grid / GBIT_PER_GBYTE)  #  gbit/s * $/gbit = $/s
        cost_egress = cp.sum(cost_per_edge)

        # instance cost
        per_instance_cost: float = p.cost_per_instance_hr / 3600 * (runtime_s + p.instance_provision_time_s)
        instance_cost = cp.sum(instances_per_region) * per_instance_cost
        total_cost = cost_egress + instance_cost * p.instance_cost_multiplier
        prob = cp.Problem(cp.Minimize(total_cost), constraints)

        if solver == cp.GUROBI or solver == "gurobi":
            solver_options = {}
            solver_options["Threads"] = 1
            if save_lp_path:
                solver_options["ResultFile"] = str(save_lp_path)
            if not solver_verbose:
                solver_options["OutputFlag"] = 0
            prob.solve(verbose=solver_verbose, qcp=True, solver=cp.GUROBI, reoptimize=True, **solver_options)
        elif solver == cp.CBC or solver == "cbc":
            solver_options = {}
            solver_options["maximumSeconds"] = 60
            solver_options["numberThreads"] = 1
            prob.solve(verbose=solver_verbose, solver=cp.CBC, **solver_options)
        else:
            prob.solve(solver=solver, verbose=solver_verbose)

        baseline_throughput, baseline_egress_cost, baseline_instance_cost = self.get_baseline_throughput_and_cost(p)
        if prob.status == "optimal":
            return ThroughputSolution(
                problem=p,
                is_feasible=True,
                var_edge_flow_gigabits=edge_flow_gigabits.value.round(6),
                var_conn=conn.value.round(6),
                var_instances_per_region=instances_per_region.value,
                throughput_achieved_gbits=[node_flow_in[i].value for i in sinks],
                cost_egress_by_edge=cost_per_edge.value,
                cost_egress=cost_egress.value,
                cost_instance=instance_cost.value,
                cost_total=instance_cost.value + cost_egress.value,
                transfer_runtime_s=runtime_s,
                baseline_throughput_achieved_gbits=baseline_throughput,
                baseline_cost_egress=baseline_egress_cost,
                baseline_cost_instance=baseline_instance_cost,
                baseline_cost_total=baseline_egress_cost + baseline_instance_cost,
            )
        else:
            return ThroughputSolution(
                problem=p,
                is_feasible=False,
                extra_data=dict(status=prob.status),
                baseline_throughput_achieved_gbits=baseline_throughput,
                baseline_cost_egress=baseline_egress_cost,
                baseline_cost_instance=baseline_instance_cost,
                baseline_cost_total=baseline_egress_cost + baseline_instance_cost,
            )

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

    def plot_graphviz(self, solution: ThroughputSolution) -> Optional[gv.Digraph]:
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
                        src_region=e.src_region,
                        dest_region=e.src_region,
                        dest_instance=e.src_instance_idx,
                    )
                    obj_store_edges.add(("src", e.src_region, e.src_instance_idx))

                # connect destination instances to destination gateway
                if e.dst_region == solution.problem.dst and ("dst", e.dst_region, e.dst_instance_idx) not in obj_store_edges:
                    replication_topology.add_instance_objstore_edge(
                        src_region=e.dst_region,
                        src_instance=e.dst_instance_idx,
                        dest_region=e.dst_region,
                    )
                    obj_store_edges.add(("dst", e.dst_region, e.dst_instance_idx))

        return replication_topology, scale_factor
