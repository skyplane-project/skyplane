import cvxpy as cp

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

    def solve_min_cost(self, p: ThroughputProblem, solver=cp.GLPK, solver_verbose=False, save_lp_path=None) -> ThroughputSolution:
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
