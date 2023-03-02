import numpy as np

from skyplane.planner.solver import ThroughputSolver, ThroughputProblem, ThroughputSolution


class ThroughputSolverRON(ThroughputSolver):
    def solve(self, p: ThroughputProblem) -> ThroughputSolution:
        regions = self.get_regions()
        best_throughput = self.get_path_throughput(p.src, p.dst)
        best_path = [p.src, p.dst]
        for inter in regions:
            if inter == p.src or inter == p.dst:
                continue
            throughput = min(self.get_path_throughput(p.src, inter), self.get_path_throughput(inter, p.dst))
            if throughput > best_throughput:
                best_throughput = throughput
                best_path = [p.src, inter, p.dst]

        var_edge_flow_gigabits = np.zeros((len(regions), len(regions)))
        var_conn = np.zeros((len(regions), len(regions)))
        var_instances_per_region = np.zeros(len(regions))
        cost_per_gb = 0.0

        if len(best_path) == 2:
            segments = [(best_path[0], best_path[1])]
        elif len(best_path) == 3:
            segments = [(best_path[0], best_path[1]), (best_path[1], best_path[2])]

        for i, j in segments:
            idx_i = regions.index(i)
            idx_j = regions.index(j)
            var_edge_flow_gigabits[idx_i, idx_j] = self.get_path_throughput(i, j) * p.instance_limit
            var_conn[idx_i, idx_j] = p.benchmarked_throughput_connections * p.instance_limit
            var_instances_per_region[idx_i] = p.instance_limit
            var_instances_per_region[idx_j] = p.instance_limit
            cost_per_gb += self.get_path_cost(i, j)

        return ThroughputSolution(
            problem=p,
            is_feasible=True,
            var_edge_flow_gigabits=var_edge_flow_gigabits,
            var_conn=var_conn,
            var_instances_per_region=var_instances_per_region,
            throughput_achieved_gbits=best_throughput * p.instance_limit,
            cost_egress=cost_per_gb * p.gbyte_to_transfer,
        )
