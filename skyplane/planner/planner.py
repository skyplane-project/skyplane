from importlib.resources import path

from skyplane import compute
from skyplane.planner.solver import ThroughputProblem
from skyplane.planner.topology import ReplicationTopology


class Planner:
    def __init__(self, src_provider: str, src_region, dst_provider: str, dst_region: str):
        self.src_provider = src_provider
        self.src_region = src_region
        self.dst_provider = dst_provider
        self.dst_region = dst_region

    def plan(self) -> ReplicationTopology:
        raise NotImplementedError


class DirectPlanner(Planner):
    def __init__(self, src_provider: str, src_region, dst_provider: str, dst_region: str, n_instances: int, n_connections: int):
        self.n_instances = n_instances
        self.n_connections = n_connections
        super().__init__(src_provider, src_region, dst_provider, dst_region)

    def plan(self) -> ReplicationTopology:
        src_region_tag = f"{self.src_provider}:{self.src_region}"
        dst_region_tag = f"{self.dst_provider}:{self.dst_region}"
        if src_region_tag == dst_region_tag:  # intra-region transfer w/o solver
            topo = ReplicationTopology()
            for i in range(self.n_instances):
                topo.add_objstore_instance_edge(src_region_tag, src_region_tag, i)
                topo.add_instance_objstore_edge(src_region_tag, i, src_region_tag)
            topo.cost_per_gb = 0
            return topo
        else:  # inter-region transfer w/ solver
            topo = ReplicationTopology()
            for i in range(self.n_instances):
                topo.add_objstore_instance_edge(src_region_tag, src_region_tag, i)
                topo.add_instance_instance_edge(src_region_tag, i, dst_region_tag, i, self.n_connections)
                topo.add_instance_objstore_edge(dst_region_tag, i, dst_region_tag)
            topo.cost_per_gb = compute.CloudProvider.get_transfer_cost(src_region_tag, dst_region_tag)
            return topo


class ILPSolverPlanner(Planner):
    def __init__(
        self,
        src_provider: str,
        src_region,
        dst_provider: str,
        dst_region: str,
        max_instances: int,
        max_connections: int,
        required_throughput_gbits: float,
    ):
        self.max_instances = max_instances
        self.max_connections = max_connections
        self.solver_required_throughput_gbits = required_throughput_gbits
        super().__init__(src_provider, src_region, dst_provider, dst_region)

    def plan(self) -> ReplicationTopology:
        from skyplane.planner.solver_ilp import ThroughputSolverILP

        problem = ThroughputProblem(
            src=f"{self.src_provider}:{self.src_region}",
            dst=f"{self.dst_provider}:{self.dst_region}",
            required_throughput_gbits=self.solver_required_throughput_gbits,
            gbyte_to_transfer=1,
            instance_limit=self.max_instances,
        )

        with path("skyplane.data", "throughput.csv") as solver_throughput_grid:
            tput = ThroughputSolverILP(solver_throughput_grid)
        solution = tput.solve_min_cost(problem, solver=ThroughputSolverILP.choose_solver(), save_lp_path=None)
        if not solution.is_feasible:
            raise ValueError("ILP solver failed to find a solution, try solving with fewer constraints")
        topo, _ = tput.to_replication_topology(solution)
        return topo


class RONSolverPlanner(Planner):
    def __init__(
        self,
        src_provider: str,
        src_region,
        dst_provider: str,
        dst_region: str,
        max_instances: int,
        max_connections: int,
        required_throughput_gbits: float,
    ):
        self.max_instances = max_instances
        self.max_connections = max_connections
        self.solver_required_throughput_gbits = required_throughput_gbits
        super().__init__(src_provider, src_region, dst_provider, dst_region)

    def plan(self) -> ReplicationTopology:
        from skyplane.planner.solver_ron import ThroughputSolverRON

        problem = ThroughputProblem(
            src=self.src_region,
            dst=self.dst_region,
            required_throughput_gbits=self.solver_required_throughput_gbits,
            gbyte_to_transfer=1,
            instance_limit=self.max_instances,
        )

        with path("skyplane.data", "throughput.csv") as solver_throughput_grid:
            tput = ThroughputSolverRON(solver_throughput_grid)
        solution = tput.solve(problem)
        topo, _ = tput.to_replication_topology(solution)
        return topo
