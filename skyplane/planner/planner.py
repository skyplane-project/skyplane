from importlib.resources import path

from typing import List, Optional, Tuple

from skyplane import compute
from skyplane.planner.topology_old import ReplicationTopology

from skyplane.planner.topology import TopologyPlan
from skyplane.broadcast.gateway.gateway_program import (
    GatewayProgram,
    GatewayMuxOr,
    GatewayReadObjectStore,
    GatewayReceive,
    GatewayWriteObjectStore,
    GatewaySend,
)

from skyplane.api.transfer_job import TransferJob


class Planner:
    def plan(self) -> TopologyPlan:
        raise NotImplementedError


class DirectPlanner(Planner):
    def __init__(self, n_instances: int, n_connections: int):
        self.n_instances = n_instances
        self.n_connections = n_connections
        super().__init__()

    def plan(self, jobs: List[TransferJob]) -> TopologyPlan:

        # make sure only single destination
        for job in jobs: 
            assert len(job.dst_ifaces) == 1, f"DirectPlanner only support single destination jobs, got {len(job.dst_ifaces)}"

        src_region_tag = jobs[0].src_iface.region_tag()
        dst_region_tag = jobs[0].dst_ifaces[0].region_tag()
        # jobs must have same sources and destinations
        for job in jobs[1:]:
            assert job.src_iface.region_tag() == src_region_tag, "All jobs must have same source region"
            assert job.dst_ifaces[0].region_tag() == dst_region_tag, "All jobs must have same destination region"

        print(src_region_tag, dst_region_tag)

        plan = TopologyPlan(src_region_tag=src_region_tag, dest_region_tags=[dst_region_tag])
        # TODO: use VM limits to determine how many instances to create in each region
        # TODO: support on-sided transfers but not requiring VMs to be created in source/destination regions
        for i in range(self.n_instances):
            plan.add_gateway(src_region_tag)
            plan.add_gateway(dst_region_tag)

        # ids of gateways in dst region
        dst_gateways = plan.get_region_gateways(dst_region_tag)

        src_program = GatewayProgram()
        dst_program = GatewayProgram()

        for job in jobs:
            src_bucket = job.src_iface.bucket()
            dst_bucket = job.dst_ifaces[0].bucket()

            # give each job a different partition id, so we can read/write to different buckets
            partition_id = jobs.index(job)

            # source region gateway program
            obj_store_read = src_program.add_operator(
                GatewayReadObjectStore(src_bucket, src_region_tag, self.n_connections), partition_id=partition_id
            )
            mux_or = src_program.add_operator(GatewayMuxOr(), parent_handle=obj_store_read, partition_id=partition_id)
            for i in range(self.n_instances):
                src_program.add_operator(
                    GatewaySend(target_gateway_id=dst_gateways[i].gateway_id, region=src_region_tag, num_connections=self.n_connections),
                    parent_handle=mux_or,
                    partition_id=partition_id,
                )

            # dst region gateway program
            recv_op = dst_program.add_operator(GatewayReceive(), partition_id=partition_id)
            dst_program.add_operator(
                GatewayWriteObjectStore(dst_bucket, dst_region_tag, self.n_connections), parent_handle=recv_op, partition_id=partition_id
            )

        # set gateway programs
        plan.set_gateway_program(src_region_tag, src_program)
        plan.set_gateway_program(dst_region_tag, dst_program)

        return plan


# class DirectPlanner(Planner):
#    def __init__(self, src_provider: str, src_region, dst_provider: str, dst_region: str, n_instances: int, n_connections: int):
#        self.n_instances = n_instances
#        self.n_connections = n_connections
#        super().__init__(src_provider, src_region, dst_provider, dst_region)
#
#    def plan(self) -> ReplicationTopology:
#        src_region_tag = f"{self.src_provider}:{self.src_region}"
#        dst_region_tag = f"{self.dst_provider}:{self.dst_region}"
#        if src_region_tag == dst_region_tag:  # intra-region transfer w/o solver
#            topo = ReplicationTopology()
#            for i in range(self.n_instances):
#                topo.add_objstore_instance_edge(src_region_tag, src_region_tag, i)
#                topo.add_instance_objstore_edge(src_region_tag, i, src_region_tag)
#            topo.cost_per_gb = 0
#            return topo
#        else:  # inter-region transfer w/ solver
#            topo = ReplicationTopology()
#            for i in range(self.n_instances):
#                topo.add_objstore_instance_edge(src_region_tag, src_region_tag, i)
#                topo.add_instance_instance_edge(src_region_tag, i, dst_region_tag, i, self.n_connections)
#                topo.add_instance_objstore_edge(dst_region_tag, i, dst_region_tag)
#            topo.cost_per_gb = compute.CloudProvider.get_transfer_cost(src_region_tag, dst_region_tag)
#            return topo


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
        from skyplane.planner.solver import ThroughputProblem

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
        from skyplane.planner.solver import ThroughputProblem

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
