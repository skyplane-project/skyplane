from skyplane import compute
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
    def __init__(self, src_provider: str, src_region, dst_provider: str, dst_region: str, max_instances: int, max_connections: int):
        self.max_instances = max_instances
        self.max_connections = max_connections
        super().__init__(src_provider, src_region, dst_provider, dst_region)

    def plan(self) -> ReplicationTopology:
        raise NotImplementedError


class RONSolverPlanner(Planner):
    def __init__(self, src_provider: str, src_region, dst_provider: str, dst_region: str, max_instances: int, max_connections: int):
        self.max_instances = max_instances
        self.max_connections = max_connections
        super().__init__(src_provider, src_region, dst_provider, dst_region)

    def plan(self) -> ReplicationTopology:
        raise NotImplementedError
