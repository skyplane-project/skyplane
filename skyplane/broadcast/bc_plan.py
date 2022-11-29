from dataclasses import dataclass
from typing import List, Optional, Tuple, Set

import networkx as nx

from skyplane.chunk import ChunkRequest
from skyplane.obj_store.object_store_interface import ObjectStoreObject
from skyplane.replicate.replication_plan import (
    ReplicationTopology,
    ReplicationTopologyNode,
    ReplicationTopologyGateway,
    ReplicationTopologyObjectStore,
)
from skyplane.utils.definitions import MB


@dataclass
class BroadcastReplicationJob:
    ource_region: str
    dest_regions: List[str]
    source_bucket: Optional[str]
    dest_buckets: Optional[List[str]]

    # object transfer pairs (src, dest)
    transfer_pairs: List[Tuple[ObjectStoreObject, ObjectStoreObject]]

    # progress tracking via a list of chunk_requests
    chunk_requests: Optional[List[ChunkRequest]] = None

    # Generates random chunks for testing on the gateways
    random_chunk_size_mb: Optional[int] = None

    @property
    def transfer_size(self):
        if not self.random_chunk_size_mb:
            return sum(source_object.size for source_object, _ in self.transfer_pairs)
        else:
            return self.random_chunk_size_mb * len(self.transfer_pairs) * MB


class BroadcastReplicationTopology(ReplicationTopology):
    """
    Multiple destinations - edges specify a which partition of data they are responsible for carrying
    """

    def __init__(
        self,
        nx_graph: nx.DiGraph,
        num_partitions: int,
        edges: Optional[List[Tuple[ReplicationTopologyNode, ReplicationTopologyNode, int, str]]] = None,
        cost_per_gb: Optional[float] = None,
        default_max_conn_per_vm: Optional[int] = None,
    ):

        """
        Edge is represented by:
        Tuple[ReplicationTopologyNode, ReplicationTopologyNode, int, int] -> [src_node, dst_node, num_conn, partition_index]

        """
        self.nx_graph = nx_graph
        self.num_partitions = num_partitions

        self.edges: List[Tuple[ReplicationTopologyNode, ReplicationTopologyNode, int, str]] = edges or []
        self.nodes: Set[ReplicationTopologyNode] = set(k[0] for k in self.edges) | set(k[1] for k in self.edges)
        self.cost_per_gb: Optional[float] = cost_per_gb
        self.default_max_conn_per_vm: Optional[int] = default_max_conn_per_vm

    def get_outgoing_paths(self, src: ReplicationTopologyNode):
        """Return nodes that follow src in the topology."""
        return {dest_gateway: num_connections for src_gateway, dest_gateway, num_connections, _ in self.edges if src_gateway == src}

    def get_incoming_paths(self, dest: ReplicationTopologyNode):
        """Return nodes that precede dest in the topology."""
        return {src_gateway: num_connections for dest_gateway, src_gateway, num_connections, _ in self.edges if dest_gateway == dest}

    def sink_regions(self) -> List[str]:
        instances = list(self.sink_instances())
        # assert all(i.region == instances[0].region for i in instances), "All sink instances must be in the same region"
        return [instance.region for instance in instances]

    def source_instances(self) -> Set[ReplicationTopologyGateway]:
        nodes = self.nodes - {v for u, v, _, _ in self.edges if not isinstance(u, ReplicationTopologyObjectStore)}
        return {n for n in nodes if isinstance(n, ReplicationTopologyGateway)}

    def sink_instances(self) -> Set[ReplicationTopologyGateway]:
        nodes = {u for u, v, _, _ in self.edges if isinstance(v, ReplicationTopologyObjectStore)}
        return {n for n in nodes if isinstance(n, ReplicationTopologyGateway)}

    def add_instance_instance_edge(
        self,
        src_region: str,
        src_instance: int,
        dest_region: str,
        dest_instance: int,
        num_connections: int,
        partition_ids: List[str],
    ):
        """Add relay edge between two instances."""
        src_gateway = ReplicationTopologyGateway(src_region, src_instance)
        dest_gateway = ReplicationTopologyGateway(dest_region, dest_instance)
        for partition_id in partition_ids:
            self.edges.append((src_gateway, dest_gateway, int(num_connections), partition_id))
        self.nodes.add(src_gateway)
        self.nodes.add(dest_gateway)

    def add_objstore_instance_edge(self, src_region: str, dest_region: str, dest_instance: int, partition_ids: List[str]):
        """Add object store to instance node (i.e. source bucket to source gateway)."""
        src_objstore = ReplicationTopologyObjectStore(src_region)
        dest_gateway = ReplicationTopologyGateway(dest_region, dest_instance)
        for partition_id in partition_ids:
            self.edges.append((src_objstore, dest_gateway, 0, partition_id))
        self.nodes.add(src_objstore)
        self.nodes.add(dest_gateway)

    def add_instance_objstore_edge(self, src_region: str, src_instance: int, dest_region: str, partition_ids: List[str]):
        """Add instance to object store edge (i.e. destination gateway to destination bucket)."""
        src_gateway = ReplicationTopologyGateway(src_region, src_instance)
        dest_objstore = ReplicationTopologyObjectStore(dest_region)
        for partition_id in partition_ids:
            self.edges.append((src_gateway, dest_objstore, 0, partition_id))
        self.nodes.add(src_gateway)
        self.nodes.add(dest_objstore)
