import json
import shutil
from dataclasses import dataclass
from typing import Dict, List, Optional, Set, Tuple
from skyplane import MB

from skyplane.chunk import ChunkRequest
from skyplane.obj_store.object_store_interface import ObjectStoreObject
from skyplane.utils import logger


@dataclass
class ReplicationTopologyNode:
    region: str

    def to_dict(self) -> Dict:
        """Serialize to dict with type information."""
        return {"type": self.__class__.__name__, "fields": self.__dict__}

    @classmethod
    def from_dict(cls, topology_dict: Dict) -> "ReplicationTopologyNode":
        """Deserialize from dict with type information."""
        if topology_dict["type"] == "ReplicationTopologyGateway":
            return ReplicationTopologyGateway.from_dict_fields(topology_dict["fields"])
        elif topology_dict["type"] == "ReplicationTopologyObjectStore":
            return ReplicationTopologyObjectStore.from_dict_fields(topology_dict["fields"])
        else:
            raise ValueError("Unknown topology node type: {}".format(topology_dict["type"]))

    @classmethod
    def from_dict_fields(cls, fields: Dict):
        """Deserialize from dict with type information."""
        return cls(**fields)


@dataclass
class ReplicationTopologyGateway(ReplicationTopologyNode):
    instance: int

    def __hash__(self) -> int:
        return hash((self.region, self.instance))


@dataclass
class ReplicationTopologyObjectStore(ReplicationTopologyNode):
    def __hash__(self) -> int:
        return hash(self.region)


class ReplicationTopology:
    """
    ReplicationTopology stores a DAG where nodes are an instance in a cloud region
    (e.g. "aws:us-east-1", instance 0) and edges denote a connection to another
    cloud region (e.g. ("aws:us-east-1", 0) -> ("aws:us-west-2", 1) with an
    associated number of connections (e.g. 64).
    """

    def __init__(
        self,
        edges: Optional[List[Tuple[ReplicationTopologyNode, ReplicationTopologyNode, int]]] = None,
        cost_per_gb: Optional[float] = None,
    ):
        self.edges: List[Tuple[ReplicationTopologyNode, ReplicationTopologyNode, int]] = edges or []
        self.nodes: Set[ReplicationTopologyNode] = set(k[0] for k in self.edges) | set(k[1] for k in self.edges)
        self.cost_per_gb: Optional[float] = cost_per_gb

    @property
    def gateway_nodes(self) -> Set[ReplicationTopologyGateway]:
        return {n for n in self.nodes if isinstance(n, ReplicationTopologyGateway)}

    @property
    def obj_store_nodes(self) -> Set[ReplicationTopologyObjectStore]:
        return {n for n in self.nodes if isinstance(n, ReplicationTopologyObjectStore)}

    def add_instance_instance_edge(self, src_region: str, src_instance: int, dest_region: str, dest_instance: int, num_connections: int):
        """Add relay edge between two instances."""
        src_gateway = ReplicationTopologyGateway(src_region, src_instance)
        dest_gateway = ReplicationTopologyGateway(dest_region, dest_instance)
        self.edges.append((src_gateway, dest_gateway, int(num_connections)))
        self.nodes.add(src_gateway)
        self.nodes.add(dest_gateway)

    def add_objstore_instance_edge(self, src_region: str, dest_region: str, dest_instance: int):
        """Add object store to instance node (i.e. source bucket to source gateway)."""
        src_objstore = ReplicationTopologyObjectStore(src_region)
        dest_gateway = ReplicationTopologyGateway(dest_region, dest_instance)
        self.edges.append((src_objstore, dest_gateway, 0))
        self.nodes.add(src_objstore)
        self.nodes.add(dest_gateway)

    def add_instance_objstore_edge(self, src_region: str, src_instance: int, dest_region: str):
        """Add instance to object store edge (i.e. destination gateway to destination bucket)."""
        src_gateway = ReplicationTopologyGateway(src_region, src_instance)
        dest_objstore = ReplicationTopologyObjectStore(dest_region)
        self.edges.append((src_gateway, dest_objstore, 0))
        self.nodes.add(src_gateway)
        self.nodes.add(dest_objstore)

    def get_outgoing_paths(self, src: ReplicationTopologyNode):
        """Return nodes that follow src in the topology."""
        return {dest_gateway: num_connections for src_gateway, dest_gateway, num_connections in self.edges if src_gateway == src}

    def get_incoming_paths(self, dest: ReplicationTopologyNode):
        """Return nodes that precede dest in the topology."""
        return {src_gateway: num_connections for dest_gateway, src_gateway, num_connections in self.edges if dest_gateway == dest}

    def source_instances(self) -> Set[ReplicationTopologyGateway]:
        nodes = self.nodes - {v for u, v, _ in self.edges if not isinstance(u, ReplicationTopologyObjectStore)}
        return {n for n in nodes if isinstance(n, ReplicationTopologyGateway)}

    def sink_instances(self) -> Set[ReplicationTopologyGateway]:
        nodes = self.nodes - {u for u, v, _ in self.edges if not isinstance(v, ReplicationTopologyObjectStore)}
        return {n for n in nodes if isinstance(n, ReplicationTopologyGateway)}

    def source_region(self) -> str:
        instances = list(self.source_instances())
        assert all(
            i.region == instances[0].region for i in instances
        ), f"All source instances must be in the same region, but found {instances}"
        return instances[0].region

    def sink_region(self) -> str:
        instances = list(self.sink_instances())
        assert all(i.region == instances[0].region for i in instances), "All sink instances must be in the same region"
        return instances[0].region

    def per_region_count(self) -> Dict[str, int]:
        counts = {}
        for node in self.nodes:
            if isinstance(node, ReplicationTopologyGateway):
                counts[node.region] = counts.get(node.region, 0) + 1
        return counts

    def to_json(self):
        """
        Returns a JSON representation of the topology.
        """
        edges = []
        for e in self.edges:
            edges.append({"src": e[0].to_dict(), "dest": e[1].to_dict(), "num_connections": int(e[2])})
        return json.dumps(dict(replication_topology_edges=edges))

    @classmethod
    def from_json(cls, json_str: str):
        """
        Returns a ReplicationTopology from a JSON string.
        """
        in_dict = json.loads(json_str)
        assert "replication_topology_edges" in in_dict
        edges = []
        for edge in in_dict["replication_topology_edges"]:
            edges.append(
                (ReplicationTopologyNode.from_dict(edge["src"]), ReplicationTopologyNode.from_dict(edge["dest"]), edge["num_connections"])
            )
        return ReplicationTopology(edges)

    def to_graphviz(self):
        import graphviz as gv  # pytype: disable=import-error

        # if dot is not installed
        has_dot = shutil.which("dot") is not None
        if not has_dot:
            logger.error("Graphviz is not installed. Please install it to plot the solution (sudo apt install graphviz).")
            return None

        g = gv.Digraph(name="throughput_graph")
        g.attr(rankdir="LR")
        subgraphs = {}
        for src_gateway, dest_gateway, n_connections in self.edges:
            # group node instances by region
            src_region, src_instance = (
                src_gateway.region,
                src_gateway.instance if isinstance(src_gateway, ReplicationTopologyGateway) else "objstore",
            )
            dest_region, dest_instance = (
                dest_gateway.region,
                dest_gateway.instance if isinstance(dest_gateway, ReplicationTopologyGateway) else "objstore",
            )
            src_region, dest_region = src_region.replace(":", "/"), dest_region.replace(":", "/")
            src_node = f"{src_region}, {src_instance}"
            dest_node = f"{dest_region}, {dest_instance}"

            # make a subgraph for each region
            if src_region not in subgraphs:
                subgraphs[src_region] = gv.Digraph(name=f"cluster_{src_region}")
                subgraphs[src_region].attr(label=src_region)
            if dest_region not in subgraphs:
                subgraphs[dest_region] = gv.Digraph(name=f"cluster_{dest_region}")
                subgraphs[dest_region].attr(label=dest_region)

            # add nodes
            subgraphs[src_region].node(src_node, label=str(src_instance), shape="box")
            subgraphs[dest_region].node(dest_node, label=str(dest_instance), shape="box")

            # add edges
            g.edge(
                src_node,
                dest_node,
                label=f"{n_connections} connections" if src_instance != "objstore" and dest_instance != "objstore" else None,
            )

        for subgraph in subgraphs.values():
            g.subgraph(subgraph)

        return g


@dataclass
class ReplicationJob:
    source_region: str
    source_bucket: Optional[str]
    dest_region: str
    dest_bucket: Optional[str]

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
