from dataclasses import dataclass
from collections import namedtuple
import json
import shutil
from typing import Dict, List, Optional, Set, Tuple

from loguru import logger
import graphviz as gv
from skylark.chunk import ChunkRequest

from skylark.obj_store.s3_interface import S3Interface
from skylark.obj_store.gcs_interface import GCSInterface
from skylark.utils.utils import do_parallel


@dataclass
class ReplicationTopologyGateway:
    region: str
    instance_idx: int

    def to_dict(self):
        return {
            "region": self.region,
            "instance_idx": self.instance_idx,
        }

    @staticmethod
    def from_dict(topology_dict: Dict):
        return ReplicationTopologyGateway(
            region=topology_dict["region"],
            instance_idx=topology_dict["instance_idx"],
        )
    
    def __hash__(self) -> int:
        return hash(self.region) + hash(self.instance_idx)


class ReplicationTopology:
    """
    ReplicationTopology stores a DAG where nodes are an instance in a cloud region
    (e.g. "aws:us-east-1", instance 0) and edges denote a connection to another
    cloud region (e.g. ("aws:us-east-1", 0) -> ("aws:us-west-2", 1) with an
    associated number of connections (e.g. 64).
    """

    def __init__(self, edges: Optional[List[Tuple[ReplicationTopologyGateway, ReplicationTopologyGateway, int]]] = None):
        self.edges: List[Tuple[ReplicationTopologyGateway, ReplicationTopologyGateway, int]] = edges or []
        self.nodes: Set[ReplicationTopologyGateway] = set(k[0] for k in self.edges) | set(k[1] for k in self.edges)

    def add_edge(self, src_region: str, src_instance: int, dest_region: str, dest_instance: int, num_connections: int):
        """
        Adds an edge to the topology.
        """
        src_gateway = ReplicationTopologyGateway(src_region, src_instance)
        dest_gateway = ReplicationTopologyGateway(dest_region, dest_instance)
        self.edges.append((src_gateway, dest_gateway, int(num_connections)))
        self.nodes.add(src_gateway)
        self.nodes.add(dest_gateway)

    def get_outgoing_paths(self, src: ReplicationTopologyGateway):
        return {dest_gateway: num_connections for src_gateway, dest_gateway, num_connections in self.edges if src_gateway == src}

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
                (
                    ReplicationTopologyGateway.from_dict(edge["src"]),
                    ReplicationTopologyGateway.from_dict(edge["dest"]),
                    edge["num_connections"],
                )
            )
        return ReplicationTopology(edges)

    def to_graphviz(self):
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
            src_region, src_instance = src_gateway.region, src_gateway.instance_idx
            dest_region, dest_instance = dest_gateway.region, dest_gateway.instance_idx
            src_region, dest_region = src_region.replace(":", "/"), dest_region.replace(":", "/")
            src_node = f"{src_region}, {src_instance}"
            dest_node = f"{dest_region}, {dest_instance}"

            # make a subgraph for each region
            if src_region not in subgraphs:
                subgraphs[src_region] = gv.Digraph(name=f"cluster_{src_region}")
            if dest_region not in subgraphs:
                subgraphs[dest_region] = gv.Digraph(name=f"cluster_{dest_region}")

            # add nodes
            subgraphs[src_region].node(src_node, label=src_node, shape="box")
            subgraphs[dest_region].node(dest_node, label=dest_node, shape="box")

            # add edges
            g.edge(src_node, dest_node, label=f"{n_connections} connections")

        for subgraph in subgraphs.values():
            g.subgraph(subgraph)

        return g


@dataclass
class ReplicationJob:
    source_region: str
    source_bucket: str
    dest_region: str
    dest_bucket: str
    objs: List[str]

    # progress tracking via a list of chunk_requests
    chunk_requests: Optional[List[ChunkRequest]] = None

    # Generates random chunks for testing on the gateways
    random_chunk_size_mb: Optional[int] = None

    def src_obj_sizes(self):
        if self.source_region.split(":")[0] == "aws":
            interface = S3Interface(self.source_region.split(":")[1], self.source_bucket)
        if self.source_region.split(":")[0] == "gcp":
            interface = GCSInterface(self.source_region.split(":")[1][:-2], self.source_bucket)
        else:
            raise NotImplementedError
        get_size = lambda o: interface.get_obj_size(o)
        return do_parallel(get_size, self.objs, n=16, progress_bar=True, desc="Query object sizes")
