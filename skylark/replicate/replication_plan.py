from dataclasses import dataclass
from collections import namedtuple
import json
import shutil
from typing import Dict, List, Optional, Set, Tuple

from loguru import logger
import graphviz as gv

from skylark.obj_store.s3_interface import S3Interface
from skylark.obj_store.gcs_interface import GCSInterface
from skylark.utils.utils import do_parallel


@dataclass
class ReplicationTopologyGateway:
    region: str
    instance_idx: int


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
        src_gateway = (src_region, src_instance)
        dest_gateway = (dest_region, dest_instance)
        self.edges.append((src_gateway, dest_gateway, int(num_connections)))
        self.nodes.add(src_gateway)
        self.nodes.add(dest_gateway)

    def to_json(self):
        """
        Returns a JSON representation of the topology.
        """
        edges = []
        for e in self.edges:
            edges.append(
                {
                    "src": {"region": e[0][0], "instance": int(e[0][1])},
                    "dest": {"region": e[1][0], "instance": int(e[1][1])},
                    "num_connections": int(e[2]),
                }
            )
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
                    ReplicationTopologyGateway(edge["src"]["region"], edge["src"]["instance"]),
                    ReplicationTopologyGateway(edge["dest"]["region"], edge["dest"]["instance"]),
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
            src_region, src_instance = src_gateway
            dest_region, dest_instance = dest_gateway
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
