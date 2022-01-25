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


ReplicationTopologyGateway = namedtuple("ReplicationTopologyGateway", ["region", "instance_idx"])


class ReplicationTopology:
    """
    ReplicationTopology stores a DAG where nodes are an instance in a cloud region
    (e.g. "aws:us-east-1", instance 0) and edges denote a connection to another
    cloud region (e.g. ("aws:us-east-1", 0) -> ("aws:us-west-2", 1) with an
    associated number of connections (e.g. 64).
    """

    def __init__(
        self,
        topology: Optional[Dict[ReplicationTopologyGateway, List[ReplicationTopologyGateway]]] = None,
        num_connections: Optional[Dict[Tuple[ReplicationTopologyGateway, ReplicationTopologyGateway], int]] = None,
    ):
        self.topology: Dict[ReplicationTopologyGateway, List[ReplicationTopologyGateway]] = topology or {}
        self.num_connections: Dict[Tuple[ReplicationTopologyGateway, ReplicationTopologyGateway], int] = num_connections or {}
        self.nodes: Set[ReplicationTopologyGateway] = set(self.topology.keys()) | set(v for vlist in self.topology.values() for v in vlist)

    def add_edge(self, src_region: str, src_instance: int, dest_region: str, dest_instance: int, num_connections: int):
        """
        Adds an edge to the topology.
        """
        src_gateway = (src_region, src_instance)
        dest_gateway = (dest_region, dest_instance)
        self.topology[src_gateway] = self.topology.get(src_gateway, []) + [dest_gateway]
        self.num_connections[(src_gateway, dest_gateway)] = num_connections
        self.nodes.add(src_gateway)

    def get_edges(self, src_region: str, src_instance: int) -> Dict[ReplicationTopologyGateway, int]:
        """
        Returns a dict of all edges from the given instance.
        """
        src_gateway = (src_region, src_instance)
        return {d: self.num_connections[(src_gateway, d)] for d in self.topology.get(src_gateway, [])}

    def num_instances(self, region):
        return len([i for i in self.nodes if i.region == region])

    def to_json(self):
        """
        Returns a JSON representation of the topology.
        """
        out = {
            "topology": self.topology,
            "num_connections": self.num_connections,
        }
        return json.dumps(out)

    @classmethod
    def from_json(cls, json_str: str):
        """
        Returns a ReplicationTopology from a JSON string.
        """
        in_dict = json.loads(json_str)
        assert "topology" in in_dict and "num_connections" in in_dict, "Invalid JSON"
        return cls(in_dict["topology"], in_dict["num_connections"])

    def to_graphviz(self):
        # if dot is not installed
        has_dot = shutil.which("dot") is not None
        if not has_dot:
            logger.error("Graphviz is not installed. Please install it to plot the solution (sudo apt install graphviz).")
            return None

        g = gv.Digraph(name="throughput_graph")
        g.attr(rankdir="LR")
        subgraphs = {}
        for src_gateway, dest_gateways in self.topology.items():
            for dest_gateway in dest_gateways:
                # group node instances by region
                src_region, src_instance = src_gateway
                dest_region, dest_instance = dest_gateway
                src_region, dest_region = src_region.replace(":", "/"), dest_region.replace(":", "/")
                src_node = f"{src_region}x{src_instance}"
                dest_node = f"{dest_region}x{dest_instance}"

                # make a subgraph for each region
                if src_region not in subgraphs:
                    subgraphs[src_region] = gv.Digraph(name=src_region)
                    # subgraphs[src_region].attr(rankdir="LR")
                if dest_region not in subgraphs:
                    subgraphs[dest_region] = gv.Digraph(name=dest_region)
                    # subgraphs[dst_region].attr(rankdir="LR")

                # add nodes
                subgraphs[src_region].node(src_node, label=src_node)
                subgraphs[dest_region].node(dest_node, label=dest_node)

                # add edges
                g.edge(src_node, dest_node, label=str(self.num_connections[(src_gateway, dest_gateway)]))

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
