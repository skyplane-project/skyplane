from collections import defaultdict
import os
import csv
from skyplane.api.config import TransferConfig
from skyplane.utils import logger
from importlib.resources import path
from typing import Dict, List, Optional, Tuple
import numpy as np
import collections
from skyplane.planner.solver import ThroughputProblem, BroadcastProblem, \
    BroadcastSolution, GBIT_PER_GBYTE, ThroughputSolution, ThroughputSolver
from skyplane.planner.solver_ilp import ThroughputSolverILP
from skyplane import compute
from skyplane.planner.topology import TopologyPlan
from skyplane.gateway.gateway_program import (
    GatewayProgram,
    GatewayMuxOr,
    GatewayMuxAnd,
    GatewayReadObjectStore,
    GatewayWriteObjectStore,
    GatewayReceive,
    GatewaySend,
    GatewayCompress,
    GatewayDecompress,
    GatewayEncrypt,
    GatewayDecrypt
)
import networkx as nx
import math
from skyplane.api.transfer_job import TransferJob
import json
import functools

from skyplane.utils.fn import do_parallel
from skyplane.config_paths import config_path, azure_standardDv5_quota_path, aws_quota_path, gcp_quota_path
from skyplane.config import SkyplaneConfig
from pathlib import Path
from random import sample
import matplotlib.pyplot as plt


class Planner:
    def __init__(self, 
                 transfer_config: TransferConfig,
                 n_instances:int, 
                 n_connections: int, 
                 n_partitions: int = 1,
                 quota_limits_file: Optional[str] = None,
                 tp_grid_path: Optional[Path] = Path("skyplane.data", "throughput.csv")):
        self.transfer_config = transfer_config
        self.config = SkyplaneConfig.load_config(config_path)
        self.n_instances = n_instances
        self.n_connections = n_connections
        self.n_partitions = n_partitions
        self.solution_graph = None
        self.regions = None
        self.tp_grid_path = tp_grid_path

        # Loading the quota information, add ibm cloud when it is supported
        quota_limits = {}
        if quota_limits_file is not None:
            with open(quota_limits_file, "r") as f:
                quota_limits = json.load(f)
        else:
            if os.path.exists(aws_quota_path):
                with aws_quota_path.open("r") as f:
                    quota_limits["aws"] = json.load(f)
            if os.path.exists(azure_standardDv5_quota_path):
                with azure_standardDv5_quota_path.open("r") as f:
                    quota_limits["azure"] = json.load(f)
            if os.path.exists(gcp_quota_path):
                with gcp_quota_path.open("r") as f:
                    quota_limits["gcp"] = json.load(f)
        self.quota_limits = quota_limits

        # Loading the vcpu information - a dictionary of dictionaries
        # {"cloud_provider": {"instance_name": vcpu_cost}}
        self.vcpu_info = defaultdict(dict)
        with path("skyplane.data", "vcpu_info.csv") as file_path:
            with open(file_path, "r") as csvfile:
                reader = csv.reader(csvfile)
                next(reader)  # Skip the header row

                for row in reader:
                    instance_name, cloud_provider, vcpu_cost = row
                    vcpu_cost = int(vcpu_cost)
                    self.vcpu_info[cloud_provider][instance_name] = vcpu_cost

    def plan(self, jobs: List[TransferJob]) -> TopologyPlan:
        # create physical plan in TopologyPlan format
        raise NotImplementedError

    def _vm_to_vcpus(self, cloud_provider: str, vm: str) -> int:
        """Gets the vcpu_cost of the given vm instance (instance_name)

        :param cloud_provider: name of the cloud_provider
        :type cloud_provider: str
        :param instance_name: name of the vm instance
        :type instance_name: str
        """
        return self.vcpu_info[cloud_provider][vm]

    def _get_quota_limits_for(self, cloud_provider: str, region: str, spot: bool = False) -> Optional[int]:
        """Gets the quota info from the saved files. Returns None if quota_info isn't loaded during `skyplane init`
        or if the quota info doesn't include the region.

        :param cloud_provider: name of the cloud provider of the region
        :type cloud_provider: str
        :param region: name of the region for which to get the quota for
        :type region: int
        :param spot: whether to use spot specified by the user config (default: False)
        :type spot: bool
        """
        quota_limits = self.quota_limits.get(cloud_provider, None)
        if not quota_limits:
            # User needs to reinitialize to save the quota information
            return None
        if cloud_provider == "gcp":
            region_family = "-".join(region.split("-")[:2])
            if region_family in quota_limits:
                return quota_limits[region_family]
        elif cloud_provider == "azure":
            if region in quota_limits:
                return quota_limits[region]
        elif cloud_provider == "aws":
            for quota in quota_limits:
                if quota["region_name"] == region:
                    return quota["spot_standard_vcpus"] if spot else quota["on_demand_standard_vcpus"]
        return None

    def _calculate_vm_types(self, region_tag: str) -> Optional[Tuple[str, int]]:
        """Calculates the largest allowed vm type according to the regional quota limit as well as
        how many of these vm types can we launch to avoid QUOTA_EXCEEDED errors. Returns None if quota
        information wasn't properly loaded or allowed vcpu list is wrong.

        :param region_tag: tag of the node we are calculating the above for, example -> "aws:us-east-1"
        :type region_tag: str
        """
        cloud_provider, region = region_tag.split(":")

        # Get the quota limit
        quota_limit = self._get_quota_limits_for(
            cloud_provider=cloud_provider, region=region,
            spot=getattr(self.transfer_config, f"{cloud_provider}_use_spot_instances")
        )

        config_vm_type = getattr(self.transfer_config, f"{cloud_provider}_instance_class")

        # No quota limits (quota limits weren't initialized properly during skyplane init)
        if quota_limit is None:
            logger.warning(
                f"Quota limit file not found for {region_tag}. Try running `skyplane init --reinit-{cloud_provider}` to load the quota information"
            )
            # return default instance type and number of instances
            return config_vm_type, self.n_instances

        config_vcpus = self._vm_to_vcpus(cloud_provider, config_vm_type)
        if config_vcpus <= quota_limit:
            return config_vm_type, quota_limit // config_vcpus

        vm_type, vcpus = None, None
        for instance_name, vcpu_cost in sorted(self.vcpu_info[cloud_provider].items(), key=lambda x: x[1],
                                               reverse=True):
            if vcpu_cost <= quota_limit:
                vm_type, vcpus = instance_name, vcpu_cost
                break

        # shouldn't happen, but just in case we use more complicated vm types in the future
        assert vm_type is not None and vcpus is not None

        # number of instances allowed by the quota with the selected vm type
        n_instances = quota_limit // vcpus
        logger.warning(
            f"Falling back to instance class `{vm_type}` at {region_tag} "
            f"due to cloud vCPU limit of {quota_limit}. You can visit https://skyplane.org/en/latest/increase_vcpus.html "
            "to learn more about how to increase your cloud vCPU limits for any cloud provider."
        )
        return (vm_type, n_instances)

    def _get_vm_type_and_instances(
            self, src_region_tag: Optional[str] = None, dst_region_tags: Optional[List[str]] = None
    ) -> Tuple[Dict[str, str], int]:
        """Dynamically calculates the vm type each region can use (both the source region and all destination regions)
        based on their quota limits and calculates the number of vms to launch in all regions by conservatively
        taking the minimum of all regions to stay consistent.

        :param src_region_tag: the source region tag (default: None)
        :type src_region_tag: Optional[str]
        :param dst_region_tags: a list of the destination region tags (defualt: None)
        :type dst_region_tags: Optional[List[str]]
        """

        # One of them has to provided
        # assert src_region_tag is not None or dst_region_tags is not None, "There needs to be at least one source or destination"
        src_tags = [src_region_tag] if src_region_tag is not None else []
        dst_tags = dst_region_tags if dst_region_tags is not None else []

        if src_region_tag:
            assert len(src_region_tag.split(
                ":")) == 2, f"Source region tag {src_region_tag} must be in the form of `cloud_provider:region`"
        if dst_region_tags:
            assert (
                    len(dst_region_tags[0].split(":")) == 2
            ), f"Destination region tag {dst_region_tags} must be in the form of `cloud_provider:region`"

        # do_parallel returns tuples of (region_tag, (vm_type, n_instances))
        vm_info = do_parallel(self._calculate_vm_types, src_tags + dst_tags)
        # Specifies the vm_type for each region
        vm_types = {v[0]: v[1][0] for v in vm_info}  # type: ignore
        # Taking the minimum so that we can use the same number of instances for both source and destination
        n_instances = min([self.n_instances] + [v[1][1] for v in vm_info])  # type: ignore
        return vm_types, n_instances

    def verify_job_src_dsts(self, jobs: List[TransferJob], multicast=False) -> Tuple[str, List[str]]:
        src_region_tag = jobs[0].src_iface.region_tag()
        dst_region_tag = jobs[0].dst_ifaces[0].region_tag()

        assert len(src_region_tag.split(
            ":")) == 2, f"Source region tag {src_region_tag} must be in the form of `cloud_provider:region`"
        assert (
                len(dst_region_tag.split(":")) == 2
        ), f"Destination region tag {dst_region_tag} must be in the form of `cloud_provider:region`"

        # jobs must have same sources and destinations
        for job in jobs[1:]:
            assert job.src_iface.region_tag() == src_region_tag, "All jobs must have same source region"
            assert job.dst_ifaces[0].region_tag() == dst_region_tag, "All jobs must have same destination region"

        plan = TopologyPlan(src_region_tag=src_region_tag, dest_region_tags=[dst_region_tag])

        # Dynammically calculate n_instances based on quota limits
        vm_types, n_instances = self._get_vm_type_and_instances(src_region_tag=src_region_tag,
                                                                dst_region_tags=[dst_region_tag])

        # TODO: support on-sided transfers but not requiring VMs to be created in source/destination regions
        for i in range(n_instances):
            plan.add_gateway(src_region_tag, vm_types[src_region_tag])
            plan.add_gateway(dst_region_tag, vm_types[dst_region_tag])

        if multicast:
            # multicast checking
            dst_region_tags = [iface.region_tag() for iface in jobs[0].dst_ifaces]

            # jobs must have same sources and destinations
            for job in jobs[1:]:
                assert job.src_iface.region_tag() == src_region_tag, "All jobs must have same source region"
                assert [iface.region_tag() for iface in
                        job.dst_ifaces] == dst_region_tags, "Add jobs must have same destination set"
        else:
            # unicast checking
            for job in jobs:
                assert len(
                    job.dst_ifaces) == 1, f"DirectPlanner only support single destination jobs, got {len(job.dst_ifaces)}"

            # jobs must have same sources and destinations
            dst_region_tag = jobs[0].dst_ifaces[0].region_tag()
            for job in jobs[1:]:
                assert job.src_iface.region_tag() == src_region_tag, "All jobs must have same source region"
                assert job.dst_ifaces[0].region_tag() == dst_region_tag, "All jobs must have same destination region"
            dst_region_tags = [dst_region_tag]

        return src_region_tag, dst_region_tags

    @functools.lru_cache(maxsize=None)
    def make_nx_graph(self) -> nx.DiGraph:
        # create throughput / cost graph for all regions for planner
        G = nx.DiGraph()
        dataSolver = ThroughputSolver(self.tp_grid_path)
        self.regions = dataSolver.get_regions()
        for i, src in enumerate(self.regions):
            for j, dst in enumerate(self.regions):
                if i != j:
                    throughput_grid = dataSolver.get_path_throughput(src, dst) if dataSolver.get_path_throughput(src,dst) is not None else 0
                    G.add_edge(self.regions[i], self.regions[j], cost=None, throughput=throughput_grid / 1e9)

        for edge in G.edges.data():
            if edge[-1]["cost"] is None:
                edge[-1]["cost"] = dataSolver.get_path_cost(edge[0], edge[1])

        assert all([edge[-1]["cost"] is not None for edge in G.edges.data()])
        return G

    def add_src_operator(self,
            solution_graph: nx.DiGraph,
            gateway_program: GatewayProgram,
            region: str,
            partition_ids: List[int],
            plan: TopologyPlan,
            bucket_info: Optional[Tuple[str, str]] = None,
            dst_op: Optional[GatewayReceive] = None,
    ) -> bool:
        return self.add_src_or_overlay_operator( solution_graph, gateway_program, region, partition_ids, plan, bucket_info, dst_op, is_src=True)

    def add_overlay_operator(self,
            solution_graph: nx.DiGraph,
            gateway_program: GatewayProgram,
            region: str,
            partition_ids: List[int],
            plan: TopologyPlan,
            bucket_info: Optional[Tuple[str, str]] = None,
            dst_op: Optional[GatewayReceive] = None,
    ) -> bool:
        return self.add_src_or_overlay_operator( solution_graph, gateway_program, region, partition_ids, plan, bucket_info, dst_op, is_src=False)


    def add_src_or_overlay_operator(
            self,
            solution_graph: nx.DiGraph,
            gateway_program: GatewayProgram,
            region: str,
            partition_ids: List[int],
            plan: TopologyPlan,
            bucket_info: Optional[Tuple[str, str]] = None,
            dst_op: Optional[GatewayReceive] = None,
            is_src: bool = False
    ) -> bool:
        """
        :param solution_graph: nx.DiGraph of solution
        :param gateway_program: GatewayProgram of region to add operator to
        :param region: region to add operator to
        :param partition_ids: list of partition ids to add operator to
        :param partition_offset: offset of partition ids
        :param plan: TopologyPlan of solution [for getting gateway ids]
        :param bucket_info: tuple of (bucket_name, bucket_region) for object store
        :param dst_op: if None, then this is either the source node or a overlay node; otherwise, this is the destination overlay node
        """
        # partition_ids are set of ids that follow the same path from the out edges of the region
        # any_id = partition_ids[0] - partition_offset
        any_id = partition_ids[0]
        # g = self.make_nx_graph()
        next_regions = set(
            [edge[1] for edge in solution_graph.out_edges(region, data=True) if str(any_id) in edge[-1]["partitions"]])

        # if partition_ids does not have a next region, then we cannot add an operator
        if len(next_regions) == 0:
            print(
                f"Region {region}, any id: {any_id}, partition ids: {partition_ids}, has no next region to forward data to: {solution_graph.out_edges(region, data=True)}"
            )
            return False

        # identify if this is a destination overlay node or not
        if dst_op is None:
            # source node or overlay node
            # TODO: add generate data locally operator
            if bucket_info is None:
                receive_op = GatewayReceive()
            else:
                receive_op = GatewayReadObjectStore(
                    bucket_name=bucket_info[0], bucket_region=bucket_info[1], num_connections=self.n_connections
                )
        else:
            # destination overlay node, dst_op is the parent node
            receive_op = dst_op

        # find set of regions to send to for all partitions in partition_ids
        region_to_id_map = {}
        for next_region in next_regions:
            region_to_id_map[next_region] = []
            for i in range(solution_graph.nodes[next_region]["num_vms"]):
                region_to_id_map[next_region].append(plan.get_region_gateways(next_region)[i].gateway_id)

        # use muxand or muxor for partition_id
        operation = "MUX_AND" if len(next_regions) > 1 else "MUX_OR"
        mux_op = GatewayMuxAnd() if len(next_regions) > 1 else GatewayMuxOr()

        # non-dst node: add receive_op into gateway program
        if dst_op is None:
            gateway_program.add_operator(op=receive_op, partition_id=tuple(partition_ids))
            
        # MUX_AND: send this partition to multiple regions
        if operation == "MUX_AND":
            if is_src and self.transfer_config.use_e2ee:
                e2ee_key_file = "e2ee_key"
                with open(f"/tmp/{e2ee_key_file}", 'rb') as f:
                    e2ee_key_bytes = f.read()
            
            if dst_op is not None and dst_op.op_type == "mux_and":
                mux_op = receive_op
            else:  # do not add any nested mux_and if dst_op parent is mux_and
                gateway_program.add_operator(op=mux_op, parent_handle=receive_op.handle,
                                             partition_id=tuple(partition_ids))

            for next_region, next_region_ids in region_to_id_map.items():
                send_ops = [
                    GatewaySend(target_gateway_id=id, region=next_region, num_connections=self.n_connections) for id in
                    next_region_ids
                ]

                # if there is more than one region to forward data to, add MUX_OR
                if len(next_region_ids) > 1:
                    mux_or_op = GatewayMuxOr()
                    gateway_program.add_operator(op=mux_or_op, parent_handle=mux_op.handle,
                                                 partition_id=tuple(partition_ids))
                    if is_src:
                        for id in next_region_ids:
                            compress_op = GatewayCompress(compress=self.transfer_config.use_compression)
                            gateway_program.add_operator(op=compress_op, parent_handle=mux_or_op.handle,
                                                 partition_id=tuple(partition_ids))
                            encrypt_op = GatewayEncrypt(encrypt=self.transfer_config.use_e2ee, e2ee_key_bytes=e2ee_key_bytes)
                            gateway_program.add_operator(op=encrypt_op, parent_handle=encrypt_op.handle,
                                                 partition_id=tuple(partition_ids))
                            send_op = GatewaySend(target_gateway_id=id, region=next_region, num_connections=self.n_connections)
                            gateway_program.add_operator(ops=send_op, parent_handle=encrypt_op.handle,
                                                        partition_id=tuple(partition_ids))
                else:
                    # otherwise, the parent of send_op is mux_op ("MUX_AND")
                    assert len(send_ops) == 1
                    if is_src:
                        compress_op = GatewayCompress(compress=self.transfer_config.use_compression)
                        gateway_program.add_operator(op=compress_op, parent_handle=mux_op.handle,
                                                partition_id=tuple(partition_ids))
                        encrypt_op = GatewayEncrypt(encrypt=self.transfer_config.use_e2ee, e2ee_key_bytes=e2ee_key_bytes)
                        gateway_program.add_operator(op=encrypt_op, parent_handle=compress_op.handle,
                                                partition_id=tuple(partition_ids))

                    gateway_program.add_operator(op=send_ops[0], parent_handle=encrypt_op.handle if is_src else mux_op.handle,
                                                 partition_id=tuple(partition_ids))
        else:
            # only send this partition to a single region
            assert len(region_to_id_map) == 1
            next_region = list(region_to_id_map.keys())[0]
            ids = [id for next_region_ids in region_to_id_map.values() for id in next_region_ids]
            send_ops = [GatewaySend(target_gateway_id=id, region=next_region, num_connections=self.n_connections) for id
                        in
                        ids]

            # if num of gateways > 1, then connect to MUX_OR
            if len(ids) > 1:
                gateway_program.add_operator(op=mux_op, parent_handle=receive_op.handle,
                                             partition_id=tuple(partition_ids))
                gateway_program.add_operators(ops=send_ops, parent_handle=mux_op.handle)
            else:
                gateway_program.add_operators(ops=send_ops, parent_handle=receive_op.handle,
                                              partition_id=tuple(partition_ids))

        return True

    def add_dst_operator(
            self,
            solution_graph,
            gateway_program: GatewayProgram,
            region: str,
            partition_ids: List[int],
            partition_offset: int,
            plan: TopologyPlan,
            obj_store: Tuple[str, str] = None,
    ):
        # operator that receives data
        receive_op = GatewayReceive()
        gateway_program.add_operator(receive_op, partition_id=tuple(partition_ids))

        # operator that writes to the object store
        write_op = GatewayWriteObjectStore(bucket_name=obj_store[0], bucket_region=obj_store[1],
                                           num_connections=self.n_connections)

        g = solution_graph

        # partition_ids are ids that follow the same path from the out edges of the region     
        # any_id = partition_ids[0] - partition_offset
        any_id = partition_ids[0]
        next_regions = set(
            [edge[1] for edge in g.out_edges(region, data=True) if str(any_id) in edge[-1]["partitions"]])

        # if no regions to forward data to, write to the object store
        if len(next_regions) == 0:
            if self.transfer_config.use_e2ee:
                e2ee_key_file = "e2ee_key"
                with open(f"/tmp/{e2ee_key_file}", 'rb') as f:
                    e2ee_key_bytes = f.read()
                
            decrypt_op = GatewayDecrypt(decrypt=self.transfer_config.use_e2ee, e2ee_key_bytes=e2ee_key_bytes)
            gateway_program.add_operator(op=decrypt_op, parent_handle=receive_op.handle,
                                    partition_id=tuple(partition_ids))
            decompress_op = GatewayDecompress(compress=self.transfer_config.use_compression)
            gateway_program.add_operator(op=decompress_op, parent_handle=decrypt_op.handle,
                                    partition_id=tuple(partition_ids))
            
            gateway_program.add_operator(write_op, parent_handle=decompress_op.handle, partition_id=tuple(partition_ids))

        # otherwise, receive and write to the object store, then forward data to next regions
        else:
            mux_and_op = GatewayMuxAnd()
            # receive and write
            gateway_program.add_operator(mux_and_op, parent_handle=receive_op.handle, partition_id=tuple(partition_ids))
            gateway_program.add_operator(write_op, parent_handle=mux_and_op.handle, partition_id=tuple(partition_ids))

            # forward: destination nodes are also forwarders
            self.add_src_or_overlay_operator(
                solution_graph, gateway_program, region, partition_ids, partition_offset, plan, dst_op=mux_and_op, is_src=False
            )

    def logical_plan_to_topology_plan(self, jobs: List[TransferJob], solution_graph: nx.graph) -> TopologyPlan:
        """
        Given a logical plan, construct a gateway program for each region in the logical plan for the given jobs.
        """
        # get source and destination regions
        src_ifaces, dst_ifaces = [job.src_iface for job in jobs], [job.dst_ifaces for job in jobs]
        src_region_tag = src_ifaces[0].region_tag()
        dst_region_tags = [dst_iface.region_tag() for dst_iface in dst_ifaces[0]]

        # map from the node to the gateway program
        region_to_gateway_program = {region: GatewayProgram() for region in solution_graph.nodes}

        # construct TopologyPlan for all the regions in solution_graph
        overlay_region_tags = [node for node in solution_graph.nodes if
                               node != src_region_tag and node not in dst_region_tags]
        plan = TopologyPlan(src_region_tag=src_region_tag, dest_region_tags=dst_region_tags,
                            overlay_region_tags=overlay_region_tags)

        for node in solution_graph.nodes:
            for i in range(solution_graph.nodes[node]["num_vms"]):
                plan.add_gateway(node)

        # iterate through all the jobs
        for i in range(len(src_ifaces)):
            src_bucket = src_ifaces[i].bucket()
            dst_buckets = {dst_iface[i].region_tag(): dst_iface[i].bucket() for dst_iface in dst_ifaces}

            # iterate through all the regions in the solution graph
            for node in solution_graph.nodes:
                node_gateway_program = region_to_gateway_program[node]
                partition_to_next_regions = {}

                # give each job a different partition offset i, so we can read/write to different buckets
                partition_to_next_regions[jobs[i].uuid] = set(
                        [edge[1] for edge in solution_graph.out_edges(node, data=True)
                         if str(jobs[i].uuid) in edge[-1]["partitions"]])

                keys_per_set = collections.defaultdict(list)
                for key, value in partition_to_next_regions.items():
                    keys_per_set[frozenset(value)].append(key)

                list_of_partitions = list(keys_per_set.values())

                # source node: read from object store or generate random data, then forward data
                for partitions in list_of_partitions:
                    if node == src_region_tag:
                        self.add_src_operator(
                            solution_graph,
                            node_gateway_program,
                            node,
                            partitions,
                            partition_offset=i,
                            plan=plan,
                            bucket_info=(src_bucket, src_region_tag)
                        )

                    # dst receive data, write to object store, forward data if needed
                    elif node in dst_region_tags:
                        dst_bucket = dst_buckets[node]
                        self.add_dst_operator(
                            solution_graph,
                            node_gateway_program,
                            node,
                            partitions,
                            partition_offset=i,
                            plan=plan,
                            obj_store=(dst_bucket, node),
                        )

                    # overlay node only forward data
                    else:
                        self.add_overlay_operator(
                            solution_graph, node_gateway_program, node, partitions, partition_offset=i, plan=plan
                        )
            region_to_gateway_program[node] = node_gateway_program
            assert len(region_to_gateway_program) > 0, f"Empty gateway program {node}"

        for node in solution_graph.nodes:
            plan.set_gateway_program(node, region_to_gateway_program[node])

        for edge in solution_graph.edges.data():
            src_region, dst_region = edge[0], edge[1]
            plan.cost_per_gb += compute.CloudProvider.get_transfer_cost(src_region, dst_region) * (
                    len(edge[-1]["partitions"]) / self.n_partitions)

        return plan

    def draw_nxgrapg(self, save_fig=True):
        if self.solution_graph is None:
            raise Exception('please run plan method first to get nx_graph')
        nx.draw(self.solution_graph, with_labels=True)
        if save_fig:
            os.makedirs('graph', exist_ok=True)
            plt.savefig(f'graph/figure.png')
        else:
            plt.show()

    def draw_di_graph(self, di_graph, save_name = "test.png"):
        # Specify the edges you want here
        pos = nx.spring_layout(di_graph)
        nx.draw_networkx_nodes(di_graph, pos, node_size = 50)
        nx.draw_networkx_labels(di_graph, pos, font_size = 8)
        nx.draw_networkx_edges(di_graph, pos, edgelist=di_graph.edges(), edge_color='blue', arrows=True)

        plt.savefig(save_name)


class MulticastDirectPlanner(Planner):
    def __init__(self, transfer_config: TransferConfig,
                 n_instances: int,
                 n_connections: int,
                 n_partitions: int,
                 quota_limits_file: Optional[str] = None):
        super().__init__(transfer_config, n_instances, n_connections, n_partitions, quota_limits_file)

    def plan(self, jobs: List[TransferJob]) -> TopologyPlan:
        src_region_tag = jobs[0].src_iface.region_tag()
        dst_region_tags = [iface.region_tag() for iface in jobs[0].dst_ifaces]

        # jobs must have same sources and destinations
        for job in jobs[1:]:
            assert job.src_iface.region_tag() == src_region_tag, "All jobs must have same source region"
            assert [iface.region_tag() for iface in
                    job.dst_ifaces] == dst_region_tags, "Add jobs must have same destination set"

        plan = TopologyPlan(src_region_tag=src_region_tag, dest_region_tags=dst_region_tags)

        # Dynammically calculate n_instances based on quota limits
        if src_region_tag.split(":")[0] == "test":
            vm_types = None
            n_instances = self.n_instances
        else:
            vm_types, n_instances = self._get_vm_type_and_instances(src_region_tag=src_region_tag,
                                                                    dst_region_tags=dst_region_tags)

        # TODO: support on-sided transfers but not requiring VMs to be created in source/destination regions
        for i in range(n_instances):
            plan.add_gateway(src_region_tag, vm_types[src_region_tag] if vm_types else None)
            for dst_region_tag in dst_region_tags:
                plan.add_gateway(dst_region_tag, vm_types[dst_region_tag] if vm_types else None)

        # initialize gateway programs per region
        dst_program = {dst_region: GatewayProgram() for dst_region in dst_region_tags}
        src_program = GatewayProgram()

        # iterate through all jobs
        for job in jobs:
            src_bucket = job.src_iface.bucket()
            src_region_tag = job.src_iface.region_tag()
            src_provider = src_region_tag.split(":")[0]

            # give each job a different partition id, so we can read/write to different buckets
            partition_id = job.uuid

            # source region gateway program
            obj_store_read = src_program.add_operator(
                GatewayReadObjectStore(src_bucket, src_region_tag, self.n_connections), partition_id=(partition_id,))

            # send to all destination
            mux_and = src_program.add_operator(GatewayMuxAnd(), parent_handle=obj_store_read,
                                               partition_id=(partition_id,))
            dst_prefixes = job.dst_prefixes
            for i in range(len(job.dst_ifaces)):
                dst_iface = job.dst_ifaces[i]
                dst_prefix = dst_prefixes[i]
                dst_region_tag = dst_iface.region_tag()
                dst_bucket = dst_iface.bucket()
                dst_gateways = plan.get_region_gateways(dst_region_tag)

                # special case where destination is same region as source
                if dst_region_tag == src_region_tag:
                    src_program.add_operator(
                        GatewayWriteObjectStore(dst_bucket, dst_region_tag, self.n_connections, key_prefix=dst_prefix),
                        parent_handle=mux_and,
                        partition_id=(partition_id,)
                    )
                    continue

                # can send to any gateway in region
                mux_or = src_program.add_operator(GatewayMuxOr(), parent_handle=mux_and, partition_id=(partition_id,))
                for i in range(n_instances):
                    private_ip = False
                    # TODO pcnl openstack|tencent|hw|ali|cloudflare|ibm|gcp|azure|aws
                    if dst_gateways[i].provider == src_provider and src_provider in {"gcp", "openstack"}:
                        # print("Using private IP for GCP to GCP transfer", src_region_tag, dst_region_tag)
                        private_ip = True
                    src_program.add_operator(
                        GatewaySend(
                            target_gateway_id=dst_gateways[i].gateway_id,
                            region=dst_region_tag,
                            num_connections=int(self.n_connections / len(dst_gateways)),
                            private_ip=private_ip,
                            compress=self.transfer_config.use_compression,
                            encrypt=self.transfer_config.use_e2ee,
                        ),
                        parent_handle=mux_or,
                        partition_id=(partition_id,),
                    )

                # each gateway also recieves data from source
                recv_op = dst_program[dst_region_tag].add_operator(
                    GatewayReceive(decompress=self.transfer_config.use_compression, decrypt=self.transfer_config.use_e2ee),
                    partition_id=partition_id,
                )
                dst_program[dst_region_tag].add_operator(
                    GatewayWriteObjectStore(dst_bucket, dst_region_tag, self.n_connections, key_prefix=dst_prefix),
                    parent_handle=recv_op,
                    partition_id=(partition_id,),
                )

                # update cost per GB
                plan.cost_per_gb += compute.CloudProvider.get_transfer_cost(src_region_tag, dst_region_tag)

        # set gateway programs
        plan.set_gateway_program(src_region_tag, src_program)
        for dst_region_tag, program in dst_program.items():
            if dst_region_tag != src_region_tag:  # don't overwrite
                plan.set_gateway_program(dst_region_tag, program)
        return plan


class DirectPlannerSourceOneSided(MulticastDirectPlanner):
    """Planner that only creates VMs in the source region"""

    def plan(self, jobs: List[TransferJob]) -> TopologyPlan:
        src_region_tag = jobs[0].src_iface.region_tag()
        dst_region_tags = [iface.region_tag() for iface in jobs[0].dst_ifaces]
        # jobs must have same sources and destinations
        for job in jobs[1:]:
            assert job.src_iface.region_tag() == src_region_tag, "All jobs must have same source region"
            assert [iface.region_tag() for iface in
                    job.dst_ifaces] == dst_region_tags, "Add jobs must have same destination set"

        plan = TopologyPlan(src_region_tag=src_region_tag, dest_region_tags=dst_region_tags)

        # Dynammically calculate n_instances based on quota limits
        vm_types, n_instances = self._get_vm_type_and_instances(src_region_tag=src_region_tag)

        # TODO: support on-sided transfers but not requiring VMs to be created in source/destination regions
        for i in range(n_instances):
            plan.add_gateway(src_region_tag, vm_types[src_region_tag])

        # initialize gateway programs per region
        src_program = GatewayProgram()

        # iterate through all jobs
        for job in jobs:
            src_bucket = job.src_iface.bucket()
            src_region_tag = job.src_iface.region_tag()
            src_provider = src_region_tag.split(":")[0]

            # give each job a different partition id, so we can read/write to different buckets
            partition_id = job.uuid

            # source region gateway program
            obj_store_read = src_program.add_operator(
                GatewayReadObjectStore(src_bucket, src_region_tag, self.n_connections), partition_id=(partition_id,)
            )
            # send to all destination
            mux_and = src_program.add_operator(GatewayMuxAnd(), parent_handle=obj_store_read,
                                               partition_id=(partition_id,))
            dst_prefixes = job.dst_prefixes
            for i in range(len(job.dst_ifaces)):
                dst_iface = job.dst_ifaces[i]
                dst_prefix = dst_prefixes[i]
                dst_region_tag = dst_iface.region_tag()
                dst_bucket = dst_iface.bucket()
                dst_gateways = plan.get_region_gateways(dst_region_tag)

                # special case where destination is same region as source
                src_program.add_operator(
                    GatewayWriteObjectStore(dst_bucket, dst_region_tag, self.n_connections, key_prefix=dst_prefix),
                    parent_handle=mux_and,
                    partition_id=(partition_id,),
                )
                # update cost per GB
                plan.cost_per_gb += compute.CloudProvider.get_transfer_cost(src_region_tag, dst_region_tag)

        # set gateway programs
        plan.set_gateway_program(src_region_tag, src_program)
        return plan


class DirectPlannerDestOneSided(MulticastDirectPlanner):
    """Planner that only creates instances in the destination region"""

    def plan(self, jobs: List[TransferJob]) -> TopologyPlan:
        # only create in destination region
        src_region_tag = jobs[0].src_iface.region_tag()
        dst_region_tags = [iface.region_tag() for iface in jobs[0].dst_ifaces]
        # jobs must have same sources and destinations
        for job in jobs[1:]:
            assert job.src_iface.region_tag() == src_region_tag, "All jobs must have same source region"
            assert [iface.region_tag() for iface in
                    job.dst_ifaces] == dst_region_tags, "Add jobs must have same destination set"

        plan = TopologyPlan(src_region_tag=src_region_tag, dest_region_tags=dst_region_tags)
        # TODO: use VM limits to determine how many instances to create in each region
        # TODO: support on-sided transfers but not requiring VMs to be created in source/destination regions
        for i in range(self.n_instances):
            for dst_region_tag in dst_region_tags:
                plan.add_gateway(dst_region_tag)

        # initialize gateway programs per region
        dst_program = {dst_region: GatewayProgram() for dst_region in dst_region_tags}

        # iterate through all jobs
        for job in jobs:
            src_bucket = job.src_iface.bucket()
            src_region_tag = job.src_iface.region_tag()
            src_provider = src_region_tag.split(":")[0]

            partition_id = jobs.index(job)

            # send to all destination
            dst_prefixes = job.dst_prefixes
            for i in range(len(job.dst_ifaces)):
                dst_iface = job.dst_ifaces[i]
                dst_prefix = dst_prefixes[i]
                dst_region_tag = dst_iface.region_tag()
                dst_bucket = dst_iface.bucket()
                dst_gateways = plan.get_region_gateways(dst_region_tag)

                # source region gateway program
                obj_store_read = dst_program[dst_region_tag].add_operator(
                    GatewayReadObjectStore(src_bucket, src_region_tag, self.n_connections), partition_id=partition_id
                )

                dst_program[dst_region_tag].add_operator(
                    GatewayWriteObjectStore(dst_bucket, dst_region_tag, self.n_connections, key_prefix=dst_prefix),
                    parent_handle=obj_store_read,
                    partition_id=partition_id,
                )

                # update cost per GB
                plan.cost_per_gb += compute.CloudProvider.get_transfer_cost(src_region_tag, dst_region_tag)

        # set gateway programs
        for dst_region_tag, program in dst_program.items():
            plan.set_gateway_program(dst_region_tag, program)
        return plan


class MulticastILPPlanner(Planner):
    def __init__(
            self,
            transfer_config: TransferConfig,
            n_instances: int,
            n_connections: int,
            n_partitions: int,
            target_time,
            gbyte_to_transfer: int,
            filter_node: bool = False,
            filter_edge: bool = False,
            solver_verbose: bool = False,
            save_lp_path: Optional[str] = None,
    ):
        super().__init__(transfer_config, n_connections, n_partitions)
        self.target_time = target_time
        self.G = super().make_nx_graph()
        self.gbyte_to_transfer = gbyte_to_transfer
        self.filter_node = filter_node
        self.filter_edge = filter_edge
        self.solver_verbose = solver_verbose
        self.save_lp_path = save_lp_path
        self.n_instances = n_instances

    def multicast_solution_to_nxgraph(self, solution: BroadcastSolution) -> nx.DiGraph:
        """
        Convert ILP solution to logical plan in nx graph
        """
        v_result = solution.var_instances_per_region
        result = np.array(solution.var_edge_partitions)
        result_g = nx.DiGraph()  # solution nx graph
        for i in range(result.shape[0]):
            edge = solution.var_edges[i]
            partitions = [str(partition_i) for partition_i in range(result.shape[1]) if result[i][partition_i] > 0.5]

            if len(partitions) == 0:
                continue

            src_node, dst_node = edge[0], edge[1]
            result_g.add_edge(
                src_node,
                dst_node,
                partitions=partitions,
                throughput=self.G[src_node][dst_node]["throughput"],
                cost=self.G[src_node][dst_node]["cost"],
            )

        for i in range(len(v_result)):
            num_vms = int(v_result[i])
            node = solution.var_nodes[i]
            if node in result_g.nodes:
                result_g.nodes[node]["num_vms"] = num_vms

        return result_g

    def logical_plan(
            self,
            src_region: str,
            dst_regions: List[str],
    ) -> nx.DiGraph:
        import cvxpy as cp
        solver = cp.CBC

        problem = BroadcastProblem(
            src=src_region,
            dsts=dst_regions,
            gbyte_to_transfer=self.gbyte_to_transfer,
            instance_limit=self.n_instances,
            num_partitions=self.n_partitions,
            required_time_budget=self.target_time,
        )

        g = self.G

        # node-approximation
        if self.filter_node:
            src_dst_li = [problem.src] + problem.dsts
            sampled = [i for i in sample(list(self.G.nodes), 15) if i not in src_dst_li]
            g = g.subgraph(src_dst_li + sampled).copy()
            print(f"Filter node (only use): {src_dst_li + sampled}")

        cost = np.array([e[2] for e in g.edges(data="cost")])
        tp = np.array([e[2] for e in g.edges(data="throughput")])

        edges = list(g.edges)
        nodes = list(g.nodes)
        num_edges, num_nodes = len(edges), len(nodes)
        num_dest = len(problem.dsts)
        print(f"Num edges: {num_edges}, num nodes: {num_nodes}, num dest: {num_dest}, runtime budget: {problem.required_time_budget}s")

        partition_size_gb = problem.gbyte_to_transfer / problem.num_partitions
        partition_size_gbit = partition_size_gb * GBIT_PER_GBYTE
        print("Partition size (gbit): ", partition_size_gbit)

        # define variables
        p = cp.Variable((num_edges, problem.num_partitions), boolean=True)  # whether edge is carrying partition
        n = cp.Variable((num_nodes), boolean=True)  # whether node transfers partition
        f = cp.Variable((num_nodes * problem.num_partitions, num_nodes + 1), integer=True)  # enforce flow conservation
        v = cp.Variable((num_nodes), integer=True)  # number of VMs per region

        # define objective
        egress_cost = cp.sum(cost @ p) * partition_size_gb
        instance_cost = cp.sum(v) * (problem.cost_per_instance_hr / 3600) * problem.required_time_budget
        tot_cost = egress_cost + instance_cost
        obj = cp.Minimize(tot_cost)

        # define constants
        constraints = []

        # constraints on VM per region
        for i in range(num_nodes):
            constraints.append(v[i] <= problem.instance_limit)
            constraints.append(v[i] >= 0)

        # constraints to enforce flow between source/dest nodes
        for c in range(problem.num_partitions):
            for i in range(num_nodes):
                for j in range(num_nodes + 1):
                    if i != j:
                        if j != num_nodes:
                            edge = (nodes[i], nodes[j])

                            constraints.append(f[c * num_nodes + i][j] <= p[edges.index(edge)][c] * num_dest)
                            # p = 0 -> f <= 0
                            # p = 1 -> f <= num_dest
                            constraints.append(
                                f[c * num_nodes + i][j] >= (p[edges.index(edge)][c] - 1) * (num_dest + 1) + 1)
                            # p = 0 -> f >= -(num_dest)
                            # p = 1 -> f >= 1

                            constraints.append(f[c * num_nodes + i][j] == -f[c * num_nodes + j][i])

                        # capacity constraint for special node
                        else:
                            if nodes[i] in problem.dsts:  # only connected to destination nodes
                                constraints.append(f[c * num_nodes + i][j] <= 1)
                            else:
                                constraints.append(f[c * num_nodes + i][j] <= 0)
                    else:
                        constraints.append(f[c * num_nodes + i][i] == 0)

                # flow conservation
                if nodes[i] != problem.src and i != num_nodes + 1:
                    constraints.append(cp.sum(f[c * num_nodes + i]) == 0)

            # source must have outgoing flow
            constraints.append(cp.sum(f[c * num_nodes + nodes.index(problem.src), :]) == num_dest)

            # special node (connected to all destinations) must recieve all flow
            constraints.append(cp.sum(f[c * num_nodes: (c + 1) * num_nodes, -1]) == num_dest)

        # node contained if edge is contained
        for edge in edges:
            constraints.append(n[nodes.index(edge[0])] >= cp.max(p[edges.index(edge)]))
            constraints.append(n[nodes.index(edge[1])] >= cp.max(p[edges.index(edge)]))

        # edge approximation
        if self.filter_edge:
            for edge in edges:
                if edge[0] != problem.src and edge[1] not in problem.dsts:
                    # cannot be in graph
                    constraints.append(p[edges.index(edge)] == 0)

        # throughput constraint
        for edge_i in range(num_edges):
            node_i = nodes.index(edge[0])
            constraints.append(
                cp.sum(p[edge_i] * partition_size_gbit) <= problem.required_time_budget * tp[edge_i] * v[node_i])

        # instance limits
        for node in nodes:
            region = node.split(":")[0]
            if region == "aws":
                ingress_limit_gbps, egress_limit_gbps = problem.aws_instance_throughput_limit
            elif region == "gcp":
                ingress_limit_gbps, egress_limit_gbps = problem.gcp_instance_throughput_limit
            elif region == "azure":
                ingress_limit_gbps, egress_limit_gbps = problem.azure_instance_throughput_limit
            elif region == "cloudflare":  # TODO: not supported yet in the tput / cost graph
                ingress_limit_gbps, egress_limit_gbps = 1, 1
            else:
                raise Exception('region limit gbps is not given')

            node_i = nodes.index(node)
            # egress
            i = np.zeros(num_edges)
            for e in g.edges:
                if e[0] == node:  # edge goes to dest
                    i[edges.index(e)] = 1

            constraints.append(
                cp.sum(i @ p) * partition_size_gbit <= problem.required_time_budget * egress_limit_gbps * v[node_i])

            # ingress
            i = np.zeros(num_edges)
            for e in g.edges:
                # edge goes to dest
                if e[1] == node:
                    i[edges.index(e)] = 1
            constraints.append(
                cp.sum(i @ p) * partition_size_gbit <= problem.required_time_budget * ingress_limit_gbps * v[node_i])

        print("Define problem done.")

        # solve
        prob = cp.Problem(obj, constraints)
        if solver == cp.GUROBI or solver == "gurobi":
            solver_options = {}
            solver_options["Threads"] = 1
            if self.save_lp_path:
                solver_options["ResultFile"] = str(self.save_lp_path)
            if not self.solver_verbose:
                solver_options["OutputFlag"] = 0
            cost = prob.solve(verbose=self.solver_verbose, qcp=True, solver=cp.GUROBI, reoptimize=True,
                              **solver_options)
        elif solver == cp.CBC or solver == "cbc":
            solver_options = {}
            solver_options["maximumSeconds"] = 60
            solver_options["numberThreads"] = 1
            cost = prob.solve(verbose=self.solver_verbose, solver=cp.CBC, **solver_options)
        else:
            cost = prob.solve(solver=solver, verbose=self.solver_verbose)

        if prob.status == "optimal":
            solution = BroadcastSolution(
                problem=problem,
                is_feasible=True,
                var_edges=edges,
                var_nodes=nodes,
                var_edge_partitions=p.value,
                var_node_transfer_partitions=n.value,
                var_instances_per_region=v.value,
                var_flow=f.value,
                cost_egress=egress_cost.value,
                cost_instance=instance_cost.value,
                cost_total=tot_cost.value,
            )
        else:
            solution = BroadcastSolution(problem=problem, is_feasible=False, extra_data=dict(status=prob.status))

        if not solution.is_feasible:
            raise Exception('no feasible solution found')

        return self.multicast_solution_to_nxgraph(solution)

    def plan(self, jobs: List[TransferJob]) -> TopologyPlan:
        src_region_tag, dst_region_tags = self.verify_job_src_dsts(jobs, multicast=True)
        # src_provider, src_region = src_region_tag.split(":")
        # dst_regions = [dst_region_tag.split(":")[1] for dst_region_tag in dst_region_tags]
        solution_graph = self.logical_plan(src_region_tag, dst_region_tags)
        self.solution_graph = solution_graph
        return self.logical_plan_to_topology_plan(jobs, solution_graph)


class MulticastMDSTPlanner(Planner):
    def __init__(self, transfer_config, n_instances: int, n_connections: int, n_partitions:int):
        super().__init__(transfer_config, n_instances, n_connections, n_partitions)
        self.G = super().make_nx_graph()

    def logical_plan(self, src_region: str, dst_regions: List[str]) -> nx.DiGraph:
        h = self.G.copy()
        h.remove_edges_from(list(h.in_edges(src_region)) + list(nx.selfloop_edges(h)))

        DST_graph = nx.algorithms.tree.Edmonds(h.subgraph([src_region] + dst_regions))
        opt_DST = DST_graph.find_optimum(attr="cost", kind="min", preserve_attrs=True, style="arborescence")

        # Construct MDST graph
        MDST_graph = nx.DiGraph()
        for edge in list(opt_DST.edges()):
            s, d = edge[0], edge[1]
            MDST_graph.add_edge(s, d, partitions=[str(i) for i in list(range(self.n_partitions))])

        for node in MDST_graph.nodes:
            MDST_graph.nodes[node]["num_vms"] = self.n_instances

        return MDST_graph

    def plan(self, jobs: List[TransferJob]) -> TopologyPlan:
        src_region_tag, dst_region_tags = self.verify_job_src_dsts(jobs, multicast=True)
        # src_provider, src_region = src_region_tag.split(":")
        # dst_regions = [dst_region_tag.split(":")[1] for dst_region_tag in dst_region_tags]
        solution_graph = self.logical_plan(src_region_tag, dst_region_tags)
        self.solution_graph = solution_graph
        return self.logical_plan_to_topology_plan(jobs, solution_graph)
    
class UnicastILPPlanner(Planner):
    def __init__(self, required_throughput_gbits, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.solver_required_throughput_gbits = required_throughput_gbits
        self.G = super().make_nx_graph()

    def logical_plan(self, src_region: str, dst_regions: List[str], jobs) -> nx.DiGraph:

        if isinstance(dst_regions, List):
            dst_regions = dst_regions[0]

        problem = ThroughputProblem(
            src=src_region,
            dst=dst_regions,
            required_throughput_gbits=self.solver_required_throughput_gbits,
            gbyte_to_transfer=1,
            instance_limit=self.n_instances,
        )

        with path("skyplane.data", "throughput.csv") as solver_throughput_grid:
            tput = ThroughputSolverILP(solver_throughput_grid)
        # solver = tput.choose_solver()
        solution = tput.solve_min_cost(problem)

        if not solution.is_feasible:
            raise RuntimeError("No feasible solution found")

        regions = tput.get_regions()
        return self.unicast_solution_to_nxgraph(solution, regions, jobs)

    def unicast_solution_to_nxgraph(self, solution: ThroughputSolution, regions, jobs) -> nx.DiGraph:
        """
        Convert ILP solution to logical plan in nx graph
        """
        v_result = solution.var_instances_per_region
        result_g = nx.DiGraph()  # solution nx graph
        for i, src_node in enumerate(regions):
            for j, dst_node in enumerate(regions):
                if solution.var_edge_flow_gigabits[i, j] > 0:
                    result_g.add_edge(
                        src_node,
                        dst_node,
                        partitions=[jobs[0].uuid],
                        throughput=self.G[src_node][dst_node]["throughput"],
                        cost=self.G[src_node][dst_node]["cost"],
                        num_conn=int(solution.var_conn[i][j])
                    )
        for i in range(len(v_result)):
            num_vms = math.ceil(v_result[i])
            node = regions[i]
            if node in result_g.nodes:
                result_g.nodes[node]["num_vms"] = num_vms

        return result_g

    def plan(self, jobs: List[TransferJob]) -> TopologyPlan:
        src_region_tag, dst_region_tags = self.verify_job_src_dsts(jobs)
        # src_provider, src_region = src_region_tag.split(":")
        # dst_regions = [dst_region_tag.split(":")[1] for dst_region_tag in dst_region_tags]
        solution_graph = self.logical_plan(src_region_tag, dst_region_tags, jobs)
        self.solution_graph = solution_graph
        return self.logical_plan_to_topology_plan(jobs, solution_graph)

class UnicastDirectPlanner(Planner):

    def __init__(self, transfer_config: TransferConfig,
                 n_instances: int,
                 n_connections: int,
                 n_partitions: Optional[int] = 1,
                 quota_limits_file: Optional[str] = None):
        super().__init__(transfer_config, n_instances, n_connections, n_partitions, quota_limits_file)

    def logical_plan(self, src_region: str, dst_regions: List[str], jobs) -> nx.DiGraph:
        graph = nx.DiGraph()
        graph.add_node(src_region)
        for dst_region in dst_regions:
            graph.add_node(dst_region)
            graph.add_edge(src_region, dst_region, partitions=[jobs[0].uuid])

        for node in graph.nodes:
            graph.nodes[node]["num_vms"] = self.n_instances

        return graph

    def plan(self, jobs: List[TransferJob]) -> TopologyPlan:
        src_region_tag, dst_region_tags = self.verify_job_src_dsts(jobs)
        # src_provider, src_region = src_region_tag.split(":")
        # dst_regions = [dst_region_tag.split(":")[1] for dst_region_tag in dst_region_tags]
        solution_graph = self.logical_plan(src_region_tag, dst_region_tags, jobs)
        self.solution_graph = solution_graph
        return self.logical_plan_to_topology_plan(jobs, solution_graph)

