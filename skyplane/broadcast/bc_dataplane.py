import threading
import functools
from collections import Counter

import urllib3
import os

from typing import TYPE_CHECKING, Dict, List, Optional, Tuple
from collections import defaultdict, Counter
from skyplane import compute
from skyplane.api.dataplane import Dataplane
from skyplane.api.config import TransferConfig
from skyplane.planner.topology import ReplicationTopology, ReplicationTopologyGateway
from skyplane.api.tracker import TransferHook
from skyplane.broadcast.impl.bc_tracker import BCTransferProgressTracker
from skyplane.broadcast.impl.bc_transfer_job import BCCopyJob, BCSyncJob, BCTransferJob
from skyplane.utils.definitions import gateway_docker_image
from pprint import pprint
from skyplane.broadcast.bc_plan import BroadcastReplicationTopology
from skyplane.broadcast.gateway.gateway_program import (
    GatewayProgram,
    GatewaySend,
    GatewayReceive,
    GatewayReadObjectStore,
    GatewayWriteObjectStore,
    GatewayWriteLocal,
    GatewayGenData,
    GatewayMuxAnd,
    GatewayMuxOr,
    GatewayOperator,
)

from skyplane.utils import logger
from skyplane.utils.fn import PathLike
import nacl.secret
import nacl.utils
import urllib3

if TYPE_CHECKING:
    from skyplane.api.provisioner import Provisioner


class BroadcastDataplane(Dataplane):
    # TODO: need to change this
    """A Dataplane represents a concrete Skyplane broadcast network, including topology and VMs."""

    def __init__(
        self,
        clientid: str,
        topology: BroadcastReplicationTopology,
        provisioner: "Provisioner",
        transfer_config: TransferConfig,
    ):
        self.clientid = clientid
        self.topology = topology
        self.src_region_tag = self.topology.source_region()
        self.dst_region_tags = self.topology.sink_regions()
        regions = Counter([node.region for node in self.topology.gateway_nodes])
        self.max_instances = int(regions[max(regions, key=regions.get)])
        self.provisioner = provisioner
        self.transfer_config = transfer_config
        self.http_pool = urllib3.PoolManager(retries=urllib3.Retry(total=3))
        self.provisioning_lock = threading.Lock()
        self.provisioned = False
        self.gateway_programs = None

        # pending tracker tasks
        self.jobs_to_dispatch: List[BCTransferJob] = []
        self.pending_transfers: List[BCTransferProgressTracker] = []
        self.bound_nodes: Dict[ReplicationTopologyGateway, compute.Server] = {}

    def get_ips_in_region(self, region: str):
        public_ips = [self.bound_nodes[n].public_ip() for n in self.topology.gateway_nodes if n.region == region]
        try:  # NOTE: Azure does not have private ips implemented
            private_ips = [self.bound_nodes[n].private_ip() for n in self.topology.gateway_nodes if n.region == region]
        except Exception as e:
            private_ips = public_ips

        return public_ips, private_ips

    def get_object_store_connection(self, region: str):
        provider = region.split(":")[0]
        if provider == "aws" or provider == "gcp":
            #n_conn = 32
            n_conn = 32
        elif provider == "azure":
            n_conn = 24  # due to throttling limits from authentication
        return n_conn

    def add_operator_receive_send(
        self,
        solution_graph,
        bc_pg: GatewayProgram,
        region: str,
        partition_ids: List[int],
        obj_store: Optional[Tuple[str, str]] = None,
        dst_op: Optional[GatewayReceive] = None,
        gen_random_data: bool = False,
        max_conn_per_vm: int = 256,
    ) -> bool:
        if dst_op is not None:
            receive_op = dst_op
        else:
            if obj_store is None:
                if gen_random_data:
                    receive_op = GatewayGenData(size_mb=self.transfer_config.random_chunk_size_mb)
                else:
                    receive_op = GatewayReceive()
            else:
                receive_op = GatewayReadObjectStore(
                    bucket_name=obj_store[0], bucket_region=obj_store[1], num_connections=self.get_object_store_connection(region)
                )

        # find set of regions & ips in each region to send to for this partition
        g = solution_graph

        any_id = partition_ids[0]
        next_regions = set([edge[1] for edge in g.out_edges(region, data=True) if str(any_id) in edge[-1]["partitions"]])

        # if no regions to forward data to
        if len(next_regions) == 0:
            print(
                f"Region {region}, any id: {any_id}, partition ids: {partition_ids}, has no next region to forward data to: {g.out_edges(region, data=True)}"
            )
            return False

        # region name --> ips in this region
        region_to_ips_map = {}
        region_to_private_ips_map = {}
        for next_region in next_regions:
            region_to_ips_map[next_region], region_to_private_ips_map[next_region] = self.get_ips_in_region(next_region)

        # use muxand or muxor for partition_id
        operation = "MUX_AND" if len(next_regions) > 1 else "MUX_OR"
        mux_op = GatewayMuxAnd() if len(next_regions) > 1 else GatewayMuxOr()

        # non-dst node: add receive_op into gateway program
        if dst_op is None:
            bc_pg.add_operator(receive_op, partition_id=tuple(partition_ids))

        # MUX_AND: send this partition to multiple regions
        if operation == "MUX_AND":
            if dst_op is not None and dst_op.op_type == "mux_and":
                mux_op = receive_op
            else:  # do not add any nested mux_and if dst_op parent is mux_and
                bc_pg.add_operator(mux_op, receive_op, partition_id=tuple(partition_ids))

            tot_senders = sum([len(next_region_ips) for next_region_ips in region_to_ips_map.values()])

            for next_region, next_region_ips in region_to_ips_map.items():
                num_connections = int(max_conn_per_vm / tot_senders)

                if (
                    next_region.split(":")[0] == region.split(":")[0] and region.split(":")[0] == "gcp"
                ):  # gcp to gcp connection, use private ips
                    print("GCP to GCP connection, should use private ips")
                    send_ops = [
                        GatewaySend(ip, num_connections=num_connections, region=next_region)
                        for ip in region_to_private_ips_map[next_region]
                    ]
                else:
                    send_ops = [GatewaySend(ip, num_connections=num_connections, region=next_region) for ip in next_region_ips]

                # if next region has >1 gateways, add MUX_OR
                if len(next_region_ips) > 1:
                    mux_or_op = GatewayMuxOr()
                    bc_pg.add_operator(mux_or_op, mux_op, partition_id=tuple(partition_ids))
                    bc_pg.add_operators(send_ops, mux_or_op, partition_id=tuple(partition_ids))
                else:  # otherwise, the parent of send_op is mux_op ("MUX_AND")
                    assert len(send_ops) == 1
                    bc_pg.add_operator(send_ops[0], mux_op, partition_id=tuple(partition_ids))
        else:
            # only send this partition to a single region
            assert len(region_to_ips_map) == 1

            next_region = list(region_to_ips_map.keys())[0]

            if next_region.split(":")[0] == region.split(":")[0] and region.split(":")[0] == "gcp":
                print("GCP to GCP connection, should use private ips")
                ips = [ip for next_region_ips in region_to_private_ips_map.values() for ip in next_region_ips]
            else:
                ips = [ip for next_region_ips in region_to_ips_map.values() for ip in next_region_ips]

            num_connections = int(max_conn_per_vm / len(ips))
            send_ops = [GatewaySend(ip, num_connections=num_connections, region=next_region) for ip in ips]

            # if num of gateways > 1, then connect to MUX_OR
            if len(ips) > 1:
                bc_pg.add_operator(mux_op, receive_op, partition_id=tuple(partition_ids))
                bc_pg.add_operators(send_ops, mux_op)
            else:
                bc_pg.add_operators(send_ops, receive_op, partition_id=tuple(partition_ids))

        # print("Number of connections: ", num_connections)
        return True

    def add_dst_operator(
        self, solution_graph, bc_pg: GatewayProgram, region: str, partition_ids: List[int], obj_store: Optional[Tuple[str, str]] = None
    ):
        receive_op = GatewayReceive()
        bc_pg.add_operator(receive_op, partition_id=tuple(partition_ids))

        # write
        if obj_store is None:
            write_op = GatewayWriteLocal()  # not pass in the path for now
        else:
            write_op = GatewayWriteObjectStore(
                bucket_name=obj_store[0], bucket_region=obj_store[1], num_connections=self.get_object_store_connection(region)
            )

        g = solution_graph
        any_id = partition_ids[0]
        next_regions = set([edge[1] for edge in g.out_edges(region, data=True) if str(any_id) in edge[-1]["partitions"]])

        # if no regions to forward data to, just write
        if len(next_regions) == 0:
            bc_pg.add_operator(write_op, receive_op, partition_id=tuple(partition_ids))
        else:  # otherwise, "and" --> write and forward
            mux_and_op = GatewayMuxAnd()
            bc_pg.add_operator(mux_and_op, receive_op, partition_id=tuple(partition_ids))
            bc_pg.add_operator(write_op, mux_and_op, partition_id=tuple(partition_ids))
            self.add_operator_receive_send(solution_graph, bc_pg, region, partition_ids, dst_op=mux_and_op)

    def remap_keys(self, mapping):
        return [{"partitions": k, "value": v} for k, v in mapping.items()]

    @property
    @functools.lru_cache(maxsize=None)
    def current_gw_programs(self):
        solution_graph = self.topology.nx_graph
        # print("Solution graph: ", solution_graph.edges.data())

        num_partitions = self.topology.num_partitions
        src = self.src_region_tag
        dsts = self.dst_region_tags

        # region name --> gateway program shared by all gateways in this region
        gateway_programs = {}

        # NOTE: assume all transfer object share the same (src, dsts)? might not be correct
        one_transfer_job = self.jobs_to_dispatch[0]
        if not self.transfer_config.gen_random_data:
            src_obj_store = (one_transfer_job.src_bucket, one_transfer_job.src_region)

            dsts_obj_store_map = {}
            # dst bucket, dst region
            for b, r in one_transfer_job.dst_regions.items():
                dsts_obj_store_map[r] = (b, r)

            gen_random_data = False
        else:
            src_obj_store = None
            dsts_obj_store_map = None
            gen_random_data = True

        for node in solution_graph.nodes:
            node_gateway_program = GatewayProgram()

            partition_to_next_regions = {}
            for i in range(num_partitions):
                partition_to_next_regions[i] = set(
                    [edge[1] for edge in solution_graph.out_edges(node, data=True) if str(i) in edge[-1]["partitions"]]
                )

            import collections

            keys_per_set = collections.defaultdict(list)
            for key, value in partition_to_next_regions.items():
                keys_per_set[frozenset(value)].append(key)

            list_of_partitions = list(keys_per_set.values())

            # source node: read from object store or generate random data, then forward data
            for partitions in list_of_partitions:
                # print("Processing partitions: ", partitions)
                if node == src:
                    self.add_operator_receive_send(
                        solution_graph, node_gateway_program, node, partitions, obj_store=src_obj_store, gen_random_data=gen_random_data
                    )

                # dst receive data, write to object store / write local (if obj_store=None), forward data if needed
                elif node in dsts:
                    dst_obj_store = None if dsts_obj_store_map is None else dsts_obj_store_map[node]
                    self.add_dst_operator(solution_graph, node_gateway_program, node, partitions, obj_store=dst_obj_store)

                # overlay node only forward data
                else:
                    self.add_operator_receive_send(solution_graph, node_gateway_program, node, partitions, obj_store=None)

            gateway_programs[node] = self.remap_keys(node_gateway_program.to_dict())
            assert len(gateway_programs[node]) > 0, f"Empty gateway program {node}"
            #print("PROGRAM", gateway_programs[node])

        return gateway_programs

    def _start_gateway(
        self,
        gateway_docker_image: str,
        gateway_node: ReplicationTopologyGateway,
        gateway_server: compute.Server,
        gateway_log_dir: Optional[PathLike] = None,
        authorize_ssh_pub_key: Optional[str] = None,
        e2ee_key_bytes: Optional[str] = None,
    ):
        am_source = gateway_node in self.topology.source_instances()
        am_sink = gateway_node in self.topology.sink_instances()

        # start gateway
        if gateway_log_dir:
            gateway_server.init_log_files(gateway_log_dir)
        if authorize_ssh_pub_key:
            gateway_server.copy_public_key(authorize_ssh_pub_key)

        gateway_server.start_gateway(
            {},  # don't need setup arguments here to pass as outgoing_ports
            gateway_programs=self.current_gw_programs,  # NOTE: BC pass in gateway programs
            gateway_docker_image=gateway_docker_image,
            e2ee_key_bytes=e2ee_key_bytes if (self.transfer_config.use_e2ee and (am_source or am_sink)) else None,
            use_bbr=False,
            use_compression=self.transfer_config.use_compression,
            use_socket_tls=self.transfer_config.use_socket_tls,
        )

    def source_gateways(self) -> List[compute.Server]:
        return [self.bound_nodes[n] for n in self.topology.source_instances()] if self.provisioned else []

    def sink_gateways(self) -> List[compute.Server]:
        return [self.bound_nodes[n] for n in self.topology.sink_instances()] if self.provisioned else []

    def queue_copy(
        self,
        src: str,
        dsts: List[str],
        recursive: bool = False,
    ) -> str:
        if len(src) != 0:
            assert self.transfer_config.gen_random_data is False
            job = BCCopyJob(
                src,
                dsts[0],
                recursive,
                dst_paths=dsts,
                requester_pays=self.transfer_config.requester_pays,
                transfer_config=self.transfer_config,
            )
        else:
            assert self.transfer_config.gen_random_data is True
            job = BCCopyJob("", "", False, [], requester_pays=self.transfer_config.requester_pays, transfer_config=self.transfer_config)

        logger.fs.debug(f"[SkyplaneBroadcastClient] Queued copy job {job}")
        self.jobs_to_dispatch.append(job)
        return job.uuid

    def queue_sync(
        self,
        src: str,
        dsts: List[str],
        recursive: bool = False,
    ) -> str:
        job = BCSyncJob(
            src,
            dsts[0],
            recursive,
            dst_paths=dsts,
            requester_pays=self.transfer_config.requester_pays,
            transfer_config=self.transfer_config,
        )
        logger.fs.debug(f"[SkyplaneBroadcastClient] Queued sync job {job}")
        self.jobs_to_dispatch.append(job)
        return job.uuid

    def run_async(self, hooks: Optional[TransferHook] = None) -> BCTransferProgressTracker:
        if not self.provisioned:
            logger.error("Dataplane must be pre-provisioned. Call dataplane.provision() before starting a transfer")
        tracker = BCTransferProgressTracker(self, self.jobs_to_dispatch, self.transfer_config, hooks)
        self.pending_transfers.append(tracker)
        tracker.start()
        logger.fs.info(f"[SkyplaneBroadcastClient] Started async transfer with {len(self.jobs_to_dispatch)} jobs")
        self.jobs_to_dispatch = []
        return tracker

    def run(self, hooks: Optional[TransferHook] = None):
        tracker = self.run_async(hooks)
        logger.fs.debug(f"[SkyplaneBroadcastClient] Waiting for transfer to complete")
        tracker.join()
