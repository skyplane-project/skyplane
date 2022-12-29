import uuid
from datetime import datetime
from pathlib import Path

from typing import TYPE_CHECKING, Optional, List

from skyplane.api.client import tmp_log_dir
from skyplane.api.client import get_clientid
from skyplane.broadcast.bc_dataplane import BroadcastDataplane
from skyplane.broadcast.bc_planner import BroadcastDirectPlanner, BroadcastMDSTPlanner, BroadcastHSTPlanner, BroadcastILPSolverPlanner
from skyplane.api.provisioner import Provisioner
from skyplane.api.config import TransferConfig
from skyplane.utils import logger
from skyplane.utils.definitions import MB, GB

if TYPE_CHECKING:
    from skyplane.api.config import AWSConfig, AzureConfig, GCPConfig


class SkyplaneBroadcastClient:
    def __init__(
        self,
        aws_config: Optional["AWSConfig"] = None,
        azure_config: Optional["AzureConfig"] = None,
        gcp_config: Optional["GCPConfig"] = None,
        transfer_config: Optional[TransferConfig] = None,
        log_dir: Optional[str] = None,
        multipart_enabled: Optional[bool] = False,
        # random generate data or not
        generate_random: Optional[bool] = False,
        num_random_chunks: Optional[int] = 64,
        random_chunk_size_mb: Optional[int] = 8,
        src_region: Optional[str] = None,
        dst_regions: Optional[List[str]] = None,
    ):
        self.clientid = get_clientid()
        self.aws_auth = aws_config.make_auth_provider() if aws_config else None
        self.azure_auth = azure_config.make_auth_provider() if azure_config else None
        self.gcp_auth = gcp_config.make_auth_provider() if gcp_config else None
        self.transfer_config = (
            transfer_config
            if transfer_config
            else TransferConfig(
                multipart_enabled=multipart_enabled,
                gen_random_data=generate_random,
                num_random_chunks=num_random_chunks,
                random_chunk_size_mb=random_chunk_size_mb,
                src_region=src_region,
                dst_regions=dst_regions,
                use_bbr=False,
            )
        )
        print("transfer config: ", transfer_config)
        self.log_dir = (
            tmp_log_dir / "transfer_logs" / f"{datetime.now().strftime('%Y%m%d_%H%M%S')}-{uuid.uuid4().hex[:8]}"
            if log_dir is None
            else Path(log_dir)
        )

        # set up logging
        self.log_dir.mkdir(parents=True, exist_ok=True)
        logger.open_log_file(self.log_dir / "client.log")

        self.provisioner = Provisioner(
            host_uuid=self.clientid,
            aws_auth=self.aws_auth,
            azure_auth=self.azure_auth,
            gcp_auth=self.gcp_auth,
        )


    def networkx_to_graphviz(self, src, dsts, g, label='partitions'):
        import graphviz as gv
        """Convert `networkx` graph `g` to `graphviz.Digraph`.

        @type g: `networkx.Graph` or `networkx.DiGraph`
        @rtype: `graphviz.Digraph`
        """
        if g.is_directed():
            h = gv.Digraph()
        else:
            h = gv.Graph()
        for u, d in g.nodes(data=True):
            #u = u.split(",")[0]
            if u.split(",")[0] == src:
                h.node(str(u.replace(":", " ")), fillcolor="red", style='filled')
            elif u.split(",")[0] in dsts:
                h.node(str(u.replace(":", " ")), fillcolor="green", style='filled')
            h.node(str(u.replace(":", " ")))
        for u, v, d in g.edges(data=True):
            # print('edge', u, v, d)
            h.edge(str(u.replace(":", " ")), str(v.replace(":", " ")), label=str(d[label]))
        h.render(directory='solution', view=True)
        return h

    # methods to create dataplane
    def broadcast_dataplane(
        self,
        src_cloud_provider: str,
        src_region: str,
        dst_cloud_providers: List[str],
        dst_regions: List[str],
        type: str = "direct",
        n_vms: int = 1,
        num_connections: int = 256,
        num_partitions: int = 10,
        gbyte_to_transfer: float = 1,
        # ILP specific parameters
        target_time: float = 10,
        filter_node: bool = False,
        filter_edge: bool = False,
        solve_iterative: bool = False,
        # solve range (use aws/gcp/azure only nodes)
        aws_only: bool = False,
        gcp_only: bool = False,
        azure_only: bool = False,
    ) -> BroadcastDataplane:
        print(f"\nAlgorithm: {type}")
        if type == "Ndirect":
            planner = BroadcastDirectPlanner(
                src_cloud_provider,
                src_region,
                dst_cloud_providers,
                dst_regions,
                n_vms,
                num_connections,
                num_partitions,
                gbyte_to_transfer,
                aws_only=aws_only,
                gcp_only=gcp_only,
                azure_only=azure_only,
            )
            topo = planner.plan()
        elif type == "MDST":
            planner = BroadcastMDSTPlanner(
                src_cloud_provider,
                src_region,
                dst_cloud_providers,
                dst_regions,
                n_vms,
                num_connections,
                num_partitions,
                gbyte_to_transfer,
                aws_only=aws_only,
                gcp_only=gcp_only,
                azure_only=azure_only,
            )
            topo = planner.plan()
        elif type == "HST":
            # TODO: not usable now
            planner = BroadcastHSTPlanner(
                src_cloud_provider,
                src_region,
                dst_cloud_providers,
                dst_regions,
                n_vms,
                num_connections,
                num_partitions,
                gbyte_to_transfer,
                aws_only=aws_only,
                gcp_only=gcp_only,
                azure_only=azure_only,
            )
            topo = planner.plan()
        elif type == "ILP":
            planner = BroadcastILPSolverPlanner(
                src_cloud_provider,
                src_region,
                dst_cloud_providers,
                dst_regions,
                n_vms,
                num_connections,
                num_partitions,
                gbyte_to_transfer,
                target_time,  # target time budget
                aws_only=aws_only,
                gcp_only=gcp_only,
                azure_only=azure_only,
            )
            topo = planner.plan(filter_edge=filter_edge, filter_node=filter_node, solve_iterative=solve_iterative, solver_verbose=True)
        else:
            raise NotImplementedError(f"Dataplane type {type} not implemented")

        logger.fs.info(f"[SkyplaneClient.direct_dataplane] Topology: {topo.to_json()}")
        if type != "ILP":
            print(f"Solution: {topo.nx_graph.edges.data()}")
            print(topo.nx_graph.nodes)
            print(src_region)
            self.networkx_to_graphviz(f"{src_cloud_provider}:{src_region}", [f"{provider}:{region}" for provider, region in zip(dst_cloud_providers, dst_regions)], topo.nx_graph)

        print("Transfer src region: ", self.transfer_config.src_region)
        print("Transfer dst regions: ", self.transfer_config.dst_regions)

        return BroadcastDataplane(clientid=self.clientid, topology=topo, provisioner=self.provisioner, transfer_config=self.transfer_config)
