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
    ):
        self.clientid = get_clientid()
        self.aws_auth = aws_config.make_auth_provider() if aws_config else None
        self.azure_auth = azure_config.make_auth_provider() if azure_config else None
        self.gcp_auth = gcp_config.make_auth_provider() if gcp_config else None
        self.transfer_config = transfer_config if transfer_config else TransferConfig(multipart_enabled=multipart_enabled)
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
        azure_only: bool = False 

    ) -> BroadcastDataplane:
        # TODO: did not change the data plan yet
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
                azure_only=azure_only
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
                azure_only=azure_only
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
                azure_only=azure_only
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
                azure_only=azure_only
            )
            topo = planner.plan(filter_edge=filter_edge, filter_node=filter_node, solve_iterative=solve_iterative, solver_verbose=True)
        else:
            raise NotImplementedError(f"Dataplane type {type} not implemented")

        logger.fs.info(f"[SkyplaneClient.direct_dataplane] Topology: {topo.to_json()}")
        if type != "ILP":
            print(f"Solution: {topo.nx_graph.edges.data()}")

        return BroadcastDataplane(clientid=self.clientid, topology=topo, provisioner=self.provisioner, transfer_config=self.transfer_config)
