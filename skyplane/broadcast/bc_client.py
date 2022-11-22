import uuid
from datetime import datetime
from pathlib import Path

from typing import TYPE_CHECKING, Optional, List

from skyplane.api.client import tmp_log_dir
from skyplane.api.usage.client import get_clientid
from skyplane.broadcast import BroadcastDataplane
from skyplane.broadcast.bc_planner import BroadcastDirectPlanner, BroadcastMDSTPlanner, BroadcastHSTPlanner, BroadcastILPSolverPlanner
from skyplane.api.impl.provisioner import Provisioner
from skyplane.api.transfer_config import TransferConfig
from skyplane.utils import logger

if TYPE_CHECKING:
    from skyplane.api.auth_config import AWSConfig, AzureConfig, GCPConfig


class SkyplaneBroadcastClient:
    def __init__(
        self,
        aws_config: Optional["AWSConfig"] = None,
        azure_config: Optional["AzureConfig"] = None,
        gcp_config: Optional["GCPConfig"] = None,
        transfer_config: Optional[TransferConfig] = None,
        log_dir: Optional[str] = None,
    ):
        self.clientid = get_clientid()
        self.aws_auth = aws_config.make_auth_provider() if aws_config else None
        self.azure_auth = azure_config.make_auth_provider() if azure_config else None
        self.gcp_auth = gcp_config.make_auth_provider() if gcp_config else None
        self.transfer_config = transfer_config if transfer_config else TransferConfig()
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
        num_connections: int = 32,
        num_partitions: int = 2,
        gbyte_to_transfer: float = 10,
        target_time: float = 100,
    ) -> BroadcastDataplane:
        # TODO: did not change the data plan yet
        if type == "direct":
            planner = BroadcastDirectPlanner(
                src_cloud_provider,
                src_region,
                dst_cloud_providers,
                dst_regions,
                n_vms,
                num_connections,
                num_partitions,
                gbyte_to_transfer,
            )
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
            )
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
            )
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
            )
        else:
            raise NotImplementedError(f"Dataplane type {type} not implemented")

        topo = planner.plan()
        logger.fs.info(f"[SkyplaneClient.direct_dataplane] Topology: {topo.to_json()}")
        return BroadcastDataplane(clientid=self.clientid, topology=topo, provisioner=self.provisioner, transfer_config=self.transfer_config)
