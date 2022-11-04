import uuid
from datetime import datetime
from pathlib import Path

from typing import TYPE_CHECKING, Optional

from skyplane.api.dataplane import Dataplane
from skyplane.api.impl.path import parse_path
from skyplane.api.impl.planner import DirectPlanner
from skyplane.api.impl.provisioner import Provisioner
from skyplane.api.transfer_config import TransferConfig
from skyplane.obj_store.object_store_interface import ObjectStoreInterface
from skyplane.utils import logger

if TYPE_CHECKING:
    from skyplane.api.auth_config import AWSConfig, AzureConfig, GCPConfig


class SkyplaneClient:
    def __init__(
        self,
        aws_config: Optional["AWSConfig"] = None,
        azure_config: Optional["AzureConfig"] = None,
        gcp_config: Optional["GCPConfig"] = None,
        transfer_config: Optional[TransferConfig] = None,
        log_dir: Optional[str] = None,
    ):
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
            host_uuid=uuid.UUID(int=uuid.getnode()).hex,
            aws_auth=self.aws_auth,
            azure_auth=self.azure_auth,
            gcp_auth=self.gcp_auth,
        )

    def copy(self, src: str, dst: str, recursive: bool = False, num_vms: int = 1):
        provider_src, bucket_src, self.src_prefix = parse_path(src)
        provider_dst, bucket_dst, self.dst_prefix = parse_path(dst)
        self.src_iface = ObjectStoreInterface.create(f"{provider_src}:infer", bucket_src)
        self.dst_iface = ObjectStoreInterface.create(f"{provider_dst}:infer", bucket_dst)
        if self.transfer_config.requester_pays:
            self.src_iface.set_requester_bool(True)
            self.dst_iface.set_requester_bool(True)
        src_region = self.src_iface.region_tag()
        dst_region = self.dst_iface.region_tag()
        dp = self.dataplane(*src_region.split(":"), *dst_region.split(":"), n_vms=num_vms)
        with dp.auto_deprovision():
            dp.provision(spinner=True)
            dp.queue_copy(src, dst, recursive=recursive)
            dp.run()

    # methods to create dataplane
    def dataplane(
        self,
        src_cloud_provider: str,
        src_region: str,
        dst_cloud_provider: str,
        dst_region: str,
        type: str = "direct",
        n_vms: int = 1,
        num_connections: int = 32,
    ) -> Dataplane:
        if type == "direct":
            planner = DirectPlanner(
                src_cloud_provider,
                src_region,
                dst_cloud_provider,
                dst_region,
                n_vms,
                num_connections,
            )
            topo = planner.plan()
            logger.fs.info(f"[SkyplaneClient.direct_dataplane] Topology: {topo.to_json()}")
            return Dataplane(topology=topo, provisioner=self.provisioner, transfer_config=self.transfer_config)
        else:
            raise NotImplementedError(f"Dataplane type {type} not implemented")


tmp_log_dir = Path("/tmp/skyplane")
