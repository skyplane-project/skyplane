import uuid
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Optional

from skyplane.api.config import TransferConfig
from skyplane.api.dataplane import Dataplane
from skyplane.api.provisioner import Provisioner
from skyplane.api.usage import get_clientid
from skyplane.obj_store.object_store_interface import ObjectStoreInterface
from skyplane.planner.planner import DirectPlanner, ILPSolverPlanner, RONSolverPlanner
from skyplane.utils import logger
from skyplane.utils.definitions import tmp_log_dir
from skyplane.utils.path import parse_path

if TYPE_CHECKING:
    from skyplane.api.config import AWSConfig, AzureConfig, GCPConfig, TransferConfig


class SkyplaneClient:
    """Client for initializing cloud provider configurations."""

    def __init__(
        self,
        aws_config: Optional["AWSConfig"] = None,
        azure_config: Optional["AzureConfig"] = None,
        gcp_config: Optional["GCPConfig"] = None,
        transfer_config: Optional[TransferConfig] = None,
        log_dir: Optional[str] = None,
    ):
        """
        :param aws_config: aws cloud configurations
        :type aws_config: class AWSConfig (optional)
        :param azure_config: azure cloud configurations
        :type azure_config: class AzureConfig (optional)
        :param gcp_config: gcp cloud configurations
        :type gcp_config: class GCPConfig (optional)
        :param transfer_config: transfer configurations
        :type transfer_config: class TransferConfig (optional)
        :param log_dir: path to store transfer logs
        :type log_dir: str (optional)
        """
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

    def copy(self, src: str, dst: str, recursive: bool = False, num_vms: int = 1):
        """
        A simple version of Skyplane copy. It automatically waits for transfer to complete
        (the main thread is blocked) and deprovisions VMs at the end.

        :param src: Source prefix to copy from
        :type src: str
        :param dst: The destination of the transfer
        :type dst: str
        :param recursive: If true, will copy objects at folder prefix recursively (default: False)
        :type recursive: bool
        :param num_vms: The maximum number of instances to use per region (default: 1)
        :type num_vms: int
        """
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
        solver_type: str = "direct",
        solver_required_throughput_gbits: float = 1,
        n_vms: int = 1,
        n_connections: int = 32,
    ) -> Dataplane:
        """
        Create a dataplane and calculates the transfer topology.

        :param src_cloud_provider: the name of the source cloud provider
        :type src_cloud_provider: str
        :param src_region: the name of the source region bucket
        :type src_region: str
        :param dst_cloud_provider: the name of the destination cloud provider
        :type dst_cloud_provider: str
        :param dst_region: the name of the destination region bucket
        :type dst_region: str
        :param type: the type of the solver for calculating the topology (default: "direct")
        :type type: str
        :param num_vms: The maximum number of instances to use per region (default: 1)
        :type num_vms: int
        :param n_connections: The maximum number of connections to use in topology per region (default: 32)
        :type n_connections: int
        """
        if solver_type.lower() == "direct":
            planner = DirectPlanner(src_cloud_provider, src_region, dst_cloud_provider, dst_region, n_vms, n_connections)
            topo = planner.plan()
            logger.fs.info(f"[SkyplaneClient.direct_dataplane] Topology: {topo.to_json()}")
            return Dataplane(clientid=self.clientid, topology=topo, provisioner=self.provisioner, transfer_config=self.transfer_config)
        elif solver_type.upper() == "ILP":
            planner = ILPSolverPlanner(
                src_cloud_provider, src_region, dst_cloud_provider, dst_region, n_vms, n_connections, solver_required_throughput_gbits
            )
            topo = planner.plan()
            logger.fs.info(f"[SkyplaneClient.ilp_dataplane] Topology: {topo.to_json()}")
            return Dataplane(clientid=self.clientid, topology=topo, provisioner=self.provisioner, transfer_config=self.transfer_config)
        elif solver_type.upper() == "RON":
            planner = RONSolverPlanner(
                src_cloud_provider, src_region, dst_cloud_provider, dst_region, n_vms, n_connections, solver_required_throughput_gbits
            )
            topo = planner.plan()
            logger.fs.info(f"[SkyplaneClient.ron_dataplane] Topology: {topo.to_json()}")
            return Dataplane(clientid=self.clientid, topology=topo, provisioner=self.provisioner, transfer_config=self.transfer_config)
        else:
            raise NotImplementedError(f"Dataplane type {solver_type} not implemented")
