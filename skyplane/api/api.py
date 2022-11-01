import uuid
from datetime import datetime
from pathlib import Path
from typing import List, Optional

from skyplane import tmp_log_dir
from skyplane.api.auth_config import AWSConfig, AzureConfig, GCPConfig
from skyplane.api.config import TransferConfig
from skyplane.api.dataplane import Dataplane
from skyplane.api.impl.planner import DirectPlanner
from skyplane.api.impl.provisioner import Provisioner
from skyplane.api.impl.transfer_job import TransferJob, CopyJob, SyncJob
from skyplane.api.tracker import TransferProgressTracker
from skyplane.utils import logger


class SkyplaneClient:
    def __init__(
        self,
        aws_config: Optional[AWSConfig] = None,
        azure_config: Optional[AzureConfig] = None,
        gcp_config: Optional[GCPConfig] = None,
        log_dir: Optional[str] = None,
    ):
        self.aws_auth = aws_config.make_auth_provider() if aws_config else None
        self.azure_auth = azure_config.make_auth_provider() if azure_config else None
        self.gcp_auth = gcp_config.make_auth_provider() if gcp_config else None
        self.log_dir = (
            tmp_log_dir / "transfer_logs" / f"{datetime.now().strftime('%Y%m%d_%H%M%S')}-{uuid.uuid4()}"
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
        self.jobs_to_dispatch: List[TransferJob] = []

    def copy(self, src: str, dst: str, recursive: bool = False, num_vms: int = 1):
        raise NotImplementedError("Simple copy not yet implemented")

    # methods to create dataplane
    def direct_dataplane(
        self,
        src_cloud_provider: str,
        src_region: str,
        dst_cloud_provider: str,
        dst_region: str,
        n_vms: int = 1,
        num_connections: int = 32,
        **kwargs,
    ) -> Dataplane:
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
        return Dataplane(topology=topo, provisioner=self.provisioner, **kwargs)

    # main API methods to dispatch transfers to dataplane
    def queue_copy(
        self,
        src: str,
        dst: str,
        recursive: bool = False,
    ) -> str:
        job = CopyJob(src, dst, recursive)
        logger.fs.debug(f"[SkyplaneClient] Queued copy job {job}")
        self.jobs_to_dispatch.append(job)
        return job.uuid

    def queue_sync(
        self,
        src: str,
        dst: str,
        recursive: bool = False,
    ) -> str:
        job = SyncJob(src, dst, recursive)
        logger.fs.debug(f"[SkyplaneClient] Queued sync job {job}")
        self.jobs_to_dispatch.append(job)
        return job.uuid

    def run_async(self, dataplane: Dataplane, **kwargs) -> TransferProgressTracker:
        if not dataplane.provisioned:
            logger.error("Dataplane must be pre-provisioned. Call dataplane.provision() before starting a transfer")
        config = TransferConfig(**kwargs)
        tracker = TransferProgressTracker(dataplane, self.jobs_to_dispatch, config)
        dataplane.register_pending_transfer(tracker)
        tracker.start()
        logger.fs.info(f"[SkyplaneClient] Started async transfer with {len(self.jobs_to_dispatch)} jobs")
        self.jobs_to_dispatch = []
        return tracker

    def run(self, dataplane: Dataplane):
        tracker = self.run_async(dataplane)
        logger.fs.debug(f"[SkyplaneClient] Waiting for transfer to complete")
        tracker.join()
