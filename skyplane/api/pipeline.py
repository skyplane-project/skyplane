import json
import os
import threading
from collections import defaultdict, Counter
from datetime import datetime
from functools import partial
from datetime import datetime

import nacl.secret
import nacl.utils
import urllib3
from typing import TYPE_CHECKING, Dict, List, Optional

from skyplane import compute
from skyplane.api.tracker import TransferProgressTracker, TransferHook
from skyplane.api.transfer_job import CopyJob, SyncJob, TransferJob
from skyplane.api.config import TransferConfig
from skyplane.planner.topology_old import ReplicationTopology, ReplicationTopologyGateway
from skyplane.planner.planner import DirectPlanner
from skyplane.utils import logger
from skyplane.utils.definitions import gateway_docker_image, tmp_log_dir
from skyplane.utils.fn import PathLike, do_parallel

from skyplane.api.dataplane import Dataplane

if TYPE_CHECKING:
    from skyplane.api.provisioner import Provisioner


class Pipeline:
    """A pipeline object stores and executes a set of transfer jobs."""

    def __init__(
        self,
        clientid: str,
        provisioner: "Provisioner",
        transfer_config: TransferConfig,
        debug: bool = False,
    ):
        """
        :param clientid: the uuid of the local host to create the dataplane
        :type clientid: str
        :param provisioner: the provisioner to launch the VMs
        :type provisioner: Provisioner
        :param transfer_config: the configuration during the transfer
        :type transfer_config: TransferConfig
        """
        self.clientid = clientid
        # TODO: set max instances with VM CPU limits and/or config
        self.max_instances = 1
        self.provisioner = provisioner
        self.transfer_config = transfer_config
        self.http_pool = urllib3.PoolManager(retries=urllib3.Retry(total=3))
        self.provisioning_lock = threading.Lock()
        self.provisioned = False
        self.transfer_dir = tmp_log_dir / "transfer_logs" / datetime.now().strftime("%Y%m%d_%H%M%S")
        self.transfer_dir.mkdir(exist_ok=True, parents=True)

        # transfer logs
        self.transfer_dir = tmp_log_dir / "transfer_logs" / datetime.now().strftime("%Y%m%d_%H%M%S")
        self.transfer_dir.mkdir(exist_ok=True, parents=True)
        self.debug = debug

        # pending tracker tasks
        self.jobs_to_dispatch: List[TransferJob] = []
        self.pending_transfers: List[TransferProgressTracker] = []
        self.bound_nodes: Dict[ReplicationTopologyGateway, compute.Server] = {}

    def start(self): 

        # TODO: Set number of connections properly (or not at all)
        planner = DirectPlanner(self.max_instances, 32)

        # create plan from set of jobs scheduled
        topo = planner.plan(self.jobs_to_dispatch)

        # create dataplane from plan 
        dp = Dataplane(self.clientid, topo, self.provisioner, self.transfer_config, self.transfer_dir, debug=self.debug)
        dp.provision(spinner=True) 
        dp.run()

    def queue_copy(
        self,
        src: str,
        dst: str,
        recursive: bool = False,
    ) -> str:
        """
        Add a copy job to job list.
        Return the uuid of the job.

        :param src: source prefix to copy from
        :type src: str
        :param dst: the destination of the transfer
        :type dst: str
        :param recursive: if true, will copy objects at folder prefix recursively (default: False)
        :type recursive: bool
        """
        job = CopyJob(src, dst, recursive, requester_pays=self.transfer_config.requester_pays)
        logger.fs.debug(f"[SkyplaneClient] Queued copy job {job}")
        self.jobs_to_dispatch.append(job)
        return job.uuid

    def queue_sync(
        self,
        src: str,
        dst: str,
    ) -> str:
        """
        Add a sync job to job list.
        Return the uuid of the job.

        :param src: Source prefix to copy from
        :type src: str
        :param dst: The destination of the transfer
        :type dst: str
        :param recursive: If true, will copy objects at folder prefix recursively (default: False)
        :type recursive: bool
        """
        job = SyncJob(src, dst, recursive=True, requester_pays=self.transfer_config.requester_pays)
        logger.fs.debug(f"[SkyplaneClient] Queued sync job {job}")
        self.jobs_to_dispatch.append(job)
        return job.uuid

    def run_async(self, hooks: Optional[TransferHook] = None) -> TransferProgressTracker:
        """Start the transfer asynchronously. The main thread will not be blocked.

        :param hooks: Tracks the status of the transfer
        :type hooks: TransferHook
        """
        if not self.provisioned:
            logger.error("Dataplane must be pre-provisioned. Call dataplane.provision() before starting a transfer")
        tracker = TransferProgressTracker(self, self.jobs_to_dispatch, self.transfer_config, hooks)
        self.pending_transfers.append(tracker)
        tracker.start()
        logger.fs.info(f"[SkyplaneClient] Started async transfer with {len(self.jobs_to_dispatch)} jobs")
        self.jobs_to_dispatch = []
        return tracker

    def run(self, hooks: Optional[TransferHook] = None):
        """Start the transfer in the main thread. Wait until the transfer is complete.

        :param hooks: Tracks the status of the transfer
        :type hooks: TransferHook
        """
        tracker = self.run_async(hooks)
        logger.fs.debug(f"[SkyplaneClient] Waiting for transfer to complete")
        tracker.join()
