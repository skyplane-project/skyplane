import json
import time
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

from skyplane.planner.planner import MulticastDirectPlanner, DirectPlannerSourceOneSided, DirectPlannerDestOneSided
from skyplane.planner.topology import TopologyPlanGateway
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
        # cloud_regions: dict,
        max_instances: Optional[int] = 1,
        n_connections: Optional[int] = 64,
        planning_algorithm: Optional[str] = "direct",
        debug: Optional[bool] = False,
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
        # self.cloud_regions = cloud_regions
        # TODO: set max instances with VM CPU limits and/or config
        self.max_instances = max_instances
        self.n_connections = n_connections
        self.provisioner = provisioner
        self.transfer_config = transfer_config
        self.http_pool = urllib3.PoolManager(retries=urllib3.Retry(total=3))
        self.provisioning_lock = threading.Lock()
        self.provisioned = False
        self.transfer_dir = tmp_log_dir / "transfer_logs" / datetime.now().strftime("%Y%m%d_%H%M%S")
        self.transfer_dir.mkdir(exist_ok=True, parents=True)

        # dataplane
        self.dataplane = None

        # planner
        self.planning_algorithm = planning_algorithm
        if self.planning_algorithm == "direct":
            self.planner = MulticastDirectPlanner(self.max_instances, self.n_connections, self.transfer_config)
        elif self.planning_algorithm == "src_one_sided":
            self.planner = DirectPlannerSourceOneSided(self.max_instances, self.n_connections, self.transfer_config)
        elif self.planning_algorithm == "dst_one_sided":
            self.planner = DirectPlannerDestOneSided(self.max_instances, self.n_connections, self.transfer_config)
        else:
            raise ValueError(f"No such planning algorithm {planning_algorithm}")

        # transfer logs
        self.transfer_dir = tmp_log_dir / "transfer_logs" / datetime.now().strftime("%Y%m%d_%H%M%S")
        self.transfer_dir.mkdir(exist_ok=True, parents=True)
        self.debug = debug

        # pending tracker tasks
        self.jobs_to_dispatch: List[TransferJob] = []
        self.pending_transfers: List[TransferProgressTracker] = []
        self.bound_nodes: Dict[TopologyPlanGateway, compute.Server] = {}

    def create_dataplane(self, debug):
        # create plan from set of jobs scheduled
        topo = self.planner.plan(self.jobs_to_dispatch)

        # create dataplane from plan
        dp = Dataplane(self.clientid, topo, self.provisioner, self.transfer_config, str(self.transfer_dir), debug=debug)
        return dp

    def start(self, debug=False, progress=False):
        ## create plan from set of jobs scheduled
        # topo = self.planner.plan(self.jobs_to_dispatch)

        ## create dataplane from plan
        # dp = Dataplane(self.clientid, topo, self.provisioner, self.transfer_config, self.transfer_dir, debug=debug)
        dp = self.create_dataplane(debug)
        try:
            dp.provision(spinner=True)
            if progress:
                from skyplane.cli.impl.progress_bar import ProgressBarTransferHook

                tracker = dp.run_async(self.jobs_to_dispatch, hooks=ProgressBarTransferHook(dp.topology.dest_region_tags))
            else:
                tracker = dp.run_async(self.jobs_to_dispatch)

            # wait for job to finish
            tracker.join()

            # copy gateway logs
            if debug:
                dp.copy_gateway_logs()
        except Exception as e:
            dp.copy_gateway_logs()
        dp.deprovision(spinner=True)
        return dp

    def start_async(self, debug=False):
        dp = self.create_dataplane(debug)
        try:
            dp.provision(spinner=False)
            tracker = dp.run_async(self.jobs_to_dispatch)
            if debug:
                dp.copy_gateway_logs()
            return tracker
        except Exception as e:
            dp.copy_gateway_logs()
            return

    def queue_copy(
        self,
        src: str,
        dst: str or List[str],
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
        if isinstance(dst, str):
            dst = [dst]
        job = CopyJob(src, dst, recursive, requester_pays=self.transfer_config.requester_pays)
        logger.fs.debug(f"[SkyplaneClient] Queued copy job {job}")
        self.jobs_to_dispatch.append(job)
        return job.uuid

    def queue_sync(
        self,
        src: str,
        dst: str or List[str],
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
        if isinstance(dst, str):
            dst = [dst]
        job = SyncJob(src, dst, requester_pays=self.transfer_config.requester_pays)
        logger.fs.debug(f"[SkyplaneClient] Queued sync job {job}")
        self.jobs_to_dispatch.append(job)
        return job.uuid

    def estimate_total_cost(self):
        """Estimate total cost of queued jobs"""
        total_size = 0
        for job in self.jobs_to_dispatch:
            total_size += job.size_gb()

        # get planner topology
        topo = self.planner.plan(self.jobs_to_dispatch)

        # return size
        return total_size * topo.cost_per_gb
