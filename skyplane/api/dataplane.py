import json
import os
import threading
from collections import defaultdict
from functools import partial

import nacl.secret
import nacl.utils
import urllib3
from typing import TYPE_CHECKING, Dict, List, Optional

from skyplane import compute
from skyplane.api.impl.tracker import TransferProgressTracker
from skyplane.api.impl.transfer_job import CopyJob, SyncJob, TransferJob
from skyplane.api.transfer_config import TransferConfig
from skyplane.replicate.replication_plan import ReplicationTopology, ReplicationTopologyGateway
from skyplane.utils import logger
from skyplane.utils.definitions import gateway_docker_image
from skyplane.utils.fn import PathLike, do_parallel

if TYPE_CHECKING:
    from skyplane.api.impl.provisioner import Provisioner


class DataplaneAutoDeprovision:
    def __init__(self, dataplane: "Dataplane"):
        self.dataplane = dataplane

    def __enter__(self):
        return self.dataplane

    def __exit__(self, exc_type, exc_value, exc_tb):
        logger.error("Deprovisioning dataplane")
        self.dataplane.deprovision()


class Dataplane:
    """A Dataplane represents a concrete Skyplane network, including topology and VMs."""

    def __init__(
        self,
        topology: ReplicationTopology,
        provisioner: "Provisioner",
        transfer_config: TransferConfig,
    ):
        self.topology = topology
        self.provisioner = provisioner
        self.transfer_config = transfer_config
        self.http_pool = urllib3.PoolManager(retries=urllib3.Retry(total=3))
        self.provisioning_lock = threading.Lock()
        self.provisioned = False

        # pending tracker tasks
        self.jobs_to_dispatch: List[TransferJob] = []
        self.pending_transfers: List[TransferProgressTracker] = []
        self.bound_nodes: Dict[ReplicationTopologyGateway, compute.Server] = {}

    def provision(
        self,
        allow_firewall: bool = True,
        gateway_docker_image: str = os.environ.get("SKYPLANE_DOCKER_IMAGE", gateway_docker_image()),
        gateway_log_dir: Optional[PathLike] = None,
        authorize_ssh_pub_key: Optional[str] = None,
        max_jobs: int = 16,
        spinner: bool = False,
    ):
        with self.provisioning_lock:
            if self.provisioned:
                logger.error("Cannot provision dataplane, already provisioned!")
                return
            aws_nodes_to_provision = list(n.region.split(":")[0] for n in self.topology.nodes if n.region.startswith("aws:"))
            azure_nodes_to_provision = list(n.region.split(":")[0] for n in self.topology.nodes if n.region.startswith("azure:"))
            gcp_nodes_to_provision = list(n.region.split(":")[0] for n in self.topology.nodes if n.region.startswith("gcp:"))

            # create VMs from the topology
            for node in self.topology.gateway_nodes:
                cloud_provider, region = node.region.split(":")
                self.provisioner.add_task(
                    cloud_provider=cloud_provider,
                    region=region,
                    vm_type=getattr(self.transfer_config, f"{cloud_provider}_instance_class"),
                    spot=getattr(self.transfer_config, f"{cloud_provider}_use_spot_instances"),
                    autoterminate_minutes=self.transfer_config.autoterminate_minutes,
                )

            # initialize clouds
            self.provisioner.init_global(
                aws=len(aws_nodes_to_provision) > 0,
                azure=len(azure_nodes_to_provision) > 0,
                gcp=len(gcp_nodes_to_provision) > 0,
            )

            # provision VMs
            uuids = self.provisioner.provision(
                authorize_firewall=allow_firewall,
                max_jobs=max_jobs,
                spinner=spinner,
            )

            # bind VMs to nodes
            servers = [self.provisioner.get_node(u) for u in uuids]
            servers_by_region = defaultdict(list)
            for s in servers:
                servers_by_region[s.region_tag].append(s)
            for node in self.topology.gateway_nodes:
                instance = servers_by_region[node.region].pop()
                self.bound_nodes[node] = instance
            logger.fs.debug(f"[Dataplane.provision] {self.bound_nodes=}")
            gateway_bound_nodes = self.bound_nodes.copy()

            # start gateways
            self.provisioned = True

        def _start_gateway(
            gateway_node: ReplicationTopologyGateway,
            gateway_server: compute.Server,
        ):
            # map outgoing ports
            setup_args = {}
            for n, v in self.topology.get_outgoing_paths(gateway_node).items():
                if isinstance(n, ReplicationTopologyGateway):
                    # use private ips for gcp to gcp connection
                    src_provider, dst_provider = gateway_node.region.split(":")[0], n.region.split(":")[0]
                    if src_provider == dst_provider and src_provider == "gcp":
                        setup_args[self.bound_nodes[n].private_ip()] = v
                    else:
                        setup_args[self.bound_nodes[n].public_ip()] = v
            am_source = gateway_node in self.topology.source_instances()
            am_sink = gateway_node in self.topology.sink_instances()
            logger.fs.debug(f"[Dataplane._start_gateway] Setup args for {gateway_node}: {setup_args}")

            # start gateway
            if gateway_log_dir:
                gateway_server.init_log_files(gateway_log_dir)
            if authorize_ssh_pub_key:
                gateway_server.copy_public_key(authorize_ssh_pub_key)
            gateway_server.start_gateway(
                setup_args,
                gateway_docker_image=gateway_docker_image,
                e2ee_key_bytes=e2ee_key_bytes if (self.transfer_config.use_e2ee and (am_source or am_sink)) else None,
                use_bbr=self.transfer_config.use_bbr,
                use_compression=self.transfer_config.use_compression,
                use_socket_tls=self.transfer_config.use_socket_tls,
            )

        # todo: move server.py:start_gateway here
        logger.fs.info(f"Using {gateway_docker_image=}")
        e2ee_key_bytes = nacl.utils.random(nacl.secret.SecretBox.KEY_SIZE)

        jobs = []
        for node, server in gateway_bound_nodes.items():
            jobs.append(partial(_start_gateway, node, server))
        logger.fs.debug(f"[Dataplane.provision] Starting gateways on {len(jobs)} servers")
        do_parallel(lambda fn: fn(), jobs, n=-1, spinner=spinner, spinner_persist=spinner, desc="Starting gateway container on VMs")

    def deprovision(self, max_jobs: int = 64, spinner: bool = False):
        with self.provisioning_lock:
            if not self.provisioned:
                logger.fs.warning("Attempting to deprovision dataplane that is not provisioned, this may be from auto_deprovision.")
            # wait for tracker tasks
            try:
                for task in self.pending_transfers:
                    logger.warning(f"Before deprovisioning, waiting for jobs to finish: {list(task.jobs.keys())}")
                    task.join()
            except KeyboardInterrupt:
                logger.warning("Interrupted while waiting for transfers to finish, deprovisioning anyway.")
                raise
            finally:
                self.provisioner.deprovision(
                    max_jobs=max_jobs,
                    spinner=spinner,
                )
                self.provisioned = False

    def check_error_logs(self) -> Dict[str, List[str]]:
        def get_error_logs(args):
            _, instance = args
            reply = self.http_pool.request("GET", f"{instance.gateway_api_url}/api/v1/errors")
            if reply.status != 200:
                raise Exception(f"Failed to get error logs from gateway instance {instance.instance_name()}: {reply.data.decode('utf-8')}")
            return json.loads(reply.data.decode("utf-8"))["errors"]

        errors: Dict[str, List[str]] = {}
        for (_, instance), result in do_parallel(get_error_logs, self.bound_nodes.items(), n=-1):
            errors[instance] = result
        return errors

    def auto_deprovision(self) -> DataplaneAutoDeprovision:
        """Returns a context manager that will automatically call deprovision upon exit."""
        return DataplaneAutoDeprovision(self)

    def source_gateways(self) -> List[compute.Server]:
        return [self.bound_nodes[n] for n in self.topology.source_instances()] if self.provisioned else []

    def sink_gateways(self) -> List[compute.Server]:
        return [self.bound_nodes[n] for n in self.topology.sink_instances()] if self.provisioned else []

    def queue_copy(
        self,
        src: str,
        dst: str,
        recursive: bool = False,
    ) -> str:
        job = CopyJob(src, dst, recursive, requester_pays=self.transfer_config.requester_pays)
        logger.fs.debug(f"[SkyplaneClient] Queued copy job {job}")
        self.jobs_to_dispatch.append(job)
        return job.uuid

    def queue_sync(
        self,
        src: str,
        dst: str,
        recursive: bool = False,
    ) -> str:
        job = SyncJob(src, dst, recursive, requester_pays=self.transfer_config.requester_pays)
        logger.fs.debug(f"[SkyplaneClient] Queued sync job {job}")
        self.jobs_to_dispatch.append(job)
        return job.uuid

    def run_async(self) -> TransferProgressTracker:
        if not self.provisioned:
            logger.error("Dataplane must be pre-provisioned. Call dataplane.provision() before starting a transfer")
        tracker = TransferProgressTracker(self, self.jobs_to_dispatch, self.transfer_config)
        self.pending_transfers.append(tracker)
        tracker.start()
        logger.fs.info(f"[SkyplaneClient] Started async transfer with {len(self.jobs_to_dispatch)} jobs")
        self.jobs_to_dispatch = []
        return tracker

    def run(self):
        tracker = self.run_async()
        logger.fs.debug(f"[SkyplaneClient] Waiting for transfer to complete")
        tracker.join()
