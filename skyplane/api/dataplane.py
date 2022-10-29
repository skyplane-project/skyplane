from collections import defaultdict
from functools import partial
import os
from pathlib import Path
import pickle
import threading
from typing import Dict, List, Optional

import nacl.secret
import nacl.utils

from skyplane import gateway_docker_image
from skyplane.api.impl.provisioner import Provisioner
from skyplane.api.tracker import TransferProgressTracker
from skyplane.compute.server import Server
from skyplane.replicate.replication_plan import ReplicationTopology, ReplicationTopologyGateway
from skyplane.utils import logger
from skyplane.utils.fn import PathLike, do_parallel


class DataplaneAutoDeprovision:
    def __init__(self, dataplane: "Dataplane"):
        self.dataplane = dataplane

    def __enter__(self):
        return self.dataplane

    def __exit__(self, exc_type, exc_value, exc_tb):
        self.dataplane.deprovision()


class Dataplane:
    """A Dataplane represents a concrete Skyplane network, including topology and VMs."""

    def __init__(
        self,
        topology: ReplicationTopology,
        provisioner: Provisioner,
        **kwargs,
    ):
        self.topology = topology
        self.provisioner = provisioner
        self.provisioning_lock = threading.Lock()
        self.provisioned = False

        # pending tracker tasks
        self.pending_transfers: List[TransferProgressTracker] = []

        # config parameters
        self.config = {
            "autoterminate_minutes": 15,
            "use_bbr": True,
            "use_compression": True,
            "use_e2ee": True,
            "use_socket_tls": False,
            "aws_use_spot_instances": False,
            "azure_use_spot_instances": False,
            "gcp_use_spot_instances": False,
            "aws_instance_class": "m5.8xlarge",
            "azure_instance_class": "Standard_D2_v5",
            "gcp_instance_class": "n2-standard-16",
            "gcp_use_premium_network": True,
        }
        self.config.update(kwargs)

        # vm state
        self.bound_nodes: Dict[ReplicationTopologyGateway, Server] = {}

    def provision(
        self,
        allow_firewall: bool = True,
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
                    vm_type=self.config[f"{cloud_provider}_instance_class"],
                    spot=self.config.get(f"{cloud_provider}_use_spot_instances", False),
                    autoterminate_minutes=self.config.get("autoterminate_minutes", None),
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

            # start gateways
            self.provisioned = True

    def start_gateway(
        self,
        gateway_docker_image: str = os.environ.get("SKYPLANE_DOCKER_IMAGE", gateway_docker_image()),
        gateway_log_dir: Optional[PathLike] = None,
        authorize_ssh_pub_key: Optional[str] = None,
        spinner: bool = False,
    ):
        def _start_gateway(
            gateway_node: ReplicationTopologyGateway,
            gateway_server: Server,
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
                use_bbr=self.config.get("use_bbr", True),
                use_compression=self.config.get("use_compression", True),
                e2ee_key_bytes=e2ee_key_bytes if (am_source or am_sink) else None,
                use_socket_tls=self.config.get("use_socket_tls", False),
            )

        # todo: move server.py:start_gateway here
        logger.fs.info(f"Using {gateway_docker_image=}")
        e2ee_key_bytes = nacl.utils.random(nacl.secret.SecretBox.KEY_SIZE)

        jobs = []
        for node, server in self.bound_nodes.items():
            jobs.append(partial(_start_gateway, node, server))
        logger.fs.debug(f"[Dataplane.provision] Starting gateways on {len(jobs)} servers")
        do_parallel(lambda fn: fn(), jobs, n=-1, spinner=spinner, spinner_persist=spinner, desc="Starting gateway container on VMs")

    def deprovision(self, max_jobs: int = 64, spinner: bool = False):
        with self.provisioning_lock:
            if not self.provisioned:
                logger.warning("Attempting to deprovision dataplane that is not provisioned")
            # wait for tracker tasks
            for task in self.pending_transfers:
                logger.warning(f"[Dataplane.deprovision] Waiting for tracker task {task} to finish")
                task.join()
            self.provisioner.deprovision(
                max_jobs=max_jobs,
                spinner=spinner,
            )
            self.provisioned = False

    def auto_deprovision(self) -> DataplaneAutoDeprovision:
        """Returns a context manager that will automatically call deprovision upon exit."""
        return DataplaneAutoDeprovision(self)

    def source_gateways(self) -> List[Server]:
        return [self.bound_nodes[n] for n in self.topology.source_instances()] if self.provisioned else []

    def register_pending_transfer(self, tracker: TransferProgressTracker):
        self.pending_transfers.append(tracker)
