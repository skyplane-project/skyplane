import json
import os
import threading
from collections import defaultdict
from datetime import datetime
from functools import partial
from datetime import datetime

import nacl.secret
import nacl.utils
import urllib3
from pathlib import Path
from typing import TYPE_CHECKING, Dict, List, Optional

from skyplane import compute
from skyplane.exceptions import GatewayContainerStartException
from skyplane.api.tracker import TransferProgressTracker, TransferHook
from skyplane.api.transfer_job import TransferJob
from skyplane.api.config import TransferConfig
from skyplane.planner.topology import TopologyPlan, TopologyPlanGateway
from skyplane.utils import logger
from skyplane.utils.definitions import gateway_docker_image, tmp_log_dir
from skyplane.utils.fn import PathLike, do_parallel

if TYPE_CHECKING:
    from skyplane.api.provisioner import Provisioner


class DataplaneAutoDeprovision:
    def __init__(self, dataplane: "Dataplane"):
        self.dataplane = dataplane

    def __enter__(self):
        return self.dataplane

    def __exit__(self, exc_type, exc_value, exc_tb):
        logger.fs.warning("Deprovisioning dataplane")
        self.dataplane.deprovision()


class Dataplane:
    """A Dataplane represents a concrete Skyplane network, including topology and VMs."""

    def __init__(
        self,
        clientid: str,
        topology: TopologyPlan,
        provisioner: "Provisioner",
        transfer_config: TransferConfig,
        log_dir: str,
        debug: bool = True,
    ):
        """
        :param clientid: the uuid of the local host to create the dataplane
        :type clientid: str
        :param topology: the calculated topology during the transfer
        :type topology: TopologyPlan
        :param provisioner: the provisioner to launch the VMs
        :type provisioner: Provisioner
        :param transfer_config: the configuration during the transfer
        :type transfer_config: TransferConfig
        """
        self.clientid = clientid
        self.topology = topology
        self.provisioner = provisioner
        self.transfer_config = transfer_config
        # disable for azure
        # TODO: remove this
        self.http_pool = urllib3.PoolManager(retries=urllib3.Retry(total=3))
        self.provisioning_lock = threading.Lock()
        self.provisioned = False
        self.log_dir = Path(log_dir)
        self.transfer_dir = tmp_log_dir / "transfer_logs" / datetime.now().strftime("%Y%m%d_%H%M%S")
        self.transfer_dir.mkdir(exist_ok=True, parents=True)

        # transfer logs
        self.transfer_dir = tmp_log_dir / "transfer_logs" / datetime.now().strftime("%Y%m%d_%H%M%S")
        self.transfer_dir.mkdir(exist_ok=True, parents=True)
        self.debug = debug

        # pending tracker tasks
        self.pending_transfers: List[TransferProgressTracker] = []
        self.bound_nodes: Dict[TopologyPlanGateway, compute.Server] = {}

    def _start_gateway(
        self,
        gateway_docker_image: str,
        gateway_node: TopologyPlanGateway,
        gateway_server: compute.Server,
        gateway_log_dir: Optional[PathLike],
        authorize_ssh_pub_key: Optional[str] = None,
        e2ee_key_bytes: Optional[str] = None,
    ):
        # map outgoing ports
        setup_args = {}
        for gateway_id, n_conn in self.topology.get_outgoing_paths(gateway_node.gateway_id).items():
            node = self.topology.get_gateway(gateway_id)
            # use private ips for gcp to gcp connection
            src_provider, dst_provider = gateway_node.region.split(":")[0], node.region.split(":")[0]
            if src_provider == dst_provider and src_provider == "gcp":
                setup_args[self.bound_nodes[node].private_ip()] = n_conn
            else:
                setup_args[self.bound_nodes[node].public_ip()] = n_conn
        logger.fs.debug(f"[Dataplane._start_gateway] Setup args for {gateway_node}: {setup_args}")

        if gateway_log_dir:
            gateway_server.init_log_files(gateway_log_dir)
        if authorize_ssh_pub_key:
            gateway_server.copy_public_key(authorize_ssh_pub_key)

        # write gateway programs
        gateway_program_filename = Path(f"{gateway_log_dir}/gateway_program_{gateway_node.gateway_id}.json")
        with open(gateway_program_filename, "w") as f:
            f.write(gateway_node.gateway_program.to_json())

        # start gateway
        gateway_server.start_gateway(
            # setup_args,
            gateway_docker_image=gateway_docker_image,
            gateway_program_path=str(gateway_program_filename),
            gateway_info_path=f"{gateway_log_dir}/gateway_info.json",
            e2ee_key_bytes=e2ee_key_bytes,  # TODO: remove
            use_bbr=self.transfer_config.use_bbr,  # TODO: remove
            use_compression=self.transfer_config.use_compression,
            use_socket_tls=self.transfer_config.use_socket_tls,
        )

    def provision(
        self,
        allow_firewall: bool = True,
        gateway_docker_image: str = os.environ.get("SKYPLANE_DOCKER_IMAGE", gateway_docker_image()),
        authorize_ssh_pub_key: Optional[str] = None,
        max_jobs: int = 16,
        spinner: bool = False,
    ):
        """
        Provision the transfer gateways.

        :param allow_firewall: whether to apply firewall rules in the gatweway network (default: True)
        :type allow_firewall: bool
        :param gateway_docker_image: Docker image token in github
        :type gateway_docker_image: str
        :param authorize_ssh_pub_key: authorization ssh key to the remote gateways
        :type authorize_ssh_pub_key: str
        :param max_jobs: maximum number of provision jobs to launch concurrently (default: 16)
        :type max_jobs: int
        :param spinner: whether to show the spinner during the job (default: False)its to determine how many instances to create in each region
        :type spinner: bool
        """
        # TODO: support on-sided transfers but not requiring VMs to be created in source/destination regions
        with self.provisioning_lock:
            if self.provisioned:
                logger.error("Cannot provision dataplane, already provisioned!")
                return
            is_aws_used = any(n.region_tag.startswith("aws:") for n in self.topology.get_gateways())
            is_azure_used = any(n.region_tag.startswith("azure:") for n in self.topology.get_gateways())
            is_gcp_used = any(n.region_tag.startswith("gcp:") for n in self.topology.get_gateways())
            is_ibmcloud_used = any(n.region_tag.startswith("ibmcloud:") for n in self.topology.get_gateways())

            # create VMs from the topology
            for node in self.topology.get_gateways():
                cloud_provider, region = node.region_tag.split(":")
                assert (
                    cloud_provider != "cloudflare"
                ), f"Cannot create VMs in certain cloud providers: check planner output {self.topology.to_dict()}"
                self.provisioner.add_task(
                    cloud_provider=cloud_provider,
                    region=region,
                    vm_type=node.vm_type or getattr(self.transfer_config, f"{cloud_provider}_instance_class"),
                    spot=getattr(self.transfer_config, f"{cloud_provider}_use_spot_instances"),
                    autoterminate_minutes=self.transfer_config.autoterminate_minutes,
                )

            # initialize clouds
            self.provisioner.init_global(aws=is_aws_used, azure=is_azure_used, gcp=is_gcp_used, ibmcloud=is_ibmcloud_used)

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
            for node in self.topology.get_gateways():
                instance = servers_by_region[node.region_tag].pop()
                self.bound_nodes[node] = instance

                # set ip addresses (for gateway program generation)
                self.topology.set_ip_addresses(node.gateway_id, self.bound_nodes[node].private_ip(), self.bound_nodes[node].public_ip())

            logger.fs.debug(f"[Dataplane.provision] bound_nodes = {self.bound_nodes}")
            gateway_bound_nodes = self.bound_nodes.copy()

            # start gateways
            self.provisioned = True

        # todo: move server.py:start_gateway here
        logger.fs.info(f"Using docker image {gateway_docker_image}")
        e2ee_key_bytes = nacl.utils.random(nacl.secret.SecretBox.KEY_SIZE)

        # create gateway logging dir
        gateway_program_dir = f"{self.log_dir}/programs"
        Path(gateway_program_dir).mkdir(exist_ok=True, parents=True)
        logger.fs.info(f"Writing gateway programs to {gateway_program_dir}")

        # write gateway info file
        gateway_info_path = f"{gateway_program_dir}/gateway_info.json"
        with open(gateway_info_path, "w") as f:
            json.dump(self.topology.get_gateway_info_json(), f, indent=4)
        logger.fs.info(f"Writing gateway info to {gateway_info_path}")

        # start gateways in parallel
        jobs = []
        for node, server in gateway_bound_nodes.items():
            jobs.append(
                partial(self._start_gateway, gateway_docker_image, node, server, gateway_program_dir, authorize_ssh_pub_key, e2ee_key_bytes)
            )
        logger.fs.debug(f"[Dataplane.provision] Starting gateways on {len(jobs)} servers")
        try:
            do_parallel(lambda fn: fn(), jobs, n=-1, spinner=spinner, spinner_persist=spinner, desc="Starting gateway container on VMs")
        except Exception:
            self.copy_gateway_logs()
            raise GatewayContainerStartException(f"Error starting gateways. Please check gateway logs {self.transfer_dir}")

    def copy_gateway_logs(self):
        # copy logs from all gateways in parallel
        def copy_log(instance):
            out_file = self.transfer_dir / f"gateway_{instance.uuid()}.stdout"
            err_file = self.transfer_dir / f"gateway_{instance.uuid()}.stderr"
            logger.fs.info(f"[Dataplane.copy_gateway_logs] Copying logs from {instance.uuid()}: {out_file}")
            instance.run_command("sudo docker logs -t skyplane_gateway 2> /tmp/gateway.stderr > /tmp/gateway.stdout")
            instance.download_file("/tmp/gateway.stdout", out_file)
            instance.download_file("/tmp/gateway.stderr", err_file)

        do_parallel(copy_log, self.bound_nodes.values(), n=-1)

    def deprovision(self, max_jobs: int = 64, spinner: bool = False):
        """
        Deprovision the remote gateways

        :param max_jobs: maximum number of jobs to deprovision the remote gateways (default: 64)
        :type max_jobs: int
        :param spinner: Whether to show the spinner during the job (default: False)
        :type spinner: bool
        """
        with self.provisioning_lock:
            if self.debug and self.provisioned:
                logger.fs.info(f"Copying gateway logs to {self.transfer_dir}")
                self.copy_gateway_logs()

            if not self.provisioned:
                logger.fs.warning("Attempting to deprovision dataplane that is not provisioned, this may be from auto_deprovision.")
            # wait for tracker tasks
            try:
                for task in self.pending_transfers:
                    logger.fs.warning(f"Before deprovisioning, waiting for jobs to finish: {list(task.jobs.keys())}")
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
        """Get the error log from remote gateways if there is any error."""

        def get_error_logs(args):
            _, instance = args
            reply = self.http_pool.request("GET", f"{instance.gateway_api_url}/api/v1/errors")
            if reply.status != 200:
                raise Exception(f"Failed to get error logs from gateway instance {instance.instance_name()}: {reply.data.decode('utf-8')}")
            return json.loads(reply.data.decode("utf-8"))["errors"]

        errors: Dict[str, List[str]] = {}
        for (_, instance), result in do_parallel(get_error_logs, self.bound_nodes.items(), n=8):
            errors[instance] = result
        return errors

    def auto_deprovision(self) -> DataplaneAutoDeprovision:
        """Returns a context manager that will automatically call deprovision upon exit."""
        return DataplaneAutoDeprovision(self)

    def source_gateways(self) -> List[compute.Server]:
        """Returns a list of source gateway nodes"""
        return [self.bound_nodes[n] for n in self.topology.source_instances()] if self.provisioned else []

    def sink_gateways(self, region_tag: Optional[str] = None) -> Dict[str, List[compute.Server]]:
        """Returns a list of sink gateway nodes"""
        return (
            {region: [self.bound_nodes[n] for n in nodes] for region, nodes in self.topology.sink_instances(region_tag).items()}
            if self.provisioned
            else {}
        )

    def run_async(self, jobs: List[TransferJob], hooks: Optional[TransferHook] = None) -> TransferProgressTracker:
        """Start the transfer asynchronously. The main thread will not be blocked.

        :param hooks: Tracks the status of the transfer
        :type hooks: TransferHook
        """
        if not self.provisioned:
            logger.error("Dataplane must be pre-provisioned. Call dataplane.provision() before starting a transfer")
        tracker = TransferProgressTracker(self, jobs, self.transfer_config, hooks)
        self.pending_transfers.append(tracker)
        tracker.start()
        logger.fs.info(f"[SkyplaneClient] Started async transfer with {len(jobs)} jobs")
        return tracker

    def run(self, jobs: List[TransferJob], hooks: Optional[TransferHook] = None):
        """Start the transfer in the main thread. Wait until the transfer is complete.

        :param hooks: Tracks the status of the transfer
        :type hooks: TransferHook
        """
        tracker = self.run_async(jobs, hooks)
        logger.fs.debug(f"[SkyplaneClient] Waiting for transfer to complete")
        tracker.join()
