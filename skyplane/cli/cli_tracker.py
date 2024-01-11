from collections import defaultdict
from http import HTTPStatus
import logging
import os
import threading
from typing import Optional
import urllib3
from skyplane.api.usage import USAGE_STATS_FILE, get_clientid
from skyplane.cli.cli_transfer import SkyplaneCLI
from skyplane.gateway.gateway_tracker import TransferBody
import nacl.secret
import nacl.utils
from skyplane import compute
from skyplane.exceptions import TrackerVMException
from skyplane.planner.topology import TopologyPlanGateway
from skyplane.utils import logger
from skyplane.utils.fn import PathLike
from skyplane.config_paths import config_path
from skyplane.utils.definitions import tmp_log_dir
from pathlib import Path


TRACKER_URLS_DIR_PATH = config_path / "trackers"


class TrackerCLI:
    def __init__(
        self,
        cloud_provider: str,
        region: str,
        skyplane_cli: SkyplaneCLI,
    ) -> None:
        self.host_uuid = get_clientid()
        self.cloud_provider = cloud_provider
        self.region = region
        self.cli = skyplane_cli
        self._set_smallest_vm_type(cloud_provider)

        self.docker_image = os.environ.get("SKYPLANE_TRACKER_DOCKER_IMAGE", TrackerCLI._tracker_docker_image())
        self.provisioning_lock = threading.Lock()
        self.provisioned = False

    @property
    def http_pool(self):
        """http connection pool"""
        if not hasattr(self, "_http_pool"):
            timeout = urllib3.util.Timeout(connect=10.0, read=None)  # no read timeout
            self._http_pool = urllib3.PoolManager(retries=urllib3.Retry(total=3), timeout=timeout)
        return self._http_pool

    def provision_and_start_transfer(
        self,
        transfer_body: TransferBody,
        allow_firewall: bool = True,
        authorize_ssh_pub_key: Optional[str] = None,
        max_jobs: int = 16,
        spinner: bool = False,
    ) -> Optional[str]:
        """Provision a tracker gateway and start the transfer"""
        with self.provisioning_lock:
            if self.provisioned:
                logger.error("Cannot provision tracker gateway, already provisioned!")
                return
            self.cli.client.provisioner.add_task(
                cloud_provider=self.cloud_provider,
                region=self.region,
                vm_type=self.vm_type or getattr(self.cli.transfer_config, f"{self.cloud_provider}_instance_class"),
                spot=getattr(self.cli.transfer_config, f"{self.cloud_provider}_use_spot_instances"),
                autoterminate_minutes=self.cli.transfer_config.autoterminate_minutes,
            )
            self._init_cloud()

            uuid = self.cli.client.provisioner.provision(
                authorize_firewall=allow_firewall,
                max_jobs=max_jobs,
                spinner=spinner,
            )[0]

            node = TopologyPlanGateway(region_tag=self.region, gateway_id=uuid, gateway_vm=self.vm_type)
            server = self.cli.client.provisioner.get_node(uuid)
            self.provisioned = True

        # Start the tracker gateway
        e2ee_key_bytes = nacl.utils.random(nacl.secret.SecretBox.KEY_SIZE)
        try:
            logging.info("Starting tracker gateway container")
            self._start_tracker_gateway(self.docker_image, node, server, None, authorize_ssh_pub_key, e2ee_key_bytes)  # type: ignore
        except Exception as e:
            raise TrackerVMException(f"Error starting the tracker gateway.")

        # Start the transfer
        return self._start_transfer(transfer_body, server.gateway_api_url)

    def _start_transfer(self, transfer_body: TransferBody, tracker_api_url: Optional[str]) -> str:
        """Starts the transfer on the tracker gateway."""
        if tracker_api_url is None:
            raise TrackerVMException("Tracker API URL is not set. Please provision the tracker gateway first.")

        response = self.http_pool.request(
            "POST", tracker_api_url + "/start", body=transfer_body.model_dump(), headers={"Content-Type": "application/json"}
        )
        if response.status != HTTPStatus.OK:
            raise TrackerVMException(
                f"Error starting the transfer on the tracker gateway. Status code: {response.status}. Reason: {response.reason}."
            )

        data = response.json()
        transfer_id = data.get("transfer_id")
        if transfer_id is None:
            raise TrackerVMException(
                f"Error getting the transfer_id from the tracker gateway. Status code: {response.status}. Reason: {response.reason}."
            )
        TrackerCLI._save_tracker_api_url(transfer_id, tracker_api_url)
        return transfer_id

    @staticmethod
    def cancel_transfer(transfer_id: str) -> None:
        """Cancel the transfer on the tracker gateway."""
        tracker_api_url = TrackerCLI._retrieve_tracker_api_url(transfer_id)
        if tracker_api_url is None:
            raise TrackerVMException("Tracker API URL is not set. Please provision the tracker gateway first.")

        http_pool = urllib3.PoolManager(retries=urllib3.Retry(total=3), timeout=urllib3.util.Timeout(connect=10.0, read=None))
        response = http_pool.request("POST", tracker_api_url + "/cancel", headers={"Content-Type": "application/json"})
        if response.status != HTTPStatus.OK:
            raise TrackerVMException(
                f"Error canceling the transfer {transfer_id} on the tracker gateway. Status code: {response.status}. Reason: {response.reason}."
            )

        TrackerCLI._delete_tracker_api_url(transfer_id)

    @staticmethod
    def transfer_status(transfer_id: str) -> str:
        """Get the status of the transfer on the tracker gateway."""
        tracker_api_url = TrackerCLI._retrieve_tracker_api_url(transfer_id)
        if tracker_api_url is None:
            raise TrackerVMException("Tracker API URL is not set. Please provision the tracker gateway first.")

        http_pool = urllib3.PoolManager(retries=urllib3.Retry(total=3), timeout=urllib3.util.Timeout(connect=10.0, read=None))
        response = http_pool.request("GET", tracker_api_url + "/status", headers={"Content-Type": "application/json"})
        if response.status != HTTPStatus.OK:
            # TODO: The transfer might have already finished. So query the status from the special file in source bucket
            # and return the status from there if it exists. Else it means that the transfer errored out.

            raise TrackerVMException(
                f"Error getting the status of the transfer {transfer_id} on the tracker gateway. Status code: {response.status}. Reason: {response.reason}."
            )

        data = response.json()
        status = data.get("status")
        logs = data.get("logs")

        # Save the status to the local status logs
        if status is None:
            raise TrackerVMException(
                f"Error getting the status of the transfer {transfer_id} on the tracker gateway. Status code: {response.status}. Reason: {response.reason}."
            )
        usage_dir_path = tmp_log_dir / "usage" / get_clientid() / transfer_id
        usage_dir_path = Path(usage_dir_path)
        usage_dir_path.mkdir(exist_ok=True, parents=True)
        usage_stat_file = usage_dir_path / USAGE_STATS_FILE
        with open(usage_stat_file, "w") as f:
            f.write(status)

        # Save the logs to the transfer client logs
        if logs is not None:
            log_file = tmp_log_dir / "transfer_logs" / transfer_id / "client.log"
            if log_file.exists():
                with open(log_file, "w") as f:
                    f.write(logs)
        return status

    def _start_tracker_gateway(
        self,
        gateway_docker_image: str,
        gateway_node: TopologyPlanGateway,
        gateway_server: compute.Server,
        gateway_log_dir: Optional[PathLike],
        authorize_ssh_pub_key: Optional[str] = None,
        e2ee_key_bytes: Optional[str] = None,
        container_name: Optional[str] = "skyplane_tracker",
        port: Optional[int] = 8081,
    ):
        """Starts the tracker gateway container on the tracker VM"""
        # TODO: Implement
        pass

    def _init_cloud(self) -> None:
        """Initialize the tracker cloud providers by configuring with credentials."""
        logger.fs.info(f"[TrackerCLI._init_CLI] Initializing cloud resources in {self.cloud_provider}:{self.region} for the tracker VM")
        providers = defaultdict(bool)
        providers[self.cloud_provider] = True
        self.cli.client.provisioner.init_global(
            aws=providers["aws"], azure=providers["azure"], gcp=providers["gcp"], ibmcloud=providers["ibmcloud"]
        )
        return None

    def _set_smallest_vm_type(self, cloud_provider: str) -> None:
        if cloud_provider == "aws":
            vm_type = "m5.xlarge"
        elif cloud_provider == "azure":
            vm_type = "Standard_D2_v5"
        elif cloud_provider == "gcp":
            vm_type = "n2-standard-2"
        elif cloud_provider == "ibmcloud":
            vm_type = "bx2-2x8"
        else:
            raise ValueError(f"Unknown cloud provider {cloud_provider}")
        self.vm_type = vm_type

    @staticmethod
    def _save_tracker_api_url(transfer_id: str, tracker_api_url: str) -> None:
        """Saves the tracker api url to a file named after the transfer_id in the tracker_urls directory."""
        TRACKER_URLS_DIR_PATH.mkdir(parents=True, exist_ok=True)
        with open(TRACKER_URLS_DIR_PATH / f"{transfer_id}", "w") as f:
            f.write(tracker_api_url)

    @staticmethod
    def _delete_tracker_api_url(transfer_id: str) -> None:
        """Deletes the tracker api url from the tracker_urls directory."""
        url_file = TRACKER_URLS_DIR_PATH / f"{transfer_id}"
        if not url_file.exists():
            return None
        url_file.unlink()

    @staticmethod
    def _retrieve_tracker_api_url(transfer_id: str) -> Optional[str]:
        """Retrieves the tracker api url from the tracker_urls directory."""
        url_file = TRACKER_URLS_DIR_PATH / f"{transfer_id}"
        if not url_file.exists():
            return None
        with open(url_file, "r") as f:
            return f.read()

    @staticmethod
    def _tracker_docker_image() -> str:
        # TODO: Get the docker image like definitions::gateway_docker_image
        return ""
