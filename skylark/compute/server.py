import json
import time
import os
import subprocess
import threading
from enum import Enum, auto
from pathlib import Path
from typing import Dict

import requests
from skylark.utils import logger
from skylark.compute.utils import make_dozzle_command, make_sysctl_tcp_tuning_command
from skylark.utils.utils import PathLike, Timer, wait_for

import configparser
import os


class ServerState(Enum):
    PENDING = auto()
    RUNNING = auto()
    SUSPENDED = auto()
    TERMINATED = auto()
    UNKNOWN = auto()

    def __str__(self):
        return self.name.lower()

    @staticmethod
    def from_gcp_state(gcp_state):
        mapping = {
            "PROVISIONING": ServerState.PENDING,
            "STAGING": ServerState.PENDING,
            "RUNNING": ServerState.RUNNING,
            "REPAIRING": ServerState.RUNNING,
            "SUSPENDING": ServerState.SUSPENDED,
            "SUSPENDED": ServerState.SUSPENDED,
            "STOPPING": ServerState.TERMINATED,
            "TERMINATED": ServerState.TERMINATED,
        }
        return mapping.get(gcp_state, ServerState.UNKNOWN)

    @staticmethod
    def from_azure_state(azure_state):
        mapping = {
            "PowerState/starting": ServerState.PENDING,
            "PowerState/running": ServerState.RUNNING,
            "PowerState/stopping": ServerState.SUSPENDED,
            "PowerState/stopped": ServerState.SUSPENDED,
            "PowerState/deallocating": ServerState.TERMINATED,
            "PowerState/deallocated": ServerState.TERMINATED,
        }
        return mapping.get(azure_state, ServerState.UNKNOWN)

    @staticmethod
    def from_aws_state(aws_state):
        mapping = {
            "pending": ServerState.PENDING,
            "running": ServerState.RUNNING,
            "shutting-down": ServerState.TERMINATED,
            "terminated": ServerState.TERMINATED,
            "stopping": ServerState.SUSPENDED,
            "stopped": ServerState.SUSPENDED,
        }
        return mapping.get(aws_state, ServerState.UNKNOWN)


class Server:
    """Abstract server class to support basic SSH operations"""

    ns = threading.local()

    def __init__(self, region_tag, log_dir=None):
        self.region_tag = region_tag  # format provider:region
        self.command_log = []
        self.init_log_files(log_dir)

    def __repr__(self):
        return f"Server({self.uuid()})"

    def __hash__(self):
        return hash((self.region_tag, self.uuid()))

    def uuid(self):
        raise NotImplementedError()

    def init_log_files(self, log_dir):
        if log_dir:
            log_dir = Path(log_dir)
            log_dir.mkdir(parents=True, exist_ok=True)
            self.command_log_file = str(log_dir / f"{self.uuid()}.jsonl")
        else:
            self.command_log_file = None

    def get_ssh_client_impl(self):
        raise NotImplementedError()

    @property
    def ssh_client(self):
        """Create SSH client and cache (one connection per thread using threadlocal)"""
        if not hasattr(self, "client"):
            self.client = self.get_ssh_client_impl()
        return self.client

    @property
    def provider(self) -> str:
        """Format provider"""
        return self.region_tag.split(":")[0]

    def instance_state(self) -> ServerState:
        raise NotImplementedError()

    def public_ip(self):
        raise NotImplementedError()

    def instance_class(self):
        raise NotImplementedError()

    def region(self):
        """Per-provider region e.g. us-east-1"""
        raise NotImplementedError()

    def instance_name(self):
        raise NotImplementedError()

    def tags(self):
        raise NotImplementedError()

    def network_tier(self):
        raise NotImplementedError()

    def terminate_instance_impl(self):
        raise NotImplementedError()

    def terminate_instance(self):
        """Terminate instance"""
        self.close_server()
        self.terminate_instance_impl()

    def wait_for_ready(self, timeout=120, interval=0.25) -> bool:
        def is_up():
            try:
                ip = self.public_ip()
            except Exception:
                return False
            if ip is not None:
                cmd = ["nc", "-zvw1", str(ip), "22"]
                ping_return = subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                return ping_return.returncode == 0
            return False

        logger.debug(f"Waiting for {self.uuid()} to be ready")
        wait_for(is_up, timeout=timeout, interval=interval, desc=f"Waiting for {self.uuid()} to be ready")
        logger.debug(f"{self.uuid()} is ready")

    def close_server(self):
        if hasattr(self.ns, "client"):
            self.ns.client.close()
            del self.ns.client
        self.flush_command_log()

    def flush_command_log(self):
        if self.command_log_file and len(self.command_log) > 0:
            with open(self.command_log_file, "a") as f:
                for log_item in self.command_log:
                    f.write(json.dumps(log_item) + "\n")
            self.command_log = []

    def add_command_log(self, command, runtime=None, **kwargs):
        self.command_log.append(dict(command=command, runtime=runtime, **kwargs))
        self.flush_command_log()

    def run_command(self, command):
        """time command and run it"""
        client = self.ssh_client
        with Timer() as t:
            _, stdout, stderr = client.exec_command(command)
            stdout, stderr = (stdout.read().decode("utf-8"), stderr.read().decode("utf-8"))
        self.add_command_log(command=command, stdout=stdout, stderr=stderr, runtime=t.elapsed)
        return stdout, stderr

    def copy_public_key(self, pub_key_path: PathLike):
        """Append public key to authorized_keys file on server."""
        pub_key_path = Path(pub_key_path)
        assert pub_key_path.suffix == ".pub", f"{pub_key_path} does not have .pub extension, are you sure it is a public key?"
        pub_key = Path(pub_key_path).read_text()
        self.run_command(f"mkdir -p ~/.ssh && (echo '{pub_key}' >> ~/.ssh/authorized_keys) && chmod 600 ~/.ssh/authorized_keys")

    def install_docker(self):
        cmd = "(command -v docker >/dev/null 2>&1 || { rm -rf get-docker.sh; curl -fsSL https://get.docker.com -o get-docker.sh && sudo sh get-docker.sh; }); "
        cmd += "{ sudo docker stop $(docker ps -a -q); sudo docker kill $(sudo docker ps -a -q); sudo docker rm -f $(sudo docker ps -a -q); }; "
        cmd += f"(docker --version && echo 'Success, Docker installed' || echo 'Failed to install Docker'); "
        for i in range(4):
            out, err = self.run_command(cmd)
            docker_version = out.strip().split("\n")[-1]
            if not docker_version.startswith("Success"):  # retry since docker install fails sometimes
                logger.error(f"Docker install failed, retrying! (attempt {i}): {out} {err}")
                out, err = self.run_command(cmd)
                docker_version = out.strip().split("\n")[-1]
            else:
                if not docker_version.startswith("Success"):
                    raise Exception(f"Failed to install Docker on {self.region_tag}, {self.public_ip()}: OUT {out}\nERR {err}")
                else:
                    return

    def start_gateway(
        self,
        outgoing_ports: Dict[str, int],  # maps ip to number of connections along route
        gateway_docker_image="ghcr.io/parasj/skylark:main",
        log_viewer_port=8888,
        use_bbr=False,
    ):
        self.wait_for_ready()
        time.sleep(2)

        def check_stderr(tup):
            assert tup[1].strip() == "", f"Command failed, err: {tup[1]}"

        desc_prefix = f"Starting gateway {self.uuid()}, host: {self.public_ip()}"

        # increase TCP connections, enable BBR optionally and raise file limits
        self.run_command("sudo /sbin/iptables -A INPUT -j ACCEPT")
        check_stderr(self.run_command(make_sysctl_tcp_tuning_command(cc="bbr" if use_bbr else "cubic")))
        self.install_docker()
        self.run_command(make_dozzle_command(log_viewer_port))

        # read AWS config file to get credentials
        # TODO: Integrate this with updated skylark config file
        docker_envs = ""
        try:
            config = configparser.RawConfigParser()
            config.read(os.path.expanduser("~/.aws/credentials"))
            aws_access_key_id = config.get("default", "aws_access_key_id")
            aws_secret_access_key = config.get("default", "aws_secret_access_key")
            docker_envs += f" -e AWS_ACCESS_KEY_ID='{aws_access_key_id}'"
            docker_envs += f" -e AWS_SECRET_ACCESS_KEY='{aws_secret_access_key}'"
        except Exception as e:
            logger.error(f"Failed to read AWS credentials locally {e}")

        with Timer(f"{desc_prefix}: Docker pull"):
            docker_out, docker_err = self.run_command(f"sudo docker pull {gateway_docker_image}")
            assert "Status: Downloaded newer image" in docker_out or "Status: Image is up to date" in docker_out, (docker_out, docker_err)
        logger.debug(f"{desc_prefix}: Starting gateway container")
        docker_run_flags = f"-d --rm --log-driver=local --ipc=host --network=host --ulimit nofile={1024 * 1024} {docker_envs}"
        gateway_daemon_cmd = f"python -u /pkg/skylark/gateway/gateway_daemon.py --chunk-dir /dev/shm/skylark/chunks --outgoing-ports '{json.dumps(outgoing_ports)}' --region {self.region_tag}"
        docker_launch_cmd = f"sudo docker run {docker_run_flags} --name skylark_gateway {gateway_docker_image} {gateway_daemon_cmd}"
        start_out, start_err = self.run_command(docker_launch_cmd)
        logger.debug(desc_prefix + f": Gateway started {start_out.strip()}")
        assert not start_err.strip(), f"Error starting gateway: {start_err.strip()}"

        # load URLs
        gateway_container_hash = start_out.strip().split("\n")[-1][:12]
        self.gateway_api_url = f"http://{self.public_ip()}:8080/api/v1"
        self.gateway_log_viewer_url = f"http://{self.public_ip()}:8888/container/{gateway_container_hash}"
        self.gateway_htop_url = f"http://{self.public_ip()}:8889"

        # wait for gateways to start (check status API)
        def is_ready():
            api_url = f"http://{self.public_ip()}:8080/api/v1/status"
            try:
                status_val = requests.get(api_url)
                is_up = status_val.json().get("status") == "ok"
                return is_up
            except Exception:
                return False

        try:
            wait_for(is_ready, timeout=10, interval=0.1, desc=f"Waiting for gateway {self.uuid()} to start", leave_pbar=False)
        except Exception as e:
            logger.error(f"Gateway {self.instance_name()} is not ready {e}")
            logger.warning(desc_prefix + " gateway launch command: " + docker_launch_cmd)
            logs, err = self.run_command(f"sudo docker logs skylark_gateway --tail=100")
            logger.error(f"Docker logs: {logs}\nerr: {err}")
            raise e
