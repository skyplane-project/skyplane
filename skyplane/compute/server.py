import json
import os
import logging
import socket
from contextlib import closing
from enum import Enum, auto
from functools import partial
from pathlib import Path
from typing import Dict, Optional, Tuple

import urllib3
from skyplane import config_path, key_root, cloud_config
from skyplane.compute.const_cmds import make_autoshutdown_script, make_dozzle_command, make_sysctl_tcp_tuning_command
from skyplane.utils import logger
from skyplane.utils.fn import PathLike, wait_for
from skyplane.utils.retry import retry_backoff
from skyplane.utils.timer import Timer


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

    def __init__(self, region_tag, log_dir=None, auto_shutdown_timeout_minutes: Optional[int] = None):
        self.region_tag = region_tag  # format provider:region
        self.auto_shutdown_timeout_minutes = auto_shutdown_timeout_minutes
        self.command_log = []
        self.gateway_log_viewer_url = None
        self.gateway_api_url = None
        self.init_log_files(log_dir)
        self.ssh_tunnels: Dict = {}

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

    def get_sftp_client(self):
        raise NotImplementedError()

    def get_ssh_client_impl(self):
        raise NotImplementedError()

    def open_ssh_tunnel_impl(self, remote_port):
        raise NotImplementedError()

    def get_ssh_cmd(self) -> str:
        raise NotImplementedError()

    @property
    def ssh_client(self):
        """Create SSH client and cache."""
        if not hasattr(self, "_ssh_client"):
            self._ssh_client = self.get_ssh_client_impl()
        return self._ssh_client

    def tunnel_port(self, remote_port: int) -> int:
        """Returns a local port that tunnels to the remote port."""
        if remote_port not in self.ssh_tunnels:

            def start():
                tunnel = self.open_ssh_tunnel_impl(remote_port)
                tunnel.start()
                tunnel._check_is_started()
                return tunnel

            self.ssh_tunnels[remote_port] = retry_backoff(start)
        local_bind_port = self.ssh_tunnels[remote_port].local_bind_port
        logger.fs.debug(f"Bound remote port {self.uuid()}:{remote_port} to localhost:{local_bind_port}")
        return local_bind_port

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

    def enable_auto_shutdown(self, timeout_minutes=None):
        if timeout_minutes is None:
            timeout_minutes = cloud_config.get_flag("autoshutdown_minutes")
        self.auto_shutdown_timeout_minutes = timeout_minutes
        self.run_command(f"(echo '{make_autoshutdown_script()}' > /tmp/autoshutdown.sh) && chmod +x /tmp/autoshutdown.sh")
        self.run_command("echo 1")  # run noop to update auto_shutdown

    def disable_auto_shutdown(self):
        self.auto_shutdown_timeout_minutes = None
        self.run_command("(kill -9 $(cat /tmp/autoshutdown.pid) && rm -f /tmp/autoshutdown.pid) || true")

    def wait_for_ssh_ready(self, timeout=120, interval=0.25) -> bool:
        def is_up():
            try:
                ip = self.public_ip()
            except Exception:
                return False
            if ip is not None:
                with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
                    sock.settimeout(2)
                    return sock.connect_ex((ip, 22)) == 0
            return False

        try:
            wait_for(is_up, timeout=timeout, interval=interval, desc=f"Waiting for {self.uuid()} to be ready")
        except TimeoutError:
            logger.error(f"Gateway {self.uuid()} is not ready after {timeout} seconds, run `skyplane deprovision` to clean up resources")
            raise TimeoutError(f"{self.uuid()} is not ready after {timeout} seconds")

    def close_server(self):
        if hasattr(self, "_ssh_client"):
            self._ssh_client.close()
        for tunnel in self.ssh_tunnels.values():
            tunnel.stop()
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

    def run_command(self, command) -> Tuple[str, str]:
        client = self.ssh_client
        with Timer() as t:
            if self.auto_shutdown_timeout_minutes:
                command = f"(nohup /tmp/autoshutdown.sh {self.auto_shutdown_timeout_minutes} &> /dev/null < /dev/null); {command}"
            _, stdout, stderr = client.exec_command(command)
            stdout, stderr = (stdout.read().decode("utf-8"), stderr.read().decode("utf-8"))
        self.add_command_log(command=command, stdout=stdout, stderr=stderr, runtime=t.elapsed)
        return stdout, stderr

    def download_file(self, remote_path, local_path):
        """Download a file from the server"""
        sftp_client = self.get_sftp_client()
        sftp_client.get(remote_path, local_path)
        sftp_client.close()

    def upload_file(self, local_path, remote_path):
        """Upload a file to the server"""
        sftp_client = self.get_sftp_client()
        sftp_client.put(local_path, remote_path)
        sftp_client.close()

    def write_file(self, content_bytes, remote_path):
        """Write a file on the server"""
        sftp_client = self.get_sftp_client()
        with sftp_client.file(remote_path, mode="wb") as f:
            f.write(content_bytes)
        sftp_client.close()

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
        out, err = self.run_command(cmd)
        docker_version = out.strip().split("\n")[-1]
        if not docker_version.startswith("Success"):
            raise RuntimeError(f"Failed to install Docker on {self.region_tag}, {self.public_ip()}: OUT {out}\nERR {err}")

    def pull_docker(self, gateway_docker_image):
        docker_out, docker_err = self.run_command(f"sudo docker pull {gateway_docker_image}")
        if "Status: Downloaded newer image" not in docker_out and "Status: Image is up to date" not in docker_out:
            raise RuntimeError(
                f"Failed to pull docker image {gateway_docker_image} on {self.region_tag}, {self.public_ip()}: OUT {docker_out}\nERR {docker_err}"
            )

    def start_gateway(
        self,
        outgoing_ports: Dict[str, int],  # maps ip to number of connections along route
        gateway_docker_image: str,
        log_viewer_port=8888,
        use_bbr=False,
        use_compression=False,
        e2ee_key_bytes=None,
        use_socket_tls=False,
    ):
        def check_stderr(tup):
            assert tup[1].strip() == "", f"Command failed, err: {tup[1]}"

        desc_prefix = f"Starting gateway {self.uuid()}, host: {self.public_ip()}"

        # increase TCP connections, enable BBR optionally and raise file limits
        check_stderr(self.run_command(make_sysctl_tcp_tuning_command(cc="bbr" if use_bbr else "cubic")))
        retry_backoff(self.install_docker, exception_class=RuntimeError)

        # start log viewer
        self.run_command(make_dozzle_command(log_viewer_port))

        # copy cloud configuration
        docker_envs = {"SKYPLANE_IS_GATEWAY": "1"}
        if config_path.exists():
            self.upload_file(config_path, f"/tmp/{config_path.name}")
            docker_envs["SKYPLANE_CONFIG"] = f"/pkg/data/{config_path.name}"

        # fix issue 312, retry boto3 credential calls to instance metadata service
        if self.provider == "aws":
            docker_envs["AWS_METADATA_SERVICE_NUM_ATTEMPTS"] = "4"
            docker_envs["AWS_METADATA_SERVICE_TIMEOUT"] = "10"

        # pull docker image and start container
        with Timer() as t:
            retry_backoff(partial(self.pull_docker, gateway_docker_image), exception_class=RuntimeError)
        logger.fs.debug(f"{desc_prefix} docker pull in {t.elapsed}")
        logger.fs.debug(f"{desc_prefix}: Starting gateway container")
        docker_run_flags = f"-d --log-driver=local --log-opt max-file=16 --ipc=host --network=host --ulimit nofile={1024 * 1024}"
        docker_run_flags += " --mount type=tmpfs,dst=/skyplane,tmpfs-size=$(($(free -b  | head -n2 | tail -n1 | awk '{print $2}')/2))"
        docker_run_flags += f" -v /tmp/{config_path.name}:/pkg/data/{config_path.name}"

        # copy service account files
        if self.provider == "gcp":
            service_key_file = "service_account_key.json"
            self.upload_file(os.path.expanduser(f"{key_root}/gcp/{service_key_file}"), f"/tmp/{service_key_file}")
            docker_envs["GCP_SERVICE_ACCOUNT_FILE"] = f"/pkg/data/{service_key_file}"
            docker_run_flags += f" -v /tmp/{service_key_file}:/pkg/data/{service_key_file}"

        # set default region for boto3 on AWS
        if self.provider == "aws":
            docker_envs["AWS_DEFAULT_REGION"] = self.region_tag.split(":")[1]

        # copy E2EE keys
        if e2ee_key_bytes is not None:
            e2ee_key_file = "e2ee_key"
            self.write_file(e2ee_key_bytes, f"/tmp/{e2ee_key_file}")
            docker_envs["E2EE_KEY_FILE"] = f"/pkg/data/{e2ee_key_file}"
            docker_run_flags += f" -v /tmp/{e2ee_key_file}:/pkg/data/{e2ee_key_file}"

        docker_run_flags += " " + " ".join(f"--env {k}={v}" for k, v in docker_envs.items())
        gateway_daemon_cmd = f"/etc/init.d/stunnel4 start && python -u /pkg/skyplane/gateway/gateway_daemon.py --chunk-dir /skyplane/chunks"
        gateway_daemon_cmd += f" --outgoing-ports '{json.dumps(outgoing_ports)}'"
        gateway_daemon_cmd += f" --region {self.region_tag} {'--use-compression' if use_compression else ''}"
        gateway_daemon_cmd += f" {'--disable-e2ee' if e2ee_key_bytes is None else ''}"
        gateway_daemon_cmd += f" {'--disable-tls' if not use_socket_tls else ''}"
        escaped_gateway_daemon_cmd = gateway_daemon_cmd.replace('"', '\\"')
        docker_launch_cmd = (
            f'sudo docker run {docker_run_flags} --name skyplane_gateway {gateway_docker_image} /bin/bash -c "{escaped_gateway_daemon_cmd}"'
        )
        logger.fs.info(f"{desc_prefix}: {docker_launch_cmd}")
        start_out, start_err = self.run_command(docker_launch_cmd)
        logger.fs.debug(desc_prefix + f": Gateway started {start_out.strip()}")
        assert not start_err.strip(), f"Error starting gateway:\n{start_out.strip()}\n{start_err.strip()}"

        gateway_container_hash = start_out.strip().split("\n")[-1][:12]
        self.gateway_log_viewer_url = f"http://127.0.0.1:{self.tunnel_port(8888)}/container/{gateway_container_hash}"
        logger.fs.debug(f"{self.uuid()} log_viewer_url = {self.gateway_log_viewer_url}")
        self.gateway_api_url = f"http://127.0.0.1:{self.tunnel_port(8080 + 1)}"
        logger.fs.debug(f"{self.uuid()} gateway_api_url = {self.gateway_api_url}")

        # wait for gateways to start (check status API)
        http_pool = urllib3.PoolManager()

        def is_api_ready():
            try:
                api_url = f"{self.gateway_api_url}/api/v1/status"
                status_val = json.loads(http_pool.request("GET", api_url).data.decode("utf-8"))
                is_up = status_val.get("status") == "ok"
                return is_up
            except Exception:
                return False

        try:
            logging.disable(logging.CRITICAL)
            wait_for(is_api_ready, timeout=30, interval=0.1, desc=f"Waiting for gateway {self.uuid()} to start")
        except TimeoutError as e:
            logger.fs.error(f"Gateway {self.instance_name()} is not ready {e}")
            logger.fs.warning(desc_prefix + " gateway launch command: " + docker_launch_cmd)
            logs, err = self.run_command(f"sudo docker logs skyplane_gateway --tail=100")
            logger.fs.error(f"Docker logs: {logs}\nerr: {err}")
            logger.fs.exception(e)
            raise e
        finally:
            logging.disable(logging.NOTSET)
