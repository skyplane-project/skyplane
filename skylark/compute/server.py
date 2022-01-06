import json
import os
import subprocess
import threading
import time
import uuid
from enum import Enum, auto
from pathlib import Path, PurePath

from loguru import logger
import requests

from skylark.utils.utils import PathLike, Timer, do_parallel, wait_for


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
        self.region_tag = region_tag
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
        return self.region_tag.split(":")[0]

    def instance_state(self) -> ServerState:
        raise NotImplementedError()

    def public_ip(self):
        raise NotImplementedError()

    def instance_class(self):
        raise NotImplementedError()

    def region(self):
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
            except Exception as e:
                return False
            if ip is not None:
                cmd = ["nc", "-zvw1", str(ip), "22"]
                ping_return = subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                return ping_return.returncode == 0
            return False

        wait_for(is_up, timeout=timeout, interval=interval, progress_bar=True, desc=f"Wait for {self.uuid()} to be ready", leave_pbar=False)

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

    def copy_file(self, local_file, remote_file):
        """Copy local file to remote file."""
        client = self.ssh_client
        sftp = client.open_sftp()
        with Timer() as t:
            sftp.put(local_file, remote_file)
        self.add_command_log(command=f"<copy_file> {local_file} {remote_file}", runtime=t.elapsed)
        sftp.close()

    def copy_and_run_script(self, local_file):
        """Copy local file to remote file and run command."""
        tmp_dest_file = Path("/tmp") / f"{uuid.uuid4()}_{Path(local_file).name}"
        self.copy_file(local_file, str(tmp_dest_file))
        self.run_command(f"chmod +x {tmp_dest_file}")
        stdout, stderr = self.run_command(f"{tmp_dest_file}")
        return stdout, stderr

    def sync_directory(self, local_dir, remote_dir, delete_remote=False, ignore_globs=()):
        """Copy local directory to remote directory. If remote directory exists, delete if delete_remote else raise exception."""

        local_path = Path(local_dir)
        remote_path = PurePath(remote_dir)
        ignored_paths = [Path(local_dir).glob(pattern) for pattern in ignore_globs]
        ignored_paths = set([path.relative_to(local_dir) for path_list in ignored_paths for path in path_list])

        def copy_file(sftp, local_file: Path, remote_file: Path):
            with Timer() as t:
                sftp.put(str(local_file), str(remote_file))
            self.add_command_log(command=f"<copy_file> {local_file} {remote_file}", runtime=t.elapsed)

        client = self.ssh_client
        sftp = client.open_sftp()
        with Timer() as t:
            if delete_remote:
                self.run_command(f"rm -rf {remote_dir}")
            self.run_command(f"mkdir -p {remote_dir}")

            # make all directories
            recursive_dir_list = [x.relative_to(local_dir) for x in Path(local_dir).glob("**/*/") if x.is_dir()]
            recursive_dir_list = [x for x in recursive_dir_list if x not in ignored_paths]
            do_parallel(
                lambda x: self.run_command(f"mkdir -p {remote_dir}/{x}"),
                recursive_dir_list,
            )

            # copy all files
            file_list = [x.relative_to(local_dir) for x in Path(local_dir).glob("**/*") if x.is_file()]
            file_list = [x for x in file_list if x not in ignored_paths]
            do_parallel(
                lambda x: copy_file(sftp, local_path / x, remote_path / x),
                file_list,
                progress_bar=True,
                n=1,
            )
        sftp.close()
        self.add_command_log(command=f"<sync_directory> {local_dir} {remote_dir}", runtime=t.elapsed)

    def copy_public_key(self, pub_key_path: PathLike):
        """Append public key to authorized_keys file on server."""
        pub_key_path = Path(pub_key_path)
        assert pub_key_path.suffix == ".pub", f"{pub_key_path} does not have .pub extension, are you sure it is a public key?"
        pub_key = Path(pub_key_path).read_text()
        self.run_command(f"mkdir -p ~/.ssh && (echo '{pub_key}' >> ~/.ssh/authorized_keys) && chmod 600 ~/.ssh/authorized_keys")

    def start_gateway(
        self, gateway_docker_image="ghcr.io/parasj/skylark:main", log_viewer_port=8888, glances_port=8889, num_outgoing_connections=8
    ):
        self.wait_for_ready()

        # install docker and launch monitoring
        cmd = "(command -v docker >/dev/null 2>&1 || { rm -rf get-docker.sh; curl -fsSL https://get.docker.com -o get-docker.sh && sudo sh get-docker.sh; }); "
        cmd += "{ sudo docker stop $(docker ps -a -q); sudo docker kill $(sudo docker ps -a -q); sudo docker rm -f $(sudo docker ps -a -q); }; "
        cmd += f"sudo docker run --name dozzle -d --volume=/var/run/docker.sock:/var/run/docker.sock -p {log_viewer_port}:8080 amir20/dozzle:latest --filter name=skylark_gateway; "
        cmd += f"sudo docker run --name glances -d -p {glances_port}-{glances_port + 1}:{glances_port}-{glances_port + 1} -e GLANCES_OPT='-w' -v /var/run/docker.sock:/var/run/docker.sock:ro --pid host nicolargo/glances:latest-full; "
        cmd += f"(docker --version && echo 'Success, Docker installed' || echo 'Failed to install Docker'); "
        out, err = self.run_command(cmd)
        docker_version = out.strip().split("\n")[-1]
        assert docker_version.startswith("Success"), f"Failed to install Docker: {out}\n{err}"

        # launch gateway
        docker_out, docker_err = self.run_command(f"sudo docker pull {gateway_docker_image}")
        assert "Status: Downloaded newer image" in docker_out or "Status: Image is up to date" in docker_out, (docker_out, docker_err)

        # todo add other launch flags for gateway daemon
        docker_run_flags = "-d --log-driver=local --ipc=host --network=host"
        gateway_daemon_cmd = f"/env/bin/python /pkg/skylark/gateway/gateway_daemon.py --debug --chunk-dir /dev/shm/skylark/chunks --outgoing-connections {num_outgoing_connections}"
        docker_launch_cmd = f"sudo docker run {docker_run_flags} --name skylark_gateway {gateway_docker_image} {gateway_daemon_cmd}"
        start_out, start_err = self.run_command(docker_launch_cmd)
        assert not start_err, f"Error starting gateway: {start_err}"

        # wait for gateways to start (check status API)
        def is_ready():
            api_url = f"http://{self.public_ip()}:8080/api/v1/status"
            try:
                status_val = requests.get(api_url)
                is_up = status_val.json().get("status") == "ok"
                return is_up
            except Exception as e:
                return False

        try:
            wait_for(is_ready, timeout=10, interval=0.1)
        except Exception as e:
            logger.error(f"Gateway {self.instance_name()} is not ready")
            logs, err = self.run_command(f"sudo docker logs skylark_gateway --tail=100")
            logger.error(f"Docker logs: {logs}\nerr: {err}")
            raise e
