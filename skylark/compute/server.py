import json
import threading
import time
import uuid
from enum import Enum
from pathlib import Path, PurePath

from loguru import logger

from skylark.utils import Timer, do_parallel


class ServerState(Enum):
    PENDING = 0
    RUNNING = 1
    SUSPENDED = 2
    TERMINATED = 3
    UNKNOWN = 4

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
        return mapping[gcp_state]

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
        return mapping[aws_state]


class Server:
    """Abstract server class to support basic SSH operations"""

    ns = threading.local()

    def __init__(self, region_tag, log_dir=None):
        self.region_tag = region_tag
        self.command_log = []
        self.init_log_files(log_dir)

    def __repr__(self):
        return f"Server({self.uuid()})"

    def uuid(self):
        raise NotImplementedError()

    def init_log_files(self, log_dir):
        if log_dir:
            log_dir = Path(log_dir)
            self.command_log_file = str(log_dir / f"{self.uuid()}_command_log.json")
            self.stdout_log_file = str(log_dir / f"{self.uuid()}_stdout.log")
            self.stderr_log_file = str(log_dir / f"{self.uuid()}_stderr.log")
        else:
            self.command_log_file = None
            self.stdout_log_file = None
            self.stderr_log_file = None

    def get_ssh_client_impl(self):
        raise NotImplementedError()

    @property
    def ssh_client(self):
        """Create SSH client and cache (one connection per thread using threadlocal)"""
        if not hasattr(self.ns, "client"):
            self.ns.client = self.get_ssh_client_impl()
        return self.ns.client

    @property
    def instance_state(self) -> ServerState:
        raise NotImplementedError()

    @property
    def public_ip(self):
        raise NotImplementedError()

    @property
    def instance_class(self):
        raise NotImplementedError()

    @property
    def region(self):
        raise NotImplementedError()

    @property
    def instance_name(self):
        raise NotImplementedError()

    @property
    def tags(self):
        raise NotImplementedError()

    def terminate_instance_impl(self):
        raise NotImplementedError()

    def terminate_instance(self):
        """Terminate instance"""
        self.close_server()
        self.terminate_instance_impl()

    def wait_for_ready(self, timeout=120) -> bool:
        wait_intervals = [0.2] * 20 + [1.0] * int(timeout / 2) + [5.0] * int(timeout / 2)  # backoff
        start_time = time.time()
        while (time.time() - start_time) < timeout:
            try:
                if self.instance_state == ServerState.RUNNING:
                    return True
                time.sleep(wait_intervals.pop(0))
            except Exception as e:
                print(f"Error waiting for server to be ready: {e}")
                continue
        logger.warning(f"({self.region_tag}) Timeout waiting for server to be ready")
        return False

    def close_server(self):
        if hasattr(self.ns, "client"):
            self.ns.client.close()
            del self.ns.client
        self.flush_command_log()

    @property
    def dig_public_ip(self):
        return self.run_command("dig +short myip.opendns.com @resolver1.opendns.com")[0].strip()

    def flush_command_log(self):
        if self.command_log_file and len(self.command_log) > 0:
            with open(self.command_log_file, "a") as f:
                for log_item in self.command_log:
                    f.write(json.dumps(log_item) + "\n")
            with open(self.command_log_file + "_cmdonly", "a") as f:
                for log_item in self.command_log:
                    command, runtime = log_item.get("command"), log_item.get("runtime", None)
                    formatted_runtime = f"{runtime:>2.2f}s" if runtime is not None else " " * 5
                    f.write(f"{formatted_runtime}\t{command}\n")
            self.command_log = []

    def add_command_log(self, command, runtime=None, **kwargs):
        self.command_log.append(dict(command=command, runtime=runtime, **kwargs))
        self.flush_command_log()

    def log_comment(self, comment):
        """Log comment in command log"""
        self.add_command_log(command=f"# {comment}")

    def run_command(self, command):
        """time command and run it"""
        client = self.ssh_client
        with Timer() as t:
            if self.stdout_log_file:
                with open(self.stdout_log_file, "a") as f:
                    f.write(f"\n$ {command}\n")
            if self.stderr_log_file:
                with open(self.stderr_log_file, "a") as f:
                    f.write(f"\n$ {command}\n")
            _, stdout, stderr = client.exec_command(command)
            stdout, stderr = (stdout.read().decode("utf-8"), stderr.read().decode("utf-8"))
            if self.stdout_log_file:
                with open(self.stdout_log_file, "a") as f:
                    f.write(stdout)
                    f.write("\n")
            if self.stderr_log_file:
                with open(self.stderr_log_file, "a") as f:
                    f.write(stderr)
                    f.write("\n")
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

    def sync_directory(self, local_dir, remote_dir, delete_remote=False, ignore_globs=[]):
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
