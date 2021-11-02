import json
import os
import threading
from functools import lru_cache
from pathlib import Path, PurePath

import boto3
import click
import paramiko
from loguru import logger

from skylark.utils import Timer, do_parallel


class Server:
    """Abstract server class to support basic SSH operations"""

    ns = threading.local()

    def __init__(self, command_log_file=None):
        self.command_log_file = command_log_file
        self.command_log = []
        if self.command_log_file:
            with open(self.command_log_file, "w") as f:
                f.write("")
            with open(self.command_log_file + "_cmdonly", "w") as f:
                f.write("")

    def __repr__(self):
        return f"Server()"

    def get_ssh_client_impl(self):
        raise NotImplementedError()

    @property
    def ssh_client(self):
        """Create SSH client and cache (one connection per thread using threadlocal)"""
        if not hasattr(self.ns, "client"):
            self.ns.client = self.get_ssh_client_impl()
        return self.ns.client

    ### Instance state

    def terminate_instance_impl(self):
        raise NotImplementedError()

    def terminate_instance(self):
        """Terminate instance"""
        client = self.ssh_client
        _, _, _ = client.exec_command(f"sudo shutdown -h now")
        client.close()
        self.close_server()
        self.terminate_instance_impl()

    def wait_for_ready_impl(self):
        raise NotImplementedError()

    def wait_for_ready(self):
        """Wait for instance to be ready"""
        self.wait_for_ready_impl()

    def close_server(self):
        if hasattr(self.ns, "client"):
            self.ns.client.close()
            del self.ns.client
        self.flush_command_log()

    @property
    def dig_public_ip(self):
        return self.run_command("dig +short myip.opendns.com @resolver1.opendns.com")[
            0
        ].strip()

    def flush_command_log(self):
        if self.command_log_file and len(self.command_log) > 0:
            with open(self.command_log_file, "a") as f:
                for log_item in self.command_log:
                    f.write(json.dumps(log_item) + "\n")
            with open(self.command_log_file + "_cmdonly", "a") as f:
                for log_item in self.command_log:
                    command, runtime = log_item.get("command"), log_item.get(
                        "runtime", None
                    )
                    formatted_runtime = (
                        f"{runtime:>2.2f}s" if runtime is not None else " " * 5
                    )
                    f.write(f"{formatted_runtime}\t{command}\n")
            self.command_log = []

    def add_command_log(self, command, runtime=None, **kwargs):
        self.command_log.append(dict(command=command, runtime=runtime, **kwargs))
        if len(self.command_log) > 5:
            self.flush_command_log()

    def log_comment(self, comment):
        """Log comment in command log"""
        self.add_command_log(command=f"# {comment}")

    ### SSH interface (run commands, copy files, etc.)

    def run_command(self, command):
        """time command and run it"""
        client = self.ssh_client
        with Timer() as t:
            _, stdout, stderr = client.exec_command(command)
            results = (stdout.read().decode("utf-8"), stderr.read().decode("utf-8"))
        self.add_command_log(
            command=command, stdout=results[0], stderr=results[1], runtime=t.elapsed
        )
        return results

    def copy_file(self, local_file, remote_file):
        """Copy local file to remote file."""
        client = self.ssh_client
        sftp = client.open_sftp()
        with Timer() as t:
            sftp.put(local_file, remote_file)
        self.add_command_log(
            command=f"<copy_file> {local_file} {remote_file}", runtime=t.elapsed
        )
        sftp.close()

    def sync_directory(
            self, local_dir, remote_dir, delete_remote=False, ignore_globs=[]
    ):
        """Copy local directory to remote directory. If remote directory exists, delete if delete_remote else raise exception."""

        local_path = Path(local_dir)
        remote_path = PurePath(remote_dir)
        ignored_paths = [Path(local_dir).glob(pattern) for pattern in ignore_globs]
        ignored_paths = set(
            [
                path.relative_to(local_dir)
                for path_list in ignored_paths
                for path in path_list
            ]
        )

        def copy_file(sftp, local_file: Path, remote_file: Path):
            with Timer() as t:
                sftp.put(str(local_file), str(remote_file))
            self.add_command_log(
                command=f"<copy_file> {local_file} {remote_file}", runtime=t.elapsed
            )

        client = self.ssh_client
        sftp = client.open_sftp()
        with Timer() as t:
            if delete_remote:
                self.run_command(f"rm -rf {remote_dir}")
            self.run_command(f"mkdir -p {remote_dir}")

            # make all directories
            recursive_dir_list = [
                x.relative_to(local_dir)
                for x in Path(local_dir).glob("**/*/")
                if x.is_dir()
            ]
            recursive_dir_list = [
                x for x in recursive_dir_list if x not in ignored_paths
            ]
            do_parallel(
                lambda x: self.run_command(f"mkdir -p {remote_dir}/{x}"),
                recursive_dir_list,
            )

            # copy all files
            file_list = [
                x.relative_to(local_dir)
                for x in Path(local_dir).glob("**/*")
                if x.is_file()
            ]
            file_list = [x for x in file_list if x not in ignored_paths]
            do_parallel(
                lambda x: copy_file(sftp, local_path / x, remote_path / x),
                file_list,
                progress_bar=True,
                n=1,
            )
        sftp.close()
        self.add_command_log(
            command=f"<sync_directory> {local_dir} {remote_dir}", runtime=t.elapsed
        )


class AWSServer(Server):
    """AWS Server class to support basic SSH operations"""

    def __init__(self, region_tag, instance_id, command_log_file=None):
        super().__init__(command_log_file=command_log_file)
        self.region_tag = region_tag
        assert region_tag.split(":")[0] == "aws"
        self.aws_region = region_tag.split(":")[1]
        self.instance_id = instance_id
        self.local_keyfile = self.make_keyfile()

    ### Instance state

    @property
    def public_ip(self):
        ec2 = self.get_boto3_resource("ec2")
        instance = ec2.Instance(self.instance_id)
        return instance.public_ip_address

    @property
    @lru_cache(maxsize=1)
    def instance_class(self):
        ec2 = self.get_boto3_resource("ec2")
        instance = ec2.Instance(self.instance_id)
        return instance.instance_type

    @property
    @lru_cache(maxsize=1)
    def tags(self):
        ec2 = self.get_boto3_resource("ec2")
        instance = ec2.Instance(self.instance_id)
        return {tag["Key"]: tag["Value"] for tag in instance.tags}

    @property
    @lru_cache(maxsize=1)
    def instance_name(self):
        return self.tags.get("Name", None)

    @property
    @lru_cache(maxsize=1)
    def region(self):
        return self.region_tag.split(":")[1]

    @property
    def instance_state(self):
        ec2 = self.get_boto3_resource("ec2")
        instance = ec2.Instance(self.instance_id)
        return instance.state["Name"]

    def __repr__(self):
        str_repr = f"AWSServer("
        str_repr += f"instance_id={self.instance_id}, "
        str_repr += f"instance_name={self.instance_name}, "
        str_repr += f"instance_state={self.instance_state}, "
        str_repr += f"instance_class={self.instance_class}, "
        str_repr += f"public_ip={self.public_ip}"
        str_repr += f")"
        return str_repr

    def terminate_instance_impl(self):
        ec2 = self.get_boto3_resource("ec2")
        ec2.instances.filter(InstanceIds=[self.instance_id]).terminate()
        logger.info(f"({self.aws_region}) Terminated instance {self.instance_id}")

    def wait_for_ready_impl(self):
        """Wait for instance to be ready"""
        ec2 = self.get_boto3_resource("ec2")
        instance = ec2.Instance(self.instance_id)
        instance.wait_until_running()

    ### AWS helper methods

    def get_boto3_resource(self, service_name):
        """Get boto3 resource (cache in threadlocal)"""
        ns_key = f"boto3_{service_name}"
        if not hasattr(self.ns, ns_key):
            setattr(
                self.ns,
                ns_key,
                boto3.resource(service_name, region_name=self.aws_region),
            )
        return getattr(self.ns, ns_key)

    def make_keyfile(self):
        local_key_file = os.path.expanduser(f"~/.ssh/{self.aws_region}.pem")
        ec2 = self.get_boto3_resource("ec2")
        if not os.path.exists(local_key_file):
            if click.confirm(
                    f"Local key file {local_key_file} does not exist. Create it?",
                    default=False,
            ):
                key_pair = ec2.create_key_pair(KeyName=self.aws_region)
                with open(local_key_file, "w") as f:
                    f.write(key_pair.key_material)
                os.chmod(local_key_file, 0o600)
                logger.info(
                    f"({self.aws_region}) Created keypair and saved to {local_key_file}"
                )
            else:
                raise Exception(
                    f"Keypair for {self.aws_region} not found and user declined to create it"
                )
        return local_key_file

    def add_ip_to_security_group(
            self, security_group_id: str = None, ip="0.0.0.0/0", from_port=0, to_port=65535
    ):
        """Add IP to security group. If security group ID is None, use default."""
        ec2 = self.get_boto3_resource("ec2")
        if security_group_id is None:
            security_group_id = [
                i for i in ec2.security_groups.filter(GroupNames=["default"]).all()
            ][0].id
        sg = ec2.SecurityGroup(security_group_id)
        matches_ip = (
            lambda rule: len(rule["IpRanges"]) > 0
                         and rule["IpRanges"][0]["CidrIp"] == ip
        )
        matches_ports = (
            lambda rule: rule["FromPort"] <= from_port and rule["ToPort"] >= to_port
        )
        if not any(
                rule["IpProtocol"] == "-1" and matches_ip(rule) and matches_ports(rule)
                        for rule in sg.ip_permissions
        ):
            sg.authorize_ingress(
                IpProtocol="-1", FromPort=from_port, ToPort=to_port, CidrIp=ip
            )
            logger.info(
                f"({self.aws_region}) Added IP {ip} to security group {security_group_id}"
            )

    ### SSH interface (run commands, copy files, etc.)

    def get_ssh_client_impl(self):
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(
            self.public_ip, username="ubuntu", key_filename=self.local_keyfile
        )
        return client