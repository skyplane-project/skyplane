import os
import sys
import time
import tempfile

import argparse
import boto3
import click
import json
from loguru import logger
import paramiko
import threading

class Server:
    """Abstract server class to support basic SSH operations"""
    ns = threading.local()

    def __init__(self, region_tag, instance_id, ip, command_log_file=None):
        self.region_tag = region_tag
        self.instance_id = instance_id
        self.ip = ip
        
        # command log stores all commands with stdout and stderr
        self.command_log_file = command_log_file
        self.command_log = []

    def __repr__(self):
        return f"Server({self.region}, {self.instance_id}, {self.ip})"
    
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
    def public_ip(self):
        return self.run_command("dig +short myip.opendns.com @resolver1.opendns.com")[0].strip()
    
    def flush_command_log(self):
        if self.command_log_file and len(self.command_log) > 0:
            with open(self.command_log_file, "a") as f:
                for log_item in self.command_log:
                    f.write(json.dumps(log_item) + "\n")
            with open(self.command_log_file + "_cmdonly", "a") as f:
                for log_item in self.command_log:
                    f.write(log_item.get('command') + "\n")
            self.command_log = []
    
    def log_comment(self, comment):
        """Log comment in command log"""
        self.command_log.append(dict(command=f"# {comment}"))

    ### SSH interface (run commands, copy files, etc.)

    def run_command(self, command):
        """time command and run it"""
        client = self.ssh_client
        start_time = time.time()
        _, stdout, stderr = client.exec_command(command)
        results = (stdout.read().decode("utf-8"), stderr.read().decode("utf-8"))
        self.command_log.append(dict(command=command, stdout=results[0], stderr=results[1], runtime=time.time() - start_time))
        return results


    def copy_file(self, local_file, remote_file):
        """Copy local file to remote file."""
        client = self.ssh_client
        sftp = client.open_sftp()
        start = time.time()
        sftp.put(local_file, remote_file)
        self.command_log.append(dict(command=f"<copy_file> {local_file} {remote_file}", runtime=time.time() - start))
        sftp.close()

    def sync_directory(self, local_dir, remote_dir, delete_remote=False):
        """Copy local directory to remote directory. If remote directory exists, delete if delete_remote else raise exception."""
        client = self.ssh_client
        start = time.time()
        remote_exists = self.run_command(f"test -d {remote_dir} | echo $?")[0].strip() == "0"
        if remote_exists:
            if delete_remote:
                self.run_command(f"rm -rf {remote_dir}")
            else:
                raise Exception(f"Remote directory {remote_dir} already exists")

        def put_dir(sftp, local_dir, remote_dir):
            sftp.mkdir(remote_dir, os.stat(local_dir).st_mode)
            for local_file in os.listdir(local_dir):
                local_path = os.path.join(local_dir, local_file)
                remote_path = os.path.join(remote_dir, local_file)
                if os.path.isdir(local_path):
                    self.run_command(f"mkdir -p {remote_path}")
                    self.flush_command_log()
                    put_dir(sftp, local_path, remote_path)
                else:
                    self.copy_file(local_path, remote_path)
        

        sftp = client.open_sftp()
        put_dir(sftp, local_dir, remote_dir)
        sftp.close()
        self.command_log.append(dict(command=f"<sync_directory> {local_dir} {remote_dir}", runtime=time.time() - start))
    

class AWSServer(Server):
    """AWS Server class to support basic SSH operations"""
    def __init__(self, region_tag, instance_id, ip, command_log_file=None):
        super().__init__(region_tag, instance_id, ip, command_log_file=command_log_file)
        self.aws_region = region_tag.split(":")[1]
        self.local_keyfile = self.make_keyfile()
    
    def __repr__(self):
        return f"AWSServer({self.aws_region}, {self.instance_id}, {self.ip})"
    
    ### Instance state

    def terminate_instance_impl(self):
        ec2 = self.get_boto3_resource("ec2")
        ec2.instances.filter(InstanceIds=[self.instance_id]).terminate()
        logger.info(f"({self.aws_region}) Terminated instance {self.instance_id}")

    def wait_for_ready_impl(self):
        """Wait for instance to be ready"""
        ec2 = self.get_boto3_resource("ec2")
        instance = ec2.Instance(self.instance_id)
        instance.wait_until_running()
        logger.info(f"({self.aws_region}) Instance {self.instance_id} ready")
    
    ### AWS helper methods

    def get_boto3_resource(self, service_name):
        """Get boto3 resource (cache in threadlocal)"""
        ns_key = f"boto3_{service_name}"
        if not hasattr(self.ns, ns_key):
            setattr(self.ns, ns_key, boto3.resource(service_name, region_name=self.aws_region))
        return getattr(self.ns, ns_key)

    def make_keyfile(self):
        local_key_file = os.path.expanduser(f"~/.ssh/{self.aws_region}.pem")
        ec2 = self.get_boto3_resource("ec2")
        if not os.path.exists(local_key_file):
            if click.confirm(f'Local key file {local_key_file} does not exist. Create it?', default=False):
                key_pair = ec2.create_key_pair(KeyName=self.aws_region)
                with open(local_key_file, "w") as f:
                    f.write(key_pair.key_material)
                os.chmod(local_key_file, 0o600)
                logger.info(f"({self.aws_region}) Created keypair and saved to {local_key_file}")
            else:
                raise Exception(f"Keypair for {self.aws_region} not found and user declined to create it")
        return local_key_file

    def add_ip_to_security_group(self, security_group_id:str=None, ip='0.0.0.0/0', from_port=0, to_port=65535):
        """Add IP to security group. If security group ID is None, use default."""
        ec2 = self.get_boto3_resource("ec2")
        if security_group_id is None:
            security_group_id = [i for i in ec2.security_groups.filter(GroupNames=["default"]).all()][0].id
        sg = ec2.SecurityGroup(security_group_id)
        matches_ip = lambda rule: len(rule["IpRanges"]) > 0 and rule["IpRanges"][0]["CidrIp"] == ip
        matches_ports = lambda rule: rule["FromPort"] <= from_port and rule["ToPort"] >= to_port
        if not any(rule["IpProtocol"] == "-1" and matches_ip(rule) and matches_ports(rule) for rule in sg.ip_permissions):
            sg.authorize_ingress(IpProtocol="-1", FromPort=from_port, ToPort=to_port, CidrIp=ip)
            logger.info(f"({self.aws_region}) Added IP {ip} to security group {security_group_id}")

    ### SSH interface (run commands, copy files, etc.)
    
    def get_ssh_client_impl(self):
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(self.ip, username="ubuntu", key_filename=self.local_keyfile)
        return client


# unit test server
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--ip", required=True)
    parser.add_argument("--region", required=True)
    parser.add_argument("--instance-id", required=True)
    parser.add_argument("--command-log", default=None)
    args = parser.parse_args()
    
    server = AWSServer(args.region, args.instance_id, args.ip, command_log_file=args.command_log)
    server.wait_for_ready()

    # test run_command
    logger.info("Test: run_command")
    server.log_comment("Test: run_command")
    assert server.run_command("echo hello")[0] == "hello\n"
    
    # test file copy
    logger.info("Test: copy_file")
    server.log_comment("Test: copy_file")
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpfile = os.path.join(tmpdir, "test.txt")
        with open(tmpfile, "w") as f:
            f.write("hello world")
        server.copy_file(tmpfile, "/tmp/test.txt")
        assert open(tmpfile).read() == server.run_command("cat /tmp/test.txt")[0]
    
    # test sync_directory
    logger.info("Test: sync_directory")
    server.log_comment("Test: sync_directory")
    remote_tmpdir = "/tmp/test_sync_directory" 
    with tempfile.TemporaryDirectory() as tmpdir:
        with open(os.path.join(tmpdir, "test1.txt"), "w") as f:
            f.write("hello world")
        with open(os.path.join(tmpdir, "test2.txt"), "w") as f:
            f.write("hello world")
        
        server.sync_directory(tmpdir, "/tmp/test_sync_directory", delete_remote=True)
        server.flush_command_log()

        assert open(os.path.join(tmpdir, "test1.txt")).read() == server.run_command(f"cat {remote_tmpdir}/test1.txt")[0]
        assert open(os.path.join(tmpdir, "test2.txt")).read() == server.run_command(f"cat {remote_tmpdir}/test2.txt")[0]
        assert server.run_command(f"ls {remote_tmpdir}")[0].strip().split() == ["test1.txt", "test2.txt"]
        server.run_command(f"rm -rf {remote_tmpdir}")

    # test copy source directory
    logger.info("Test: sync_directory source")
    server.log_comment("Test: sync_directory source")
    with tempfile.TemporaryDirectory() as tmpdir:
        # get script dir's parent dir
        source_dir = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
        server.sync_directory(source_dir, "/tmp/test_copy_source", delete_remote=True)

    # clean up
    server.close_server()
    logger.info("All tests passed")