import os
from functools import lru_cache

import boto3
import paramiko
from loguru import logger

from skylark.compute.server import Server, ServerState


class AWSServer(Server):
    """AWS Server class to support basic SSH operations"""

    def __init__(self, region_tag, instance_id, command_log_file=None):
        super().__init__(command_log_file=command_log_file)
        self.region_tag = region_tag
        assert region_tag.split(":")[0] == "aws"
        self.aws_region = region_tag.split(":")[1]
        self.instance_id = instance_id
        self.local_keyfile = self.make_keyfile()

    ### AWS helper methods

    @classmethod
    def get_boto3_resource(cls, service_name, aws_region):
        """Get boto3 resource (cache in threadlocal)"""
        ns_key = f"boto3_resource_{service_name}_{aws_region}"
        if not hasattr(cls.ns, ns_key):
            setattr(
                cls.ns,
                ns_key,
                boto3.resource(service_name, region_name=aws_region),
            )
        return getattr(cls.ns, ns_key)

    @classmethod
    def get_boto3_client(cls, service_name, aws_region):
        """Get boto3 client (cache in threadlocal)"""
        ns_key = f"boto3_client_{service_name}_{aws_region}"
        if not hasattr(cls.ns, ns_key):
            setattr(
                cls.ns,
                ns_key,
                boto3.client(service_name, region_name=aws_region),
            )
        return getattr(cls.ns, ns_key)

    def make_keyfile(self):
        local_key_file = os.path.expanduser(f"~/.ssh/{self.aws_region}.pem")
        ec2 = AWSServer.get_boto3_resource("ec2", self.aws_region)
        if not os.path.exists(local_key_file):
            key_pair = ec2.create_key_pair(KeyName=self.aws_region)
            with open(local_key_file, "w") as f:
                f.write(key_pair.key_material)
            os.chmod(local_key_file, 0o600)
            logger.info(f"({self.aws_region}) Created keypair and saved to {local_key_file}")
        return local_key_file

    def add_ip_to_security_group(self, security_group_id: str = None, ip="0.0.0.0/0", from_port=0, to_port=65535):
        """Add IP to security group. If security group ID is None, use default."""
        ec2 = AWSServer.get_boto3_resource("ec2", self.aws_region)
        if security_group_id is None:
            security_group_id = [i for i in ec2.security_groups.filter(GroupNames=["default"]).all()][0].id
        sg = ec2.SecurityGroup(security_group_id)
        matches_ip = lambda rule: len(rule["IpRanges"]) > 0 and rule["IpRanges"][0]["CidrIp"] == ip
        matches_ports = lambda rule: rule["FromPort"] <= from_port and rule["ToPort"] >= to_port
        if not any(rule["IpProtocol"] == "-1" and matches_ip(rule) and matches_ports(rule) for rule in sg.ip_permissions):
            sg.authorize_ingress(IpProtocol="-1", FromPort=from_port, ToPort=to_port, CidrIp=ip)
            logger.info(f"({self.aws_region}) Added IP {ip} to security group {security_group_id}")

    ### Instance state

    @property
    def public_ip(self):
        ec2 = AWSServer.get_boto3_resource("ec2", self.aws_region)
        instance = ec2.Instance(self.instance_id)
        return instance.public_ip_address

    @property
    @lru_cache(maxsize=1)
    def instance_class(self):
        ec2 = AWSServer.get_boto3_resource("ec2", self.aws_region)
        instance = ec2.Instance(self.instance_id)
        return instance.instance_type

    @property
    @lru_cache(maxsize=1)
    def tags(self):
        ec2 = AWSServer.get_boto3_resource("ec2", self.aws_region)
        instance = ec2.Instance(self.instance_id)
        return {tag["Key"]: tag["Value"] for tag in instance.tags}

    @property
    @lru_cache(maxsize=1)
    def instance_name(self):
        return self.tags.get("Name", None)

    @property
    @lru_cache(maxsize=1)
    def region(self):
        return self.aws_region

    @property
    def instance_state(self):
        ec2 = AWSServer.get_boto3_resource("ec2", self.aws_region)
        instance = ec2.Instance(self.instance_id)
        return ServerState.from_aws_state(instance.state["Name"])

    def __repr__(self):
        str_repr = f"AWSServer("
        str_repr += f"{self.region_tag}, "
        str_repr += f"{self.instance_id}, "
        str_repr += f"{self.command_log_file}"
        str_repr += f")"
        return str_repr

    def terminate_instance_impl(self):
        ec2 = AWSServer.get_boto3_resource("ec2", self.aws_region)
        ec2.instances.filter(InstanceIds=[self.instance_id]).terminate()
        logger.info(f"({self.aws_region}) Terminated instance {self.instance_id}")

    ### SSH interface (run commands, copy files, etc.)

    def get_ssh_client_impl(self):
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(self.public_ip, username="ubuntu", key_filename=self.local_keyfile)
        return client

    @staticmethod
    def get_ubuntu_ami_id(region):
        client = AWSServer.get_boto3_client("ec2", region)
        response = client.describe_images(
            Filters=[
                {
                    "Name": "name",
                    "Values": [
                        "ubuntu/images/hvm-ssd/ubuntu-bionic-18.04-amd64-server-*",
                    ],
                },
                {
                    "Name": "owner-id",
                    "Values": [
                        "099720109477",
                    ],
                },
            ]
        )
        if len(response["Images"]) == 0:
            raise Exception("No AMI found for region {}".format(region))
        else:
            # Sort the images by date and return the last one
            image_list = sorted(response["Images"], key=lambda x: x["CreationDate"], reverse=True)
            return image_list[0]["ImageId"]

    @staticmethod
    def provision_instance(region, instance_class, name, ami_id=None, tags={"skylark": "true"}) -> "AWSServer":
        if ami_id is None:
            ami_id = AWSServer.get_ubuntu_ami_id(region)
        ec2 = AWSServer.get_boto3_resource("ec2", region)
        instance = ec2.create_instances(
            ImageId=ami_id,
            InstanceType=instance_class,
            MinCount=1,
            MaxCount=1,
            KeyName=region,
            TagSpecifications=[
                {
                    "ResourceType": "instance",
                    "Tags": [{"Key": "Name", "Value": name}] + [{"Key": k, "Value": v} for k, v in tags.items()],
                }
            ],
        )
        return AWSServer(f"aws:{region}", instance[0].id)
