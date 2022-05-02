import functools
import json
import uuid
import os
import time
from typing import List, Optional
from pathlib import Path

import botocore
from skylark.compute.aws.aws_auth import AWSAuthentication
from skylark.utils import logger
from skylark import key_root
from skylark import exceptions
from ilock import ILock
from skylark import skylark_root
from skylark.compute.aws.aws_server import AWSServer
from skylark.compute.cloud_providers import CloudProvider
from skylark.utils.utils import do_parallel, wait_for

try:
    import pandas as pd
except ImportError:
    pd = None
    logger.warning("pandas not installed, will not be able to load transfer costs")


class AWSCloudProvider(CloudProvider):
    def __init__(self):
        super().__init__()
        self.auth = AWSAuthentication()

    @property
    def name(self):
        return "aws"

    @staticmethod
    def region_list() -> List[str]:
        # todo query AWS for list of active regions
        region_list = AWSAuthentication.get_region_config()
        return region_list

    @staticmethod
    @functools.lru_cache(maxsize=None)
    def load_transfer_cost_df():
        return pd.read_csv(skylark_root / "profiles" / "aws_transfer_costs.csv").set_index(["src", "dst"])

    @staticmethod
    def get_transfer_cost(src_key, dst_key, premium_tier=True):
        assert premium_tier, "AWS transfer cost is only available for premium tier"
        transfer_df = AWSCloudProvider.load_transfer_cost_df()

        src_provider, src = src_key.split(":")
        dst_provider, dst = dst_key.split(":")

        assert src_provider == "aws"
        if dst_provider == "aws":
            if (src, dst) in transfer_df.index:
                return transfer_df.loc[src, dst]["cost"]
            else:
                logger.warning(f"No transfer cost found for {src_key} -> {dst_key}, using max of {src}")
                src_rows = transfer_df.loc[src]
                src_rows = src_rows[src_rows.index != "internet"]
                return src_rows.max()["cost"]
        else:
            return transfer_df.loc[src, "internet"]["cost"]

    def get_instance_list(self, region: str) -> List[AWSServer]:
        ec2 = self.auth.get_boto3_resource("ec2", region)
        valid_states = ["pending", "running", "stopped", "stopping"]
        instances = ec2.instances.filter(Filters=[{"Name": "instance-state-name", "Values": valid_states}])
        try:
            instance_ids = [i.id for i in instances]
        except botocore.exceptions.ClientError as e:
            logger.error(f"error provisioning in {region}: {e}")
            return []

        return [AWSServer(f"aws:{region}", i) for i in instance_ids]

    def get_security_group(self, region: str, vpc_name="skylark", sg_name="skylark"):
        ec2 = self.auth.get_boto3_resource("ec2", region)
        vpcs = list(ec2.vpcs.filter(Filters=[{"Name": "tag:Name", "Values": [vpc_name]}]).all())
        assert len(vpcs) == 1, f"Found {len(vpcs)} vpcs with name {vpc_name}"
        sgs = [sg for sg in vpcs[0].security_groups.all() if sg.group_name == sg_name]
        assert len(sgs) == 1
        return sgs[0]

    def get_vpc(self, region: str, vpc_name="skylark"):
        ec2 = self.auth.get_boto3_resource("ec2", region)
        vpcs = list(ec2.vpcs.filter(Filters=[{"Name": "tag:Name", "Values": [vpc_name]}]).all())
        if len(vpcs) == 0:
            return None
        else:
            return vpcs[0]

    def make_vpc(self, region: str, vpc_name="skylark"):
        ec2 = self.auth.get_boto3_resource("ec2", region)
        ec2client = ec2.meta.client
        vpcs = list(ec2.vpcs.filter(Filters=[{"Name": "tag:Name", "Values": [vpc_name]}]).all())

        # find matching valid VPC
        matching_vpc = None
        for vpc in vpcs:
            subsets_azs = [subnet.availability_zone for subnet in vpc.subnets.all()]
            if (
                vpc.cidr_block == "10.0.0.0/16"
                and vpc.describe_attribute(Attribute="enableDnsSupport")["EnableDnsSupport"]
                and vpc.describe_attribute(Attribute="enableDnsHostnames")["EnableDnsHostnames"]
                and all(az in subsets_azs for az in self.auth.get_azs_in_region(region))
            ):
                matching_vpc = vpc
                # delete all other vpcs
                for vpc in vpcs:
                    if vpc != matching_vpc:
                        try:
                            self.delete_vpc(region, vpc.id)
                        except botocore.exceptions.ClientError as e:
                            logger.warning(f"Failed to delete VPC {vpc.id} in {region}: {e}")
                break

        # make vpc if none found
        if matching_vpc is None:
            # delete old skylark vpcs
            for vpc in vpcs:
                self.delete_vpc(region, vpc.id)

            # enable dns support, enable dns hostnames
            matching_vpc = ec2.create_vpc(CidrBlock="10.0.0.0/16", InstanceTenancy="default")
            matching_vpc.modify_attribute(EnableDnsSupport={"Value": True})
            matching_vpc.modify_attribute(EnableDnsHostnames={"Value": True})
            matching_vpc.create_tags(Tags=[{"Key": "Name", "Value": vpc_name}])
            matching_vpc.wait_until_available()

            # make subnet for each AZ in region
            def make_subnet(az):
                subnet_cidr_id = ord(az[-1]) - ord("a")
                subnet = self.auth.get_boto3_resource("ec2", region).create_subnet(
                    CidrBlock=f"10.0.{subnet_cidr_id}.0/24", VpcId=matching_vpc.id, AvailabilityZone=az
                )
                subnet.meta.client.modify_subnet_attribute(SubnetId=subnet.id, MapPublicIpOnLaunch={"Value": True})
                return subnet.id

            subnet_ids = do_parallel(make_subnet, self.auth.get_azs_in_region(region), return_args=False)

            # make internet gateway
            igw = ec2.create_internet_gateway()
            igw.attach_to_vpc(VpcId=matching_vpc.id)
            public_route_table = list(matching_vpc.route_tables.all())[0]
            # add a default route, for Public Subnet, pointing to Internet Gateway
            ec2client.create_route(RouteTableId=public_route_table.id, DestinationCidrBlock="0.0.0.0/0", GatewayId=igw.id)
            for subnet_id in subnet_ids:
                public_route_table.associate_with_subnet(SubnetId=subnet_id)

            # make security group named "skylark"
            tagSpecifications=[
                {
                    'Tags': [
                        {
                            'skyplane': 'true',}
                    ]
                },
            ]
            sg = ec2.create_security_group(GroupName="skylark", Description="Default security group for Skylark VPC", VpcId=matching_vpc.id, TagSpecifications=tagSpecifications)
        return matching_vpc

    def delete_vpc(self, region: str, vpcid: str):
        """Delete VPC, from https://gist.github.com/vernhart/c6a0fc94c0aeaebe84e5cd6f3dede4ce"""
        logger.warning(f"[{region}] Deleting VPC {vpcid}")
        ec2 = self.auth.get_boto3_resource("ec2", region)
        ec2client = ec2.meta.client
        vpc = ec2.Vpc(vpcid)
        # delete all route table associations
        for rt in vpc.route_tables.all():
            for rta in rt.associations:
                if not rta.main:
                    rta.delete()
        # delete any instances
        for subnet in vpc.subnets.all():
            for instance in subnet.instances.all():
                instance.terminate()
        # delete our endpoints
        for ep in ec2client.describe_vpc_endpoints(Filters=[{"Name": "vpc-id", "Values": [vpcid]}])["VpcEndpoints"]:
            ec2client.delete_vpc_endpoints(VpcEndpointIds=[ep["VpcEndpointId"]])
        # delete our security groups
        for sg in vpc.security_groups.all():
            if sg.group_name != "default":
                sg.delete()
        # delete any vpc peering connections
        for vpcpeer in ec2client.describe_vpc_peering_connections(Filters=[{"Name": "requester-vpc-info.vpc-id", "Values": [vpcid]}])[
            "VpcPeeringConnections"
        ]:
            ec2.VpcPeeringConnection(vpcpeer["VpcPeeringConnectionId"]).delete()
        # delete non-default network acls
        for netacl in vpc.network_acls.all():
            if not netacl.is_default:
                netacl.delete()
        # delete network interfaces
        for subnet in vpc.subnets.all():
            for interface in subnet.network_interfaces.all():
                interface.delete()
            subnet.delete()
        # detach and delete all gateways associated with the vpc
        for gw in vpc.internet_gateways.all():
            vpc.detach_internet_gateway(InternetGatewayId=gw.id)
            gw.delete()
        # finally, delete the vpc
        ec2client.delete_vpc(VpcId=vpcid)

    def create_iam(self, iam_name: str = "skylark_gateway", attach_policy_arn: Optional[str] = None):
        """Create IAM role if it doesn't exist and grant managed role if given."""
        iam = self.auth.get_boto3_client("iam")
        with ILock(f"aws_create_iam_{iam_name}"):
            try:
                iam.get_role(RoleName=iam_name)
            except iam.exceptions.NoSuchEntityException:
                doc = {
                    "Version": "2012-10-17",
                    "Statement": [{"Effect": "Allow", "Principal": {"Service": "ec2.amazonaws.com"}, "Action": "sts:AssumeRole"}],
                }
                iam.create_role(RoleName=iam_name, AssumeRolePolicyDocument=json.dumps(doc), Tags=[{"Key": "skylark", "Value": "true"}])
            if attach_policy_arn:
                iam.attach_role_policy(RoleName=iam_name, PolicyArn=attach_policy_arn)

        return fn()

    def add_ip_to_security_group(self, aws_region: str, ip: str = "None"):
        """Add IP to security group. If security group ID is None, use group named skylark (create if not exists)."""

        @lockutils.synchronized(f"aws_add_ip_to_security_group_{aws_region}", external=True, lock_path="/tmp/skylark_locks")
        def fn():
            if ip == "None":
                # The default fallback if we don't have list of ips
                self.make_vpc(aws_region)
                sg = self.get_security_group(aws_region)
                try:
                    sg.authorize_ingress(
                        IpPermissions=[{"IpProtocol": "-1", "FromPort": -1, "ToPort": -1, "IpRanges": [{"CidrIp": "0.0.0.0/0"}]}]
                    )
                except botocore.exceptions.ClientError as e:
                    if not str(e).endswith("already exists"):
                        raise e
            elif ip:
                # Add instance IP to security group
                sg = self.get_security_group(aws_region)
                try:
                    sg.authorize_ingress(
                        IpPermissions=[{"IpProtocol": "tcp", "FromPort": 12000, "ToPort": 65535, "IpRanges": [{"CidrIp": ip + "/32"}]}]
                    )
                except botocore.exceptions.ClientError as e:
                    if not str(e).endswith("already exists"):
                        raise e

        return fn()

    def remove_ip_from_security_group(self, aws_region: str, ip: str = "None"):
        """Remove IP from security group. If security group ID is None, return."""

        @lockutils.synchronized(f"remove_ip_from_security_group{aws_region}", external=True, lock_path="/tmp/skylark_locks")
        def fn():
            if ip == "None":
                # The default fallback if we don't have list of ips. We do nothing then.
                return 
            elif ip:
                # Remove instance IP from security group
                sg = self.get_security_group(aws_region)
                try:
                    sg.revoke_ingress(
                        IpPermissions= [{"IpProtocol": "tcp", "FromPort": 12000, "ToPort": 65535, "IpRanges": [{"CidrIp": ip + "/32"}]}]
                    )
                except botocore.exceptions.ClientError as e:
                    raise e

        return fn()

    def clear_security_group(self, aws_region: str, vpc_name="skylark"):
        """Clears security group, and allows ssh and dozzle if activated"""
        logger.warn(f"Clearing the Security Group will interefere with the VPC
                affecting cuncurrent transfers.")

        @lockutils.synchronized(f"aws_clear_security_group_{aws_region}", external=True, lock_path="/tmp/skylark_locks")
        def fn():
            sg = self.get_security_group(aws_region)
            # Revoke all ingress rules
            if sg.ip_permissions:
                # logger.warn(f"Revoking {sg.ip_permissions} rules for {sg.group_name}")
                sg.revoke_ingress(IpPermissions=sg.ip_permissions)
            # Allow internal traffic
            try:
                sg.authorize_ingress(
                    IpPermissions=[{"IpProtocol": "-1", "FromPort": -1, "ToPort": -1, "IpRanges": [{"CidrIp": "0.0.0.0/32"}]}]
                )
            except botocore.exceptions.ClientError as e:
                if not str(e).endswith("already exists"):
                    raise e
            # Allow ssh access on Port 22
            try:
                logger.info(f"Adding ssh access to security group {sg.id}")
                sg.authorize_ingress(
                    IpPermissions=[{"IpProtocol": "tcp", "FromPort": 22, "ToPort": 22, "IpRanges": [{"CidrIp": "0.0.0.0/0"}]}]
                )
            except botocore.exceptions.ClientError as e:
                if not str(e).endswith("already exists"):
                    raise e
            # Allow Dozzle on log_viewer_port
            if self.logging_enabled:
                logger.info(f"Adding log viewer port {self.log_viewer_port} to security group {sg.id}")
                try:
                    sg.authorize_ingress(
                        IpPermissions=[
                            {
                                "IpProtocol": "tcp",
                                "FromPort": self.log_viewer_port,
                                "ToPort": self.log_viewer_port,
                                "IpRanges": [{"CidrIp": "0.0.0.0/0"}],
                            }
                        ]
                    )
                except botocore.exceptions.ClientError as e:
                    if not str(e).endswith("already exists"):
                        raise e

    def ensure_keyfile_exists(self, aws_region, prefix=key_root / "aws"):
        ec2 = self.auth.get_boto3_resource("ec2", aws_region)
        ec2_client = self.auth.get_boto3_client("ec2", aws_region)
        prefix = Path(prefix)
        key_name = f"skylark-{aws_region}"
        local_key_file = prefix / f"{key_name}.pem"

        if not local_key_file.exists():
            with ILock(f"aws_keyfile_lock_{aws_region}"):
                if not local_key_file.exists():  # double check due to lock
                    local_key_file.parent.mkdir(parents=True, exist_ok=True)
                    if key_name in set(p["KeyName"] for p in ec2_client.describe_key_pairs()["KeyPairs"]):
                        logger.warning(f"Deleting key {key_name} in region {aws_region}")
                        ec2_client.delete_key_pair(KeyName=key_name)
                    key_pair = ec2.create_key_pair(KeyName=f"skylark-{aws_region}", KeyType="rsa")
                    with local_key_file.open("w") as f:
                        key_str = key_pair.key_material
                        if not key_str.endswith("\n"):
                            key_str += "\n"
                        f.write(key_str)
                    os.chmod(local_key_file, 0o600)
                    logger.info(f"Created key file {local_key_file}")

        return local_key_file

    def provision_instance(
        self,
        region: str,
        instance_class: str,
        name: Optional[str] = None,
        # ami_id: Optional[str] = None,
        tags={"skylark": "true"},
        ebs_volume_size: int = 128,
        iam_name: str = "skylark_gateway",
    ) -> AWSServer:

        assert not region.startswith("aws:"), "Region should be AWS region"
        if name is None:
            name = f"skylark-aws-{str(uuid.uuid4()).replace('-', '')}"
        iam_instance_profile_name = f"{name}_profile"
        iam = self.auth.get_boto3_client("iam", region)
        ec2 = self.auth.get_boto3_resource("ec2", region)
        ec2_client = self.auth.get_boto3_client("ec2", region)
        vpc = self.get_vpc(region)
        assert vpc is not None, "No VPC found"

        # get subnet list
        def instance_class_supported(az):
            # describe_instance_type_offerings
            offerings_list = ec2_client.describe_instance_type_offerings(
                LocationType="availability-zone",
                Filters=[
                    {"Name": "location", "Values": [az]},
                ],
            )
            offerings = [o for o in offerings_list["InstanceTypeOfferings"] if o["InstanceType"] == instance_class]
            return len(offerings) > 0

        subnets = [subnet for subnet in vpc.subnets.all() if instance_class_supported(subnet.availability_zone)]
        assert len(subnets) > 0, "No subnets found that support specified instance class"

        def check_iam_role():
            try:
                iam.get_role(RoleName=iam_name)
                return True
            except iam.exceptions.NoSuchEntityException:
                return False

        def check_instance_profile():
            try:
                iam.get_instance_profile(InstanceProfileName=iam_instance_profile_name)
                return True
            except iam.exceptions.NoSuchEntityException:
                return False

        # wait for iam_role to be created and create instance profile
        wait_for(check_iam_role, timeout=60, interval=0.5)
        iam.create_instance_profile(InstanceProfileName=iam_instance_profile_name, Tags=[{"Key": "skylark", "Value": "true"}])
        iam.add_role_to_instance_profile(InstanceProfileName=iam_instance_profile_name, RoleName=iam_name)
        wait_for(check_instance_profile, timeout=60, interval=0.5)

        def start_instance(subnet_id: str):
            return ec2.create_instances(
                ImageId="resolve:ssm:/aws/service/ecs/optimized-ami/amazon-linux-2/recommended/image_id",
                InstanceType=instance_class,
                MinCount=1,
                MaxCount=1,
                KeyName=f"skylark-{region}",
                TagSpecifications=[
                    {
                        "ResourceType": "instance",
                        "Tags": [{"Key": "Name", "Value": name}] + [{"Key": k, "Value": v} for k, v in tags.items()],
                    }
                ],
                BlockDeviceMappings=[
                    {
                        "DeviceName": "/dev/sda1",
                        "Ebs": {"DeleteOnTermination": True, "VolumeSize": ebs_volume_size, "VolumeType": "gp2"},
                    }
                ],
                NetworkInterfaces=[
                    {
                        "DeviceIndex": 0,
                        "Groups": [self.get_security_group(region).group_id],
                        "SubnetId": subnet_id,
                        "AssociatePublicIpAddress": True,
                        "DeleteOnTermination": True,
                    }
                ],
                IamInstanceProfile={"Name": iam_instance_profile_name},
                InstanceInitiatedShutdownBehavior="terminate",
            )

        backoff = 1
        max_retries = 8
        max_backoff = 8
        current_subnet_id = 0
        for i in range(max_retries):
            try:
                instance = start_instance(subnets[current_subnet_id].id)
                break
            except botocore.exceptions.ClientError as e:
                if i == max_retries - 1:
                    raise e
                elif "VcpuLimitExceeded" in str(e):
                    raise exceptions.InsufficientVCPUException() from e
                elif "Invalid IAM Instance Profile name" not in str(e):
                    logger.warning(str(e))
                elif "InsufficientInstanceCapacity" in str(e):
                    # try another subnet
                    current_subnet_id = (current_subnet_id + 1) % len(subnets)
                time.sleep(backoff)
                backoff = min(backoff * 2, max_backoff)

        assert len(instance) == 1, f"Expected 1 instance, got {len(instance)}"
        instance[0].wait_until_running()
        server = AWSServer(f"aws:{region}", instance[0].id)
        server.wait_for_ready()
        return server
