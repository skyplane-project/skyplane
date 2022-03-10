import uuid
from typing import List, Optional

import botocore
import pandas as pd
from skylark.compute.aws.aws_auth import AWSAuthentication
from skylark.utils import logger

from oslo_concurrency import lockutils
from skylark import skylark_root
from skylark.compute.aws.aws_server import AWSServer
from skylark.compute.cloud_providers import CloudProvider
from skylark.utils.utils import retry_backoff


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
        all_regions = [
            "af-south-1",
            "ap-east-1",
            "ap-northeast-1",
            "ap-northeast-2",
            "ap-northeast-3",
            "ap-south-1",
            "ap-southeast-1",
            "ap-southeast-2",
            "ap-southeast-3",
            "ca-central-1",
            "eu-central-1",
            "eu-north-1",
            "eu-south-1",
            "eu-west-1",
            "eu-west-2",
            "eu-west-3",
            "me-south-1",
            "sa-east-1",
            "us-east-1",
            "us-east-2",
            "us-west-1",
            "us-west-2",
        ]
        return all_regions

    @staticmethod
    def get_transfer_cost(src_key, dst_key, premium_tier=True):
        assert premium_tier, "AWS transfer cost is only available for premium tier"
        transfer_df = pd.read_csv(skylark_root / "profiles" / "aws_transfer_costs.csv").set_index(["src", "dst"])

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
        ec2 = self.auth.get_boto3_client("ec2", region)
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
            if (
                vpc.cidr_block == "10.0.0.0/16"
                and vpc.describe_attribute(Attribute="enableDnsSupport")["EnableDnsSupport"]
                and vpc.describe_attribute(Attribute="enableDnsHostnames")["EnableDnsHostnames"]
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

            # make subnet
            subnet = ec2.create_subnet(CidrBlock="10.0.2.0/24", VpcId=matching_vpc.id)
            subnet.meta.client.modify_subnet_attribute(SubnetId=subnet.id, MapPublicIpOnLaunch={"Value": True})

            # make internet gateway
            igw = ec2.create_internet_gateway()
            igw.attach_to_vpc(VpcId=matching_vpc.id)
            public_route_table = list(matching_vpc.route_tables.all())[0]
            # add a default route, for Public Subnet, pointing to Internet Gateway
            ec2client.create_route(RouteTableId=public_route_table.id, DestinationCidrBlock="0.0.0.0/0", GatewayId=igw.id)
            public_route_table.associate_with_subnet(SubnetId=subnet.id)

            # make security group named "default"
            sg = ec2.create_security_group(GroupName="skylark", Description="Default security group for Skylark VPC", VpcId=matching_vpc.id)
        return matching_vpc

    def delete_vpc(self, region: str, vpcid: str):
        """Delete VPC, from https://gist.github.com/vernhart/c6a0fc94c0aeaebe84e5cd6f3dede4ce"""
        logger.warning(f"[{region}] Deleting VPC {vpcid}")
        ec2 = self.auth.get_boto3_resource("ec2", region)
        ec2client = ec2.meta.client
        vpc = ec2.Vpc(vpcid)
        # detach and delete all gateways associated with the vpc
        for gw in vpc.internet_gateways.all():
            vpc.detach_internet_gateway(InternetGatewayId=gw.id)
            gw.delete()
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
        # finally, delete the vpc
        ec2client.delete_vpc(VpcId=vpcid)

    def add_ip_to_security_group(self, aws_region: str):
        """Add IP to security group. If security group ID is None, use group named skylark (create if not exists)."""

        @lockutils.synchronized(f"aws_add_ip_to_security_group_{aws_region}", external=True, lock_path="/tmp/skylark_locks")
        def fn():
            self.make_vpc(aws_region)
            sg = self.get_security_group(aws_region)
            try:
                sg.authorize_ingress(
                    IpPermissions=[{"IpProtocol": "-1", "FromPort": -1, "ToPort": -1, "IpRanges": [{"CidrIp": "0.0.0.0/0"}]}]
                )
            except botocore.exceptions.ClientError as e:
                if not str(e).endswith("already exists"):
                    raise e

        return fn()

    def provision_instance(
        self,
        region: str,
        instance_class: str,
        name: Optional[str] = None,
        # ami_id: Optional[str] = None,
        tags={"skylark": "true"},
        ebs_volume_size: int = 128,
    ) -> AWSServer:
        assert not region.startswith("aws:"), "Region should be AWS region"
        if name is None:
            name = f"skylark-aws-{str(uuid.uuid4()).replace('-', '')}"
        ec2 = self.auth.get_boto3_resource("ec2", region)
        vpc = self.get_vpc(region)
        assert vpc is not None, "No VPC found"
        subnets = list(vpc.subnets.all())
        assert len(subnets) > 0, "No subnets found"

        def start_instance():
            # todo instance-initiated-shutdown-behavior terminate
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
                        "SubnetId": subnets[0].id,
                        "AssociatePublicIpAddress": True,
                        "DeleteOnTermination": True,
                    }
                ],
            )

        instance = retry_backoff(start_instance, initial_backoff=1)
        assert len(instance) == 1, f"Expected 1 instance, got {len(instance)}"
        instance[0].wait_until_running()
        server = AWSServer(f"aws:{region}", instance[0].id)
        server.wait_for_ready()
        return server
