import functools
import json
import os
import time
import uuid
from multiprocessing import BoundedSemaphore
from pathlib import Path
from typing import List, Optional

from skyplane import exceptions as skyplane_exceptions
from skyplane import key_root
from skyplane import skyplane_root
from skyplane.compute.aws.aws_auth import AWSAuthentication
from skyplane.compute.aws.aws_server import AWSServer
from skyplane.compute.cloud_providers import CloudProvider
from skyplane.utils import logger, imports
from skyplane.utils.fn import do_parallel, wait_for

try:
    import pandas as pd
except ImportError:
    pd = None
    logger.warning("pandas not installed, will not be able to load transfer costs")


class AWSCloudProvider(CloudProvider):
    def __init__(self):
        super().__init__()
        self.auth = AWSAuthentication()
        self.provisioning_semaphore = BoundedSemaphore(16)

    @property
    def name(self):
        return "aws"

    @staticmethod
    def region_list() -> List[str]:
        return AWSAuthentication.get_region_config()

    @staticmethod
    @functools.lru_cache(maxsize=None)
    def load_transfer_cost_df():
        return pd.read_csv(skyplane_root / "profiles" / "aws_transfer_costs.csv").set_index(["src", "dst"])

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

    @imports.inject("botocore.exceptions", pip_extra="aws")
    def get_instance_list(exceptions, self, region: str) -> List[AWSServer]:
        ec2 = self.auth.get_boto3_resource("ec2", region)
        valid_states = ["pending", "running", "stopped", "stopping"]
        instances = ec2.instances.filter(Filters=[{"Name": "instance-state-name", "Values": valid_states}])
        try:
            instance_ids = [i.id for i in instances]
        except exceptions.ClientError as e:
            logger.error(f"error provisioning in {region}: {e}")
            return []

        return [AWSServer(f"aws:{region}", i) for i in instance_ids]

    def get_security_group(self, region: str, vpc_name="skyplane", sg_name="skyplane"):
        ec2 = self.auth.get_boto3_resource("ec2", region)
        vpcs = list(ec2.vpcs.filter(Filters=[{"Name": "tag:Name", "Values": [vpc_name]}]).all())
        assert len(vpcs) == 1, f"Found {len(vpcs)} vpcs with name {vpc_name}"
        sgs = [sg for sg in vpcs[0].security_groups.all() if sg.group_name == sg_name]
        assert len(sgs) == 1, f"Found {len(sgs)} security groups with name {sg_name} in vpc {vpcs[0].id}"
        return sgs[0]

    def get_vpcs(self, region: str, vpc_name="skyplane"):
        ec2 = self.auth.get_boto3_resource("ec2", region)
        vpcs = list(ec2.vpcs.filter(Filters=[{"Name": "tag:Name", "Values": [vpc_name]}]).all())
        if len(vpcs) == 0:
            return []
        else:
            return vpcs

    @imports.inject("botocore.exceptions", pip_extra="aws")
    def make_vpc(exceptions, self, region: str, vpc_name="skyplane"):
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
                and any(sg.group_name == "skyplane" for sg in vpc.security_groups.all())
            ):
                matching_vpc = vpc
                # delete all other vpcs
                for vpc in vpcs:
                    if vpc != matching_vpc:
                        try:
                            self.delete_vpc(region, vpc.id)
                        except exceptions.ClientError as e:
                            logger.warning(f"Failed to delete VPC {vpc.id} in {region}: {e}")
                break

        # make vpc if none found
        if matching_vpc is None:
            # delete old skyplane vpcs
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

            # make security group named "skyplane"
            tagSpecifications = [{"Tags": [{"Key": "skyplane", "Value": "true"}], "ResourceType": "security-group"}]
            sg = ec2.create_security_group(
                GroupName="skyplane",
                Description="Default security group for Skyplane VPC",
                VpcId=matching_vpc.id,
                TagSpecifications=tagSpecifications,
            )
        return matching_vpc

    def delete_vpc(self, region: str, vpcid: str):
        """Delete VPC, from https://gist.github.com/vernhart/c6a0fc94c0aeaebe84e5cd6f3dede4ce"""
        logger.fs.warning(f"[{region}] Deleting VPC {vpcid}")
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

    def remove_sg_ips(self, region: str, vpcid: str):
        """Remove all IPs from a security group"""
        ec2 = self.auth.get_boto3_resource("ec2", region)
        vpc = ec2.Vpc(vpcid)
        for sg in vpc.security_groups.all():
            if sg.group_name == "default":
                continue
            # revoke all ingress rules except for ssh
            for rule in sg.ip_permissions:
                if not (rule.get("IpProtocol") == "tcp" and rule.get("FromPort") == 22 and rule.get("ToPort") == 22):
                    sg.revoke_ingress(
                        IpPermissions=[{k: v for k, v in rule.items() if k in ["IpProtocol", "FromPort", "ToPort", "IpRanges"]}]
                    )

    def list_instance_profiles(self, prefix: Optional[str] = None):
        """List instance profile names in a region"""
        paginator = self.auth.get_boto3_client("iam").get_paginator("list_instance_profiles")
        matched_names = []
        for page in paginator.paginate():
            for profile in page["InstanceProfiles"]:
                if prefix is None or profile["InstanceProfileName"].startswith(prefix):
                    matched_names.append(profile["InstanceProfileName"])
        return matched_names

    def delete_instance_profile(self, profile_name: str):
        # remove all roles from the instance profile
        profile = self.auth.get_boto3_resource("iam").InstanceProfile(profile_name)
        for role in profile.roles:
            profile.remove_role(RoleName=role.name)
        # delete the instance profile
        profile.delete()

    def create_iam(self, iam_name: str = "skyplane_gateway", attach_policy_arn: Optional[str] = None):
        """Create IAM role if it doesn't exist and grant managed role if given."""
        iam = self.auth.get_boto3_client("iam")
        try:
            iam.get_role(RoleName=iam_name)
        except iam.exceptions.NoSuchEntityException:
            doc = {
                "Version": "2012-10-17",
                "Statement": [{"Effect": "Allow", "Principal": {"Service": "ec2.amazonaws.com"}, "Action": "sts:AssumeRole"}],
            }
            iam.create_role(RoleName=iam_name, AssumeRolePolicyDocument=json.dumps(doc), Tags=[{"Key": "skyplane", "Value": "true"}])
        if attach_policy_arn:
            iam.attach_role_policy(RoleName=iam_name, PolicyArn=attach_policy_arn)

    def authorize_client(self, aws_region: str, client_ip: str, port=22):
        sg = self.get_security_group(aws_region)

        # check if we already have a rule for this security group
        for rule in sg.ip_permissions:
            if "FromPort" in rule and rule["FromPort"] <= port and "ToPort" in rule and rule["ToPort"] >= port:
                for ipr in rule["IpRanges"]:
                    if ipr["CidrIp"] == client_ip:
                        logger.fs.debug(f"[AWS] Found existing rule for {client_ip}:{port} in {sg.group_name}, not adding again")
                        return
        logger.fs.debug(f"[AWS] Authorizing {client_ip}:{port} in {sg.group_name}")
        sg.authorize_ingress(IpPermissions=[{"IpProtocol": "tcp", "FromPort": port, "ToPort": port, "IpRanges": [{"CidrIp": client_ip}]}])

    @imports.inject("botocore.exceptions", pip_extra="aws")
    def add_ips_to_security_group(exceptions, self, aws_region: str, ips: Optional[List[str]] = None):
        """Add IPs to security group. If security group ID is None, use group named skyplane (create if not exists). If ip is None, authorize all IPs."""
        sg = self.get_security_group(aws_region)
        try:
            logger.fs.debug(f"[AWS] Adding IPs {ips} to security group {sg.group_name}")
            sg.authorize_ingress(
                IpPermissions=[
                    {"IpProtocol": "-1", "FromPort": -1, "ToPort": -1, "IpRanges": [{"CidrIp": f"{ip}/32" if ip else "0.0.0.0/0"}]}
                    for ip in ips
                ]
            )
        except exceptions.ClientError as e:
            if str(e).endswith("already exists") or str(e).endswith("already exist"):
                logger.warn(f"[AWS] Error adding IPs to security group, since it already exits: {e}")
            else:
                logger.error(f"[AWS] Error adding IPs {ips} to security group {sg.group_name}")
                logger.fs.exception(e)
                raise e

    @imports.inject("botocore.exceptions", pip_extra="aws")
    def remove_ips_from_security_group(exceptions, self, aws_region: str, ips: List[str]):
        """Remove IP from security group. If security group ID is None, return."""
        # Remove instance IP from security group
        sg = self.get_security_group(aws_region)
        try:
            logger.fs.debug(f"[AWS] Removing IPs {ips} from security group {sg.group_name}")
            sg.revoke_ingress(
                IpPermissions=[
                    {"IpProtocol": "tcp", "FromPort": 12000, "ToPort": 65535, "IpRanges": [{"CidrIp": ip + "/32"}]} for ip in ips
                ]
            )
        except exceptions.ClientError as e:
            logger.fs.error(f"[AWS] Error removing IPs {ips} from security group {sg.group_name}: {e}")
            if "The specified rule does not exist in this security group." not in str(e):
                logger.warn(f"[AWS] Error removing IPs from security group: {e}")

    def ensure_keyfile_exists(self, aws_region, prefix=key_root / "aws"):
        ec2 = self.auth.get_boto3_resource("ec2", aws_region)
        ec2_client = self.auth.get_boto3_client("ec2", aws_region)
        prefix = Path(prefix)
        key_name = f"skyplane-{aws_region}"
        local_key_file = prefix / f"{key_name}.pem"

        local_key_file.parent.mkdir(parents=True, exist_ok=True)
        if not local_key_file.exists():
            logger.fs.debug(f"[AWS] Creating keypair {key_name} in {aws_region}")
            if key_name in set(p["KeyName"] for p in ec2_client.describe_key_pairs()["KeyPairs"]):
                logger.fs.warning(f"Deleting key {key_name} in region {aws_region}")
                ec2_client.delete_key_pair(KeyName=key_name)
            key_pair = ec2.create_key_pair(KeyName=f"skyplane-{aws_region}", KeyType="rsa")
            with local_key_file.open("w") as f:
                key_str = key_pair.key_material
                if not key_str.endswith("\n"):
                    key_str += "\n"
                f.write(key_str)
            os.chmod(local_key_file, 0o600)
            logger.fs.info(f"Created key file {local_key_file}")

        return local_key_file

    @imports.inject("botocore.exceptions", pip_extra="aws")
    def provision_instance(
        exceptions,
        self,
        region: str,
        instance_class: str,
        name: Optional[str] = None,
        tags={"skyplane": "true"},
        ebs_volume_size: int = 128,
        iam_name: str = "skyplane_gateway",
        use_spot_instances: bool = False,
    ) -> AWSServer:
        assert not region.startswith("aws:"), "Region should be AWS region"
        if name is None:
            name = f"skyplane-aws-{str(uuid.uuid4()).replace('-', '')}"
        iam_instance_profile_name = f"{name}_profile"
        with self.provisioning_semaphore:
            iam = self.auth.get_boto3_client("iam", region)
            ec2 = self.auth.get_boto3_resource("ec2", region)
            ec2_client = self.auth.get_boto3_client("ec2", region)
            vpcs = self.get_vpcs(region)
            assert vpcs, "No VPC found"
            vpc = vpcs[0]

            # get subnet list
            def instance_class_supported(az):
                # describe_instance_type_offerings
                offerings_list = ec2_client.describe_instance_type_offerings(
                    LocationType="availability-zone", Filters=[{"Name": "location", "Values": [az]}]
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
            iam.create_instance_profile(InstanceProfileName=iam_instance_profile_name, Tags=[{"Key": "skyplane", "Value": "true"}])
            iam.add_role_to_instance_profile(InstanceProfileName=iam_instance_profile_name, RoleName=iam_name)
            wait_for(check_instance_profile, timeout=60, interval=0.5)

            def start_instance(subnet_id: str):
                if use_spot_instances:
                    market_options = {"MarketType": "spot"}
                else:
                    market_options = {}
                return ec2.create_instances(
                    ImageId="resolve:ssm:/aws/service/ecs/optimized-ami/amazon-linux-2/recommended/image_id",
                    InstanceType=instance_class,
                    MinCount=1,
                    MaxCount=1,
                    KeyName=f"skyplane-{region}",
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
                    InstanceMarketOptions=market_options,
                )

            backoff = 1
            max_retries = 8
            max_backoff = 8
            current_subnet_id = 0
            for i in range(max_retries):
                try:
                    instance = start_instance(subnets[current_subnet_id].id)
                    break
                except exceptions.ClientError as e:
                    if i == max_retries - 1:
                        raise e
                    elif "VcpuLimitExceeded" in str(e):
                        raise skyplane_exceptions.InsufficientVCPUException() from e
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
            server.wait_for_ssh_ready()
            return server
