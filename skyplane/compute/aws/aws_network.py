from typing import List, Optional

from skyplane.compute.aws.aws_auth import AWSAuthentication
from skyplane.utils import imports, logger
from skyplane.utils.fn import do_parallel


class AWSNetwork:
    def __init__(self, auth: AWSAuthentication, vpc_name="skyplane", sg_name="skyplane"):
        self.auth = auth
        self.vpc_name = vpc_name
        self.sg_name = sg_name

    def get_security_group(self, region: str):
        ec2 = self.auth.get_boto3_resource("ec2", region)
        vpcs = list(ec2.vpcs.filter(Filters=[{"Name": "tag:Name", "Values": [self.vpc_name]}]).all())
        assert len(vpcs) == 1, f"Found {len(vpcs)} vpcs with name {self.vpc_name}"
        sgs = [sg for sg in vpcs[0].security_groups.all() if sg.group_name == self.sg_name]
        assert len(sgs) == 1, f"Found {len(sgs)} security groups with name {self.sg_name} in vpc {vpcs[0].id}"
        return sgs[0]

    def get_vpcs(self, region: str):
        ec2 = self.auth.get_boto3_resource("ec2", region)
        vpcs = list(ec2.vpcs.filter(Filters=[{"Name": "tag:Name", "Values": [self.vpc_name]}]).all())
        if len(vpcs) == 0:
            return []
        else:
            return vpcs

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

    @imports.inject("botocore.exceptions", pip_extra="aws")
    def make_vpc(exceptions, self, region: str):
        ec2 = self.auth.get_boto3_resource("ec2", region)
        ec2client = ec2.meta.client
        vpcs = list(ec2.vpcs.filter(Filters=[{"Name": "tag:Name", "Values": [self.vpc_name]}]).all())

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
            matching_vpc.create_tags(Tags=[{"Key": "Name", "Value": self.vpc_name}])
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
            ec2.create_security_group(
                GroupName="skyplane",
                Description="Default security group for Skyplane VPC",
                VpcId=matching_vpc.id,
                TagSpecifications=tagSpecifications,
            )
        return matching_vpc

    @imports.inject("botocore.exceptions", pip_extra="aws")
    def add_ips_to_security_group(exceptions, self, aws_region: str, ips: Optional[List[str]] = None):
        """Add IPs to security group. If security group ID is None, use group named skyplane (create if not exists). If ip is None, authorize all IPs."""
        sg = self.get_security_group(aws_region)
        try:
            logger.fs.debug(f"[aws_network]:{aws_region} Adding IPs {ips} to security group {sg.group_name}")
            if ips is None:
                sg.authorize_ingress(
                    IpPermissions=[{"IpProtocol": "-1", "FromPort": -1, "ToPort": -1, "IpRanges": [{"CidrIp": f"0.0.0.0/0"}]}]
                )
            else:
                sg.authorize_ingress(
                    IpPermissions=[
                        {"IpProtocol": "-1", "FromPort": -1, "ToPort": -1, "IpRanges": [{"CidrIp": f"{ip}/32" if ip else "0.0.0.0/0"}]}
                        for ip in ips
                    ]
                )

        except exceptions.ClientError as e:
            if str(e).endswith("already exists") or str(e).endswith("already exist"):
                logger.warn(f"[aws_network]:{aws_region} Error adding IPs to security group, since it already exits: {e}")
            else:
                logger.error(f"[aws_network]:{aws_region} Error adding IPs {ips} to security group {sg.group_name}")
                logger.fs.exception(e)
                raise e from None

    @imports.inject("botocore.exceptions", pip_extra="aws")
    def remove_ips_from_security_group(exceptions, self, aws_region: str, ips: List[str]):
        """Remove IP from security group. If security group ID is None, return."""
        # Remove instance IP from security group
        sg = self.get_security_group(aws_region)
        try:
            for rule in sg.ip_permissions:
                if any([ip_range.get("CidrIp", "").split("/")[0] in ips for ip_range in rule.get("IpRanges", [])]):
                    response = sg.revoke_ingress(IpPermissions=[rule])
                    logger.fs.debug(
                        f"[aws_network]:{aws_region} Removing rule {rule} from security group {sg.group_name}, got response {response}"
                    )
        except exceptions.ClientError as e:
            logger.fs.error(f"[aws_network]:{aws_region} Error removing IPs {ips} from security group {sg.group_name}: {e}")
            if "The specified rule does not exist in this security group." not in str(e):
                logger.warn(f"[aws_network]:{aws_region} Error removing IPs from security group: {e}")

    @imports.inject("botocore.exceptions", pip_extra="aws")
    def add_ssh_to_security_group(exceptions, self, aws_region: str, ip: str = "0.0.0.0/0", port=22):
        sg = self.get_security_group(aws_region)
        try:
            for rule in sg.ip_permissions:
                if "FromPort" in rule and rule["FromPort"] <= port and "ToPort" in rule and rule["ToPort"] >= port:
                    for ipr in rule["IpRanges"]:
                        if ipr["CidrIp"] == ip:
                            logger.fs.debug(
                                f"[aws_network]:{aws_region} Found existing rule for {ip}:{port} in {sg.group_name}, not adding again"
                            )
                            return
            logger.fs.debug(f"[aws_network]:{aws_region} Authorizing SSH {ip}:{port} in {sg.group_name}")
            sg.authorize_ingress(IpPermissions=[{"IpProtocol": "tcp", "FromPort": port, "ToPort": port, "IpRanges": [{"CidrIp": ip}]}])
        except exceptions.ClientError as e:
            if str(e).endswith("already exists") or str(e).endswith("already exist"):
                logger.warn(f"[aws_network]:{aws_region} Error adding IPs to security group, since it already exits: {e}")
            else:
                logger.error(f"[aws_network]:{aws_region} Error adding SSH port {port} for IPs {ip} to security group {sg.group_name}")
                logger.fs.exception(e)
                raise e from None
