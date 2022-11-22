import json
import time
import uuid
from multiprocessing import BoundedSemaphore

from typing import List, Optional

from skyplane import exceptions as skyplane_exceptions
from skyplane.compute.aws.aws_auth import AWSAuthentication
from skyplane.compute.aws.aws_key_manager import AWSKeyManager
from skyplane.compute.aws.aws_network import AWSNetwork
from skyplane.compute.aws.aws_pricing import AWSPricing
from skyplane.compute.aws.aws_server import AWSServer
from skyplane.compute.cloud_provider import CloudProvider
from skyplane.utils import imports, logger
from skyplane.utils.fn import do_parallel, wait_for


class AWSCloudProvider(CloudProvider):
    pricing = AWSPricing()

    def __init__(
        self,
        key_prefix: str = "skyplane",
        auth: Optional[AWSAuthentication] = None,
        key_manager: Optional[AWSKeyManager] = None,
        network: Optional[AWSNetwork] = None,
    ):
        super().__init__()
        self.key_prefix = key_prefix
        self.auth = auth if auth else AWSAuthentication()
        self.network = network if network else AWSNetwork(self.auth)
        self.key_manager = key_manager if key_manager else AWSKeyManager(self.auth)
        self.provisioning_semaphore = BoundedSemaphore(16)

    @property
    def name(self):
        return "aws"

    @staticmethod
    def region_list() -> List[str]:
        return AWSAuthentication.get_region_config()

    @classmethod
    def get_transfer_cost(cls, src_key, dst_key, premium_tier=True):
        assert src_key.startswith("aws:")
        return cls.pricing.get_transfer_cost(src_key, dst_key, premium_tier)

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

    def setup_global(self, iam_name: str = "skyplane_gateway", attach_policy_arn: Optional[str] = None):
        # Create IAM role if it doesn't exist and grant managed role if given.
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

    def setup_region(self, region: str):
        # set up network
        self.network.make_vpc(region)
        self.network.add_ssh_to_security_group(region)

        # set up keys
        self.key_manager.ensure_key_exists(region, f"{self.key_prefix}-{region}")

    def teardown_global(self):
        def list_instance_profiles(prefix: Optional[str] = None):
            paginator = self.auth.get_boto3_client("iam").get_paginator("list_instance_profiles")
            matched_names = []
            for page in paginator.paginate():
                for profile in page["InstanceProfiles"]:
                    if prefix is None or profile["InstanceProfileName"].startswith(prefix):
                        matched_names.append(profile["InstanceProfileName"])
            return matched_names

        def delete_instance_profile(profile_name: str):
            profile = self.auth.get_boto3_resource("iam").InstanceProfile(profile_name)
            for role in profile.roles:
                profile.remove_role(RoleName=role.name)
            # delete the instance profile
            profile.delete()

        vpcs = do_parallel(self.network.get_vpcs, self.region_list(), desc="Querying VPCs", spinner=True)
        args = [(x[0], vpc.id) for x in vpcs for vpc in x[1]]
        do_parallel(lambda x: self.network.remove_sg_ips(*x), args, desc="Removing IPs from VPCs", spinner=True, spinner_persist=True)
        profiles = list_instance_profiles(prefix="skyplane-aws")
        if profiles:
            do_parallel(delete_instance_profile, profiles, desc="Deleting instance profiles", spinner=True, spinner_persist=True, n=4)

    def add_ips_to_security_group(self, aws_region: str, ips: Optional[List[str]] = None):
        """Add IPs to security group. If security group ID is None, use group named skyplane (create if not exists). If ip is None, authorize all IPs."""
        self.network.add_ips_to_security_group(aws_region, ips)

    def remove_ips_from_security_group(self, aws_region: str, ips: List[str]):
        """Remove IP from security group. If security group ID is None, return."""
        self.network.remove_ips_from_security_group(aws_region, ips)

    @imports.inject("botocore.exceptions", pip_extra="aws")
    def provision_instance(
        exceptions,
        self,
        region: str,
        instance_class: str,
        disk_size: int = 32,
        use_spot_instances: bool = False,
        name: Optional[str] = None,
        tags={"skyplane": "true"},
        aws_iam_name: str = "skyplane_gateway",
        instance_os: str = "ecs-aws-linux-2",
    ) -> AWSServer:
        assert not region.startswith("aws:"), "Region should be AWS region"
        if name is None:
            name = f"skyplane-aws-{str(uuid.uuid4().hex[:8])}"
        iam_instance_profile_name = f"{name}_profile"

        # set default image used for provisioning instances in AWS
        if instance_os == "ubuntu":
            image_id = "resolve:ssm:/aws/service/canonical/ubuntu/server/20.04/stable/current/amd64/hvm/ebs-gp2/ami-id"
        elif instance_os == "ecs-aws-linux-2":
            image_id = "resolve:ssm:/aws/service/ecs/optimized-ami/amazon-linux-2/recommended/image_id"
        else:
            raise ValueError(f"Provisioning in {region}: instance OS {instance_os} not supported")

        with self.provisioning_semaphore:
            iam = self.auth.get_boto3_client("iam", region)
            ec2 = self.auth.get_boto3_resource("ec2", region)
            ec2_client = self.auth.get_boto3_client("ec2", region)
            vpcs = self.network.get_vpcs(region)
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
                    iam.get_role(RoleName=aws_iam_name)
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
            iam.add_role_to_instance_profile(InstanceProfileName=iam_instance_profile_name, RoleName=aws_iam_name)
            wait_for(check_instance_profile, timeout=60, interval=0.5)

            def start_instance(subnet_id: str):
                if use_spot_instances:
                    market_options = {"MarketType": "spot"}
                else:
                    market_options = {}
                return ec2.create_instances(
                    ImageId=image_id,
                    InstanceType=instance_class,
                    MinCount=1,
                    MaxCount=1,
                    KeyName=f"{self.key_prefix}-{region}",
                    TagSpecifications=[
                        {
                            "ResourceType": "instance",
                            "Tags": [{"Key": "Name", "Value": name}] + [{"Key": k, "Value": v} for k, v in tags.items()],
                        }
                    ],
                    BlockDeviceMappings=[
                        {
                            "DeviceName": "/dev/sda1",
                            "Ebs": {"DeleteOnTermination": True, "VolumeSize": disk_size, "VolumeType": "gp2"},
                        }
                    ],
                    NetworkInterfaces=[
                        {
                            "DeviceIndex": 0,
                            "Groups": [self.network.get_security_group(region).group_id],
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
                        raise
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
            try:
                instance[0].wait_until_running()
            except KeyboardInterrupt:
                logger.fs.warning(f"Terminating instance {instance[0].id} due to keyboard interrupt")
                instance[0].terminate()
                raise
            return AWSServer(f"aws:{region}", instance[0].id)
