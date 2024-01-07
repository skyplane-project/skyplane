import typer
from typing import List

from skyplane import compute
from skyplane.cli.experiments.provision import provision
from skyplane.compute.const_cmds import make_sysctl_tcp_tuning_command
from skyplane.utils import logger
from skyplane.utils.fn import do_parallel

all_aws_regions = compute.AWSCloudProvider.region_list()
all_azure_regions = compute.AzureCloudProvider.region_list()
all_gcp_regions = compute.GCPCloudProvider.region_list()
all_gcp_regions_standard = compute.GCPCloudProvider.region_list_standard()
all_ibmcloud_regions = compute.IBMCloudProvider.region_list()
from skyplane.compute.aws.aws_auth import AWSAuthentication


def aws_credentials():
    auth = AWSAuthentication()
    access_key, secret_key = auth.get_credentials()
    return access_key, secret_key


def create_instance(
    # regions
    aws_region_list: List[str] = typer.Option(all_aws_regions, "-aws"),
    azure_region_list: List[str] = typer.Option(all_azure_regions, "-azure"),
    gcp_region_list: List[str] = typer.Option(all_gcp_regions, "-gcp"),
    gcp_standard_region_list: List[str] = typer.Option(all_gcp_regions_standard, "-gcp-standard"),
    ibmcloud_region_list: List[str] = typer.Option(all_ibmcloud_regions, "-ibmcloud"),
    #
    enable_aws: bool = typer.Option(True),
    enable_azure: bool = typer.Option(False),
    enable_gcp: bool = typer.Option(False),
    enable_gcp_standard: bool = typer.Option(False),
    enable_ibmcloud: bool = typer.Option(False),
    # instances to provision
    aws_instance_class: str = typer.Option("m5.8xlarge", help="AWS instance class to use"),
    azure_instance_class: str = typer.Option("Standard_D32_v5", help="Azure instance class to use"),
    gcp_instance_class: str = typer.Option("n2-standard-32", help="GCP instance class to use"),
    ibmcloud_instance_class: str = typer.Option("bx2-2x8", help="IBM Cloud instance class to use"),
):
    def check_stderr(tup):
        assert tup[1].strip() == "", f"Command failed, err: {tup[1]}"

    # validate arguments
    # aws_region_list = aws_region_list if enable_aws else []
    # azure_region_list = azure_region_list if enable_azure else []
    # gcp_region_list = gcp_region_list if enable_gcp else []

    # aws_region_list = ["us-east-1", "us-west-1"]
    # gcp_region_list = ['me-west1-a','europe-north1-a',]
    aws_region_list = ["us-east-1"]
    # gcp_region_list = ["me-west1-a"]

    # validate AWS regions
    aws_region_list = aws_region_list if enable_aws else []
    azure_region_list = azure_region_list if enable_azure else []
    gcp_region_list = gcp_region_list if enable_gcp else []
    ibmcloud_region_list = ibmcloud_region_list if enable_ibmcloud else []
    if not enable_aws and not enable_azure and not enable_gcp and not enable_ibmcloud:
        logger.error("At least one of -aws, -azure, -gcp, -ibmcloud must be enabled.")
        raise typer.Abort()

    # validate AWS regions
    if not enable_aws:
        aws_region_list = []
    elif not all(r in all_aws_regions for r in aws_region_list):
        logger.error(f"Invalid AWS region list: {aws_region_list}")
        raise typer.Abort()

    # validate Azure regions
    if not enable_azure:
        azure_region_list = []
    elif not all(r in all_azure_regions for r in azure_region_list):
        logger.error(f"Invalid Azure region list: {azure_region_list}")
        raise typer.Abort()

    # validate GCP regions
    assert not enable_gcp_standard or enable_gcp, f"GCP is disabled but GCP standard is enabled"
    if not enable_gcp:
        gcp_region_list = []
    elif not all(r in all_gcp_regions for r in gcp_region_list):
        logger.error(f"Invalid GCP region list: {gcp_region_list}")
        raise typer.Abort()

    # validate GCP standard instances
    if not enable_gcp_standard:
        gcp_standard_region_list = []
    if not all(r in all_gcp_regions_standard for r in gcp_standard_region_list):
        logger.error(f"Invalid GCP standard region list: {gcp_standard_region_list}")
        raise typer.Abort()

    # validate IBM Cloud regions
    if not enable_ibmcloud:
        ibmcloud_region_list = []
    elif not all(r in all_ibmcloud_regions for r in ibmcloud_region_list):
        logger.error(f"Invalid IBM Cloud region list: {ibmcloud_region_list}")
        raise typer.Abort()

    # provision servers
    aws = compute.AWSCloudProvider()
    azure = compute.AzureCloudProvider()
    gcp = compute.GCPCloudProvider()
    ibmcloud = compute.IBMCloudProvider()

    aws_instances, azure_instances, gcp_instances, ibmcloud_instances = provision(
        aws=aws,
        azure=azure,
        gcp=gcp,
        ibmcloud=ibmcloud,
        aws_regions_to_provision=aws_region_list,
        azure_regions_to_provision=azure_region_list,
        gcp_regions_to_provision=gcp_region_list,
        ibmcloud_regions_to_provision=ibmcloud_region_list,
        aws_instance_class=aws_instance_class,
        azure_instance_class=azure_instance_class,
        gcp_instance_class=gcp_instance_class,
        ibmcloud_instance_class=ibmcloud_instance_class,
        aws_instance_os="ubuntu",
        gcp_instance_os="ubuntu",
        gcp_use_premium_network=True,
    )
    instance_list: List[compute.Server] = [i for ilist in aws_instances.values() for i in ilist]
    instance_list.extend([i for ilist in azure_instances.values() for i in ilist])
    instance_list.extend([i for ilist in gcp_instances.values() for i in ilist])

    # setup instances
    def setup(server: compute.Server):
        check_stderr(server.run_command("echo 'debconf debconf/frontend select Noninteractive' | sudo debconf-set-selections"))
        check_stderr(
            server.run_command(
                "sudo add-apt-repository universe;\
                (sudo apt-get update && sudo apt-get install python3-pip -y && sudo pip3 install awscli)"
            )
        )
        check_stderr(server.run_command(make_sysctl_tcp_tuning_command(cc="cubic")))
        server.run_command(
            f"aws configure set aws_access_key_id {aws_credentials()[0]}; aws configure set aws_secret_access_key {aws_credentials()[1]}"
        )

    do_parallel(setup, instance_list, spinner=True, n=-1, desc="Setup")

    with open("ssh_cmd.txt", "a") as f:
        for instance in instance_list:
            print("instance: ", instance.region_tag)
            ssh_cmd = instance.get_ssh_cmd()
            print(ssh_cmd)

            # Insert the '-o StrictHostKeyChecking=accept-new' option in the middle of the ssh command
            ssh_parts = ssh_cmd.split(" ", 1)
            modified_ssh_cmd = f"{ssh_parts[0]} -o StrictHostKeyChecking=accept-new {ssh_parts[1]}"
            f.write(modified_ssh_cmd + "\n")

        f.close()
