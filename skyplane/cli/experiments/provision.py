from functools import partial
from typing import Dict, Iterable, List
from typing import Optional, Tuple

from skyplane import compute
from skyplane.utils import logger
from skyplane.utils.fn import do_parallel
from skyplane.utils.timer import Timer


def refresh_instance_list(
    provider: compute.CloudProvider, region_list: Iterable[str] = (), instance_filter=None, n=-1
) -> Dict[str, List[compute.Server]]:
    if instance_filter is None:
        instance_filter = {"tags": {"skyplane": "true"}}
    results = do_parallel(
        lambda region: provider.get_matching_instances(region=region, **instance_filter),
        region_list,
        spinner=True,
        n=n,
        desc="Querying clouds for active instances",
    )
    return {r: ilist for r, ilist in results if ilist}


def provision(
    aws: compute.AWSCloudProvider,
    azure: compute.AzureCloudProvider,
    gcp: compute.GCPCloudProvider,
    ibmcloud: compute.IBMCloudProvider,
    aws_regions_to_provision: List[str],
    azure_regions_to_provision: List[str],
    gcp_regions_to_provision: List[str],
    ibmcloud_regions_to_provision: List[str],
    aws_instance_class: str,
    azure_instance_class: str,
    gcp_instance_class: str,
    ibmcloud_instance_class: str,
    aws_instance_os: str = "ecs-aws-linux-2",
    gcp_instance_os: str = "cos",
    azure_instance_os: str = "ubuntu",
    gcp_use_premium_network: bool = True,
    log_dir: Optional[str] = None,
) -> Tuple[
    Dict[str, List[compute.AWSServer]],
    Dict[str, List[compute.AzureServer]],
    Dict[str, List[compute.GCPServer]],
    Dict[str, List[compute.IBMCloudServer]],
]:
    """Provision list of instances in AWS, Azure, and GCP in each specified region."""
    aws_instances = {}
    azure_instances = {}
    gcp_instances = {}
    ibmcloud_instances = {}

    # TODO: It might be significantly faster to provision AWS, Azure, and GCP concurrently (e.g., using threads)

    jobs = []
    jobs.append(partial(aws.setup_global, attach_policy_arn="arn:aws:iam::aws:policy/AmazonS3FullAccess"))
    if aws_regions_to_provision:
        for r in set(aws_regions_to_provision):
            jobs.append(partial(aws.setup_region, r))
    if azure_regions_to_provision:
        jobs.append(azure.create_ssh_key)
        jobs.append(azure.set_up_resource_group)
    if gcp_regions_to_provision:
        jobs.append(gcp.setup_global)
    if ibmcloud_regions_to_provision:
        for r in set(ibmcloud_regions_to_provision):
            jobs.append(partial(ibmcloud.setup_region, r.split(":")[1]))

    with Timer("Cloud SSH key initialization"):
        do_parallel(lambda fn: fn(), jobs)

    if len(aws_regions_to_provision) > 0:
        logger.info(f"Provisioning AWS instances in {aws_regions_to_provision}")
        aws_instance_filter = {
            "tags": {"skyplane": "true"},
            "instance_type": aws_instance_class,
            "state": [compute.ServerState.PENDING, compute.ServerState.RUNNING],
        }
        do_parallel(aws.add_ips_to_security_group, aws_regions_to_provision, spinner=True, desc="Add IP to aws security groups")
        aws_instances = refresh_instance_list(aws, aws_regions_to_provision, aws_instance_filter)
        missing_aws_regions = set(aws_regions_to_provision) - set(aws_instances.keys())
        if missing_aws_regions:
            logger.info(f"(AWS) provisioning missing regions: {missing_aws_regions}")
            aws_provisioner = lambda r: aws.provision_instance(r, aws_instance_class, instance_os=aws_instance_os)
            results = do_parallel(aws_provisioner, missing_aws_regions, spinner=True, desc="provision aws")
            for region, result in results:
                aws_instances[region] = [result]
            aws_instances = refresh_instance_list(aws, aws_regions_to_provision, aws_instance_filter)

    if len(azure_regions_to_provision) > 0:
        logger.info(f"Provisioning Azure instances in {azure_regions_to_provision}")
        azure_instance_filter = {
            "tags": {"skyplane": "true"},
            "instance_type": azure_instance_class,
            "state": [compute.ServerState.PENDING, compute.ServerState.RUNNING],
        }
        azure_instances = refresh_instance_list(azure, azure_regions_to_provision, azure_instance_filter)
        missing_azure_regions = set(azure_regions_to_provision) - set(azure_instances.keys())
        if missing_azure_regions:
            logger.info(f"(Azure) provisioning missing regions: {missing_azure_regions}")

            def azure_provisioner(r):
                try:
                    return azure.provision_instance(r, azure_instance_class, instance_os=azure_instance_os)
                except Exception as e:
                    logger.error(f"Failed to provision Azure instance in {r}: {e}")
                    logger.error(f"Skipping region {r}")
                    return None

            results = do_parallel(azure_provisioner, missing_azure_regions, spinner=True, desc="provision Azure")
            for region, result in results:
                assert region not in azure_instances
                if result is not None:
                    azure_instances[region] = [result]
            azure_instances = refresh_instance_list(azure, azure_regions_to_provision, azure_instance_filter)

    if len(gcp_regions_to_provision) > 0:
        logger.info(f"Provisioning GCP instances in {gcp_regions_to_provision}")
        gcp_instance_filter = {
            "tags": {"skyplane": "true"},
            "instance_type": gcp_instance_class,
            "state": [compute.ServerState.PENDING, compute.ServerState.RUNNING],
            "network_tier": "PREMIUM" if gcp_use_premium_network else "STANDARD",
        }
        gcp_instances = refresh_instance_list(gcp, gcp_regions_to_provision, gcp_instance_filter, n=4)
        missing_gcp_regions = set(gcp_regions_to_provision) - set(gcp_instances.keys())

        # filter duplicate regions from list and select lexicographically smallest region by zone
        zone_map = {}
        for region in missing_gcp_regions:
            continent, region_name, zone = region.split("-")
            region = f"{continent}-{region_name}"
            if region not in zone_map:
                zone_map[region] = []
            zone_map[region].append(zone)
        missing_gcp_regions = []
        for region, zones in zone_map.items():
            missing_gcp_regions.append(f"{region}-{min(zones)}")

        if missing_gcp_regions:
            logger.info(f"(GCP) provisioning missing regions: {missing_gcp_regions}")

            def gcp_provisioner(r):
                try:
                    gcp.provision_instance(r, gcp_instance_class, gcp_premium_network=gcp_use_premium_network, instance_os=gcp_instance_os)
                except Exception as e:
                    logger.error(f"Failed to provision GCP instance in {r}: {e}")
                    logger.error(f"Skipping region {r}")
                    return None

            results = do_parallel(
                gcp_provisioner, missing_gcp_regions, spinner=True, desc=f"provision GCP (premium network = {gcp_use_premium_network})"
            )
            for region, result in results:
                gcp_instances[region] = [result]
            gcp_instances = refresh_instance_list(gcp, gcp_regions_to_provision, gcp_instance_filter)

    if len(ibmcloud_regions_to_provision) > 0:
        logger.info(f"Provisioning IBM Cloud instances in {ibmcloud_regions_to_provision}")
        ibmcloud_instance_filter = {
            "tags": {"skyplane": "true"},
            "instance_type": ibmcloud_instance_class,
            "state": [compute.ServerState.PENDING, compute.ServerState.RUNNING],
        }
        do_parallel(aws.add_ips_to_security_group, ibmcloud_regions_to_provision, spinner=True, desc="Add IP to IBM Cloud security groups")
        ibmcloud_instances = refresh_instance_list(ibmcloud, ibmcloud_regions_to_provision, ibmcloud_instance_filter)
        missing_ibmcloud_regions = set(ibmcloud_regions_to_provision) - set(ibmcloud_instances.keys())
        if missing_ibmcloud_regions:
            logger.info(f"(IBM Cloud) provisioning missing regions: {missing_ibmcloud_regions}")
            ibmcloud_provisioner = lambda r: ibmcloud.provision_instance(r, ibmcloud_instance_class)
            results = do_parallel(ibmcloud_provisioner, missing_ibmcloud_regions, spinner=True, desc="provision IBM Cloud")
            for region, result in results:
                aws_instances[region] = [result]
            aws_instances = refresh_instance_list(ibmcloud, ibmcloud_regions_to_provision, ibmcloud_instance_filter)

    # init log files
    def init(i: compute.Server):
        i.init_log_files(log_dir)

    all_instances = (
        [i for ilist in aws_instances.values() for i in ilist]
        + [i for ilist in azure_instances.values() for i in ilist]
        + [i for ilist in gcp_instances.values() for i in ilist]
        + [i for ilist in ibmcloud_instances.values() for i in ilist]
    )
    do_parallel(init, all_instances, spinner=True, desc="Provisioning init")
    return aws_instances, azure_instances, gcp_instances, ibmcloud_instances  # pytype: disable=bad-return-type
