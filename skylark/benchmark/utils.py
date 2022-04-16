from typing import Dict, Iterable, List, Optional, Tuple

from skylark.utils import logger
from skylark.compute.aws.aws_cloud_provider import AWSCloudProvider
from skylark.compute.aws.aws_server import AWSServer
from skylark.compute.azure.azure_cloud_provider import AzureCloudProvider
from skylark.compute.azure.azure_server import AzureServer
from skylark.compute.cloud_providers import CloudProvider
from skylark.compute.gcp.gcp_cloud_provider import GCPCloudProvider
from skylark.compute.gcp.gcp_server import GCPServer
from skylark.compute.server import Server, ServerState
from skylark.utils.utils import do_parallel


def refresh_instance_list(provider: CloudProvider, region_list: Iterable[str] = (), instance_filter=None, n=-1) -> Dict[str, List[Server]]:
    if instance_filter is None:
        instance_filter = {"tags": {"skylark": "true"}}
    results = do_parallel(lambda region: provider.get_matching_instances(region=region, **instance_filter), region_list, progress_bar=False, n=n)
    return {r: ilist for r, ilist in results if ilist}


def split_list(l):
    pairs = set(l)
    groups = []
    elems_in_last_group = set()
    while pairs:
        group = []
        for x, y in pairs:
            if x not in elems_in_last_group and y not in elems_in_last_group:
                group.append((x, y))
                elems_in_last_group.add(x)  
                elems_in_last_group.add(y)
        groups.append(group)
        elems_in_last_group = set()
        pairs -= set(group)
    return groups


def provision(
    aws: AWSCloudProvider,
    azure: AzureCloudProvider,
    gcp: GCPCloudProvider,
    aws_regions_to_provision: List[str],
    azure_regions_to_provision: List[str],
    gcp_regions_to_provision: List[str],
    aws_instance_class: str,
    azure_instance_class: str,
    gcp_instance_class: str,
    gcp_use_premium_network: bool = True,
    log_dir: Optional[str] = None,
) -> Tuple[Dict[str, List[AWSServer]], Dict[str, List[AzureServer]], Dict[str, List[GCPServer]]]:
    """Provision list of instances in AWS, Azure, and GCP in each specified region."""
    aws_instances = {}
    azure_instances = {}
    gcp_instances = {}

    # TODO: It might be significantly faster to provision AWS, Azure, and GCP concurrently (e.g., using threads)

    if len(aws_regions_to_provision) > 0:
        logger.info(f"Provisioning AWS instances in {aws_regions_to_provision}")
        aws_instance_filter = {
            "tags": {"skylark": "true"},
            "instance_type": aws_instance_class,
            "state": [ServerState.PENDING, ServerState.RUNNING],
        }
        do_parallel(aws.add_ip_to_security_group, aws_regions_to_provision, progress_bar=True, desc="add IP to aws security groups")
        aws_instances = refresh_instance_list(aws, aws_regions_to_provision, aws_instance_filter)
        missing_aws_regions = set(aws_regions_to_provision) - set(aws_instances.keys())
        if missing_aws_regions:
            logger.info(f"(AWS) provisioning missing regions: {missing_aws_regions}")
            aws_provisioner = lambda r: aws.provision_instance(r, aws_instance_class)
            results = do_parallel(aws_provisioner, missing_aws_regions, progress_bar=True, desc="provision aws")
            for region, result in results:
                aws_instances[region] = [result]
            aws_instances = refresh_instance_list(aws, aws_regions_to_provision, aws_instance_filter)

    if len(azure_regions_to_provision) > 0:
        logger.info(f"Provisioning Azure instances in {azure_regions_to_provision}")
        azure_instance_filter = {
            "tags": {"skylark": "true"},
            "instance_type": azure_instance_class,
            "state": [ServerState.PENDING, ServerState.RUNNING],
        }
        azure_instances = refresh_instance_list(azure, azure_regions_to_provision, azure_instance_filter)
        missing_azure_regions = set(azure_regions_to_provision) - set(azure_instances.keys())
        if missing_azure_regions:
            logger.info(f"(Azure) provisioning missing regions: {missing_azure_regions}")

            def azure_provisioner(r):
                try:
                    return azure.provision_instance(r, azure_instance_class)
                except Exception as e:
                    logger.error(f"Failed to provision Azure instance in {r}: {e}")
                    logger.error(f"Skipping region {r}")
                    return None

            results = do_parallel(azure_provisioner, missing_azure_regions, progress_bar=True, desc="provision Azure")
            for region, result in results:
                assert region not in azure_instances
                if result is not None:
                    azure_instances[region] = [result]
            azure_instances = refresh_instance_list(azure, azure_regions_to_provision, azure_instance_filter)

    if len(gcp_regions_to_provision) > 0:
        logger.info(f"Provisioning GCP instances in {gcp_regions_to_provision}")
        gcp_instance_filter = {
            "tags": {"skylark": "true"},
            "instance_type": gcp_instance_class,
            "state": [ServerState.PENDING, ServerState.RUNNING],
            "network_tier": "PREMIUM" if gcp_use_premium_network else "STANDARD",
        }
        gcp.create_ssh_key()
        gcp.configure_default_network()
        gcp.configure_default_firewall()
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
                    gcp.provision_instance(r, gcp_instance_class, premium_network=gcp_use_premium_network)
                except Exception as e:
                    logger.error(f"Failed to provision GCP instance in {r}: {e}")
                    logger.error(f"Skipping region {r}")
                    return None

            results = do_parallel(
                gcp_provisioner, missing_gcp_regions, progress_bar=True, desc=f"provision GCP (premium network = {gcp_use_premium_network})"
            )
            for region, result in results:
                gcp_instances[region] = [result]
            gcp_instances = refresh_instance_list(gcp, gcp_regions_to_provision, gcp_instance_filter)

    # init log files
    def init(i: Server):
        i.wait_for_ready()
        i.init_log_files(log_dir)

    all_instances = (
        [i for ilist in aws_instances.values() for i in ilist]
        + [i for ilist in azure_instances.values() for i in ilist]
        + [i for ilist in gcp_instances.values() for i in ilist]
    )
    do_parallel(init, all_instances, progress_bar=True, desc="Provisioning init")
    return aws_instances, azure_instances, gcp_instances  # pytype: disable=bad-return-type
