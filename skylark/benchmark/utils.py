from typing import Dict, List, Tuple

from loguru import logger

from skylark.compute.aws.aws_cloud_provider import AWSCloudProvider
from skylark.compute.aws.aws_server import AWSServer
from skylark.compute.cloud_providers import CloudProvider
from skylark.compute.gcp.gcp_cloud_provider import GCPCloudProvider
from skylark.compute.gcp.gcp_server import GCPServer
from skylark.compute.server import Server, ServerState
from skylark.utils import do_parallel


def refresh_instance_list(provider: CloudProvider, region_list, instance_filter=None) -> Dict[str, List[Server]]:
    if instance_filter is None:
        instance_filter = {"tags": {"skylark": "true"}}
    instances = {}
    fn = lambda region: provider.get_matching_instances(region, **instance_filter)
    results = do_parallel(fn, region_list, progress_bar=True)
    for region, instance_list in results:
        if instance_list:
            instances[region] = instance_list
    return instances


def provision(
    aws: AWSCloudProvider,
    gcp: GCPCloudProvider,
    aws_regions_to_provision: List[str],
    gcp_regions_to_provision: List[str],
    aws_instance_class: str,
    gcp_instance_class: str,
    setup_script: object = None,
) -> Tuple[Dict[str, List[AWSServer]], Dict[str, List[GCPServer]]]:
    """Provision list of instances in AWS and GCP in each specified region."""
    aws_instance_filter = {
        "tags": {"skylark": "true"},
        "instance_type": aws_instance_class,
        "state": [ServerState.PENDING, ServerState.RUNNING],
    }
    gcp_instance_filter = {
        "tags": {"skylark": "true"},
        "instance_type": gcp_instance_class,
        "state": [ServerState.PENDING, ServerState.RUNNING],
    }

    # setup
    logger.info("(aws) configuring security group")
    do_parallel(aws.add_ip_to_security_group, aws_regions_to_provision, progress_bar=True)
    logger.info("(gcp) creating ssh key")
    gcp.create_ssh_key()
    logger.info("(gcp) configuring firewall")
    gcp.configure_default_firewall()

    # regions to provision
    logger.info("refreshing region list")
    aws_instances = refresh_instance_list(aws, aws_regions_to_provision, aws_instance_filter)
    gcp_instances = refresh_instance_list(gcp, gcp_regions_to_provision, gcp_instance_filter)

    # provision missing regions using do_parallel
    missing_aws_regions = set(aws_regions_to_provision) - set(aws_instances.keys())
    missing_gcp_regions = set(gcp_regions_to_provision) - set(gcp_instances.keys())
    if missing_aws_regions:
        logger.info(f"(aws) provisioning missing regions: {missing_aws_regions}")
        aws_provisioner = lambda r: aws.provision_instance(r, aws_instance_class)
        results = do_parallel(aws_provisioner, missing_aws_regions, progress_bar=True)
        for region, result in results:
            aws_instances[region] = [result]
            logger.info(f"(aws:{region}) provisioned {result}, waiting for ready")
            result.wait_for_ready()
            logger.info(f"(aws:{region}) ready")
        aws_instances = refresh_instance_list(aws, aws_regions_to_provision, aws_instance_filter)
    if missing_gcp_regions:
        logger.info(f"(gcp) provisioning missing regions: {missing_gcp_regions}")
        gcp_provisioner = lambda r: gcp.provision_instance(r, gcp_instance_class)
        results = do_parallel(gcp_provisioner, missing_gcp_regions, progress_bar=True)
        for region, result in results:
            gcp_instances[region] = [result]
            logger.info(f"(gcp:{region}) provisioned {result}")
        gcp_instances = refresh_instance_list(gcp, gcp_regions_to_provision, gcp_instance_filter)

    # run setup script on each instance (use do_parallel)
    if setup_script:
        run_script = lambda i: i.copy_and_run_script(setup_script)
        do_parallel(run_script, [i for _, ilist in aws_instances.items() for i in ilist], progress_bar=True)
        do_parallel(run_script, [i for _, ilist in gcp_instances.items() for i in ilist], progress_bar=True)
    return aws_instances, gcp_instances
