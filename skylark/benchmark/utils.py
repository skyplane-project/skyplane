from typing import Dict, List, Tuple

from loguru import logger

from skylark.compute.aws.aws_cloud_provider import AWSCloudProvider
from skylark.compute.aws.aws_server import AWSServer
from skylark.compute.cloud_providers import CloudProvider
from skylark.compute.gcp.gcp_cloud_provider import GCPCloudProvider
from skylark.compute.gcp.gcp_server import GCPServer
from skylark.compute.server import Server, ServerState
from skylark.utils import do_parallel


def refresh_instance_list(provider: CloudProvider, region_list=[None], instance_filter=None) -> Dict[str, List[Server]]:
    if instance_filter is None:
        instance_filter = {"tags": {"skylark": "true"}}
    results = do_parallel(
        lambda region: provider.get_matching_instances(region, **instance_filter),
        region_list,
        progress_bar=True,
        desc=f"refresh {provider.name}",
    )
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
    gcp: GCPCloudProvider,
    aws_regions_to_provision: List[str],
    gcp_regions_to_provision: List[str],
    aws_instance_class: str,
    gcp_instance_class: str,
    setup_script: object = None,
    log_dir: str = None,
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
    do_parallel(aws.add_ip_to_security_group, aws_regions_to_provision, progress_bar=True, desc="add IP to aws security groups")
    aws_instances = refresh_instance_list(aws, aws_regions_to_provision, aws_instance_filter)

    gcp.create_ssh_key()
    gcp.configure_default_firewall()
    gcp_instances = refresh_instance_list(gcp, gcp_regions_to_provision, gcp_instance_filter)

    # provision missing regions using do_parallel
    missing_aws_regions = set(aws_regions_to_provision) - set(aws_instances.keys())
    missing_gcp_regions = set(gcp_regions_to_provision) - set(gcp_instances.keys())
    if missing_aws_regions:
        logger.info(f"(aws) provisioning missing regions: {missing_aws_regions}")
        aws_provisioner = lambda r: aws.provision_instance(r, aws_instance_class)
        results = do_parallel(aws_provisioner, missing_aws_regions, progress_bar=True, desc="provision aws")
        for region, result in results:
            aws_instances[region] = [result]
            logger.info(f"(aws:{region}) provisioned {result}, waiting for ready")
            result.wait_for_ready()
            logger.info(f"(aws:{region}) ready")
        aws_instances = refresh_instance_list(aws, aws_regions_to_provision, aws_instance_filter)
    if missing_gcp_regions:
        logger.info(f"(gcp) provisioning missing regions: {missing_gcp_regions}")
        gcp_provisioner = lambda r: gcp.provision_instance(r, gcp_instance_class)
        results = do_parallel(gcp_provisioner, missing_gcp_regions, progress_bar=True, desc="provision gcp")
        for region, result in results:
            gcp_instances[region] = [result]
            logger.info(f"(gcp:{region}) provisioned {result}")
        gcp_instances = refresh_instance_list(gcp, gcp_regions_to_provision, gcp_instance_filter)

    all_instances = [i for ilist in aws_instances.values() for i in ilist]
    all_instances += [i for ilist in gcp_instances.values() for i in ilist]
    for i in all_instances:
        i.init_log_files(log_dir)

    # run setup script on each instance (use do_parallel)
    if setup_script:
        run_script = lambda i: i.copy_and_run_script(setup_script)
        do_parallel(run_script, all_instances, progress_bar=True, desc="setup script")
    return aws_instances, gcp_instances
