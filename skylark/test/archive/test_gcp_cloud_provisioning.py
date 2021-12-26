from loguru import logger

from skylark.compute.gcp.gcp_server import GCPServer
from skylark.compute.gcp.gcp_cloud_provider import GCPCloudProvider

if __name__ == "__main__":
    gcp_project = "bair-commons-307400"
    gcp = GCPCloudProvider(gcp_project)

    grouped_by_region = {}
    for region in gcp.region_list:
        grouped_by_region[region] = gcp.get_matching_instances(region=region)
        if len(grouped_by_region[region]) == 0:
            logger.info(f"No instances found in {region}, provisioning")
            server = GCPServer.provision_instance(
                region=region,
                gcp_project=gcp_project,
                instance_class="f1-micro",
                premium_network=False,
                ssh_private_key="~/.ssh/google_compute_engine",
                ssh_public_key="~/.ssh/google_compute_engine.pub",
                tags={"skylark": "true"},
            )
            logger.info(f"Provisioned server {server}")
            grouped_by_region[region] = gcp.get_matching_instances(region=region)
            logger.info(f"Instances now: {grouped_by_region[region]}")

    logger.info(f"Terminating skylark instances")
    for region in grouped_by_region:
        for server in grouped_by_region[region]:
            if server.tags.get("skylark") == "true":
                server.terminate_instance()
