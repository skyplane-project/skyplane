from loguru import logger

from skylark.compute.aws.aws_cloud_provider import AWSCloudProvider

if __name__ == "__main__":
    aws = AWSCloudProvider()

    grouped_by_region = {}
    for region in aws.region_list():
        grouped_by_region[region] = aws.get_matching_instances(region=region)
        if len(grouped_by_region[region]) == 0:
            logger.info(f"No instances found in {region}, provisioning")
            ami = aws.get_ubuntu_ami_id(region)
            server = aws.provision_instance(region, ami, "i3en.large", f"skylark-{region}")
            logger.debug(f"Provisioned {server}")
            grouped_by_region[region].append(server)
        else:
            logger.info(f"Found {len(grouped_by_region[region])} instances in {region}")

    logger.info(f"Terminating skylark instances")
    for region in grouped_by_region:
        for server in grouped_by_region[region]:
            if server.tags.get("skylark") == "true":
                server.terminate_instance()
