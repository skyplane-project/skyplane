from typing import List
from skylark.compute.aws.aws_cloud_provider import AWSCloudProvider
from skylark.compute.gcp.gcp_cloud_provider import GCPCloudProvider
from skylark.compute.server import Server
from skylark.replicate.replication_plan import ReplicationPlan, ReplicationTopology
from skylark.utils import do_parallel

from loguru import logger


class ReplicatorCoordinator:
    def __init__(
        self,
        topology: ReplicationTopology,
        gcp_project: str,
        gateway_docker_image: str="ghcr.io/parasj/skylark-docker:latest",
        aws_instance_class: str='m5.4xlarge',
        gcp_instance_class: str='n2-standard-16',
        gcp_use_premium_network: bool=True
    ):
        self.topology = topology
        self.gateway_docker_image = gateway_docker_image
        self.aws_instance_class = aws_instance_class
        self.gcp_instance_class = gcp_instance_class
        self.gcp_use_premium_network = gcp_use_premium_network
        
        # provisioning
        self.aws = AWSCloudProvider()
        self.gcp = GCPCloudProvider(gcp_project)
        self.init_clouds()
        self.bound_paths: List[List[Server]] = None
    
    def init_clouds(self):
        """Initialize AWS and GCP clouds."""
        do_parallel(self.aws.add_ip_to_security_group, self.aws.region_list())
        self.gcp.create_ssh_key()
        self.gcp.configure_default_network()
        self.gcp.configure_default_firewall()
        logger.debug("Initialized GCP and AWS clouds.")
    
    def provision_gateway(self, region: str) -> Server:
        # provision instance
        provider, subregion = region.split(':')
        if provider == 'aws':
            server = self.aws.provision_instance(subregion, self.aws_instance_class)
        elif provider == 'gcp':
            server = self.gcp.provision_instance(subregion, self.gcp_instance_class, premium_network=self.gcp_use_premium_network)
        else:
            raise NotImplementedError(f"Unknown provider {provider}")
        logger.info(f"Provisioned gateway {server.instance_id} in {server.region}")
        
        # setup server
        server.wait_for_ready()
        server.run_command("sudo apt-get update && sudo apt-get install -y iperf3")
        docker_installed = "Docker version" in server.run_command(f"sudo docker --version")[0]
        if not docker_installed:
            server.run_command("curl -fsSL https://get.docker.com -o get-docker.sh && sudo sh get-docker.sh")
        out, err = server.run_command("sudo docker run --rm hello-world")
        assert "Hello from Docker!" in out
        server.run_command("sudo docker pull {}".format(self.gateway_docker_image))
        return server
    
    def deprovision_gateway(self, server: Server):
        logger.warning(f"Deprovisioning gateway {server.name}")
        server.terminate_instance()

    def provision_gateways(self):
        regions_to_provision = [r for path in self.topology.paths for r in path]
        results = do_parallel(self.provision_gateway, regions_to_provision, n=len(regions_to_provision), progress_bar=True, desc="Provisioning gateways")
        instances_by_region = {r: [instance for instance_region, instance in results if instance_region == r] for r in set(regions_to_provision)}
        
        bound_paths = []
        for path in self.topology.paths:
            bound_paths.append([instances_by_region[r].pop() for r in path])
        self.bound_paths = bound_paths

    def deprovision_gateways(self):
        instances = [instance for path in self.bound_paths for instance in path]
        do_parallel(self.deprovision_gateway, instances, n=len(instances), progress_bar=True, desc="Deprovisioning gateways")
    
    