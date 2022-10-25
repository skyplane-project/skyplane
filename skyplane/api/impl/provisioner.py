import uuid
from dataclasses import dataclass, field
from functools import partial
from typing import Optional, Dict, Set, List

from skyplane.compute.aws.aws_auth import AWSAuthentication
from skyplane.compute.aws.aws_cloud_provider import AWSCloudProvider
from skyplane.compute.azure.azure_auth import AzureAuthentication
from skyplane.compute.azure.azure_cloud_provider import AzureCloudProvider
from skyplane.compute.gcp.gcp_auth import GCPAuthentication
from skyplane.compute.gcp.gcp_cloud_provider import GCPCloudProvider
from skyplane.compute.server import Server
from skyplane.utils import logger
from skyplane.utils.fn import do_parallel


@dataclass
class ProvisionerTask:
    """Dataclass to track a single VM provisioning job."""

    cloud_provider: str
    region: str
    vm_type: str
    spot: bool = False
    autoterminate_minutes: Optional[int] = None
    tags: Dict[str, str] = field(default_factory=dict)
    uuid: str = field(default_factory=lambda: str(uuid.uuid4()))


class Provisioner:
    def __init__(
        self,
        host_uuid: Optional[str] = None,
        aws_auth: Optional[AWSAuthentication] = None,
        azure_auth: Optional[AzureAuthentication] = None,
        gcp_auth: Optional[GCPAuthentication] = None,
    ):
        self.host_uuid = host_uuid
        self.aws = AWSCloudProvider(key_prefix=f"skyplane-{host_uuid.replace('-', '') if host_uuid else ''}", auth=aws_auth)
        self.azure = AzureCloudProvider(auth=azure_auth)
        self.gcp = GCPCloudProvider(auth=gcp_auth)
        self.temp_nodes: Set[Server] = set()  # temporary area to store nodes that should be terminated upon exit
        self.pending_provisioner_tasks: List[ProvisionerTask] = []
        self.provisioned_vms: Dict[str, Server] = {}

    def init_global(self, aws: bool = True, azure: bool = True, gcp: bool = True):
        jobs = []
        if aws:
            jobs.append(partial(self.aws.setup_global, attach_policy_arn="arn:aws:iam::aws:policy/AmazonS3FullAccess"))
        if azure:
            jobs.append(self.azure.create_ssh_key)
            jobs.append(self.azure.set_up_resource_group)
        if gcp:
            jobs.append(self.gcp.create_ssh_key)
            jobs.append(self.gcp.configure_skyplane_network)
            jobs.append(self.gcp.configure_skyplane_firewall)
        do_parallel(lambda fn: fn(), jobs, spinner=False)

    def add_task(
        self,
        cloud_provider: str,
        region: str,
        vm_type: str,
        spot: bool = False,
        autoterminate_minutes: Optional[int] = None,
        tags: Optional[Dict[str, str]] = None,
    ):
        if tags is None:
            tags = {"skyplane": "true", "skyplaneclientid": self.host_uuid} if self.host_uuid else {"skyplane": "true"}
        task = ProvisionerTask(cloud_provider, region, vm_type, spot, autoterminate_minutes, tags)
        self.pending_provisioner_tasks.append(task)
        return task.uuid

    def get_node(self, uuid: str) -> Server:
        return self.provisioned_vms[uuid]

    def _provision_task(self, task: ProvisionerTask):
        if task.cloud_provider == "aws":
            assert self.aws.auth.enabled(), "AWS credentials not configured"
            server = self.aws.provision_instance(task.region, task.vm_type, use_spot_instances=task.spot, tags=task.tags)
        elif task.cloud_provider == "azure":
            assert self.azure.auth.enabled(), "Azure credentials not configured"
            server = self.azure.provision_instance(task.region, task.vm_type, use_spot_instances=task.spot, tags=task.tags)
        elif task.cloud_provider == "gcp":
            assert self.gcp.auth.enabled(), "GCP credentials not configured"
            # todo specify network tier in ReplicationTopology
            server = self.gcp.provision_instance(
                task.region,
                task.vm_type,
                use_spot_instances=task.spot,
                gcp_premium_network=False,
                tags=task.tags,
            )
        else:
            raise NotImplementedError(f"Unknown provider {task.cloud_provider}")
        self.temp_nodes.add(server)
        self.provisioned_vms[task.uuid] = server
        server.wait_for_ssh_ready()
        if task.autoterminate_minutes:
            server.enable_auto_shutdown(task.autoterminate_minutes)
        self.temp_nodes.remove(server)
        return server

    def provision(self, allow_firewall: bool = True, max_jobs: int = 16, spinner: bool = False) -> List[str]:
        """Provision the VMs in the pending_provisioner_tasks list. Returns UUIDs of provisioned VMs."""
        if not self.pending_provisioner_tasks:
            return []

        # copy list to avoid concurrency issue
        provision_tasks = set(self.pending_provisioner_tasks)
        aws_provisioned = any([task.cloud_provider == "aws" for task in provision_tasks])
        azure_provisioned = any([task.cloud_provider == "azure" for task in provision_tasks])
        gcp_provisioned = any([task.cloud_provider == "gcp" for task in provision_tasks])
        results: Dict[ProvisionerTask, Server] = dict(
            do_parallel(
                self._provision_task,
                provision_tasks,
                n=max_jobs,
                spinner=spinner,
                spinner_persist=True,
                desc="Provisioning VMs",
            )
        )

        # configure regions
        aws_regions = set([task.region for task in provision_tasks if task.cloud_provider == "aws"])
        if aws_provisioned:
            do_parallel(
                self.aws.setup_region, list(set(aws_regions)), spinner=spinner, spinner_persist=False, desc="Configuring AWS regions"
            )

        # configure firewall
        if allow_firewall:
            public_ips = [s.public_ip() for s in results.values()]
            # authorize access to private IPs for GCP VMs due to global VPC
            private_ips = [s.private_ip() for t, s in results.items() if t.cloud_provider == "gcp"]
            authorize_ip_jobs = []
            if aws_provisioned:
                authorize_ip_jobs.extend([partial(self.aws.add_ips_to_security_group, r, public_ips) for r in set(aws_regions)])
            if gcp_provisioned:
                authorize_ip_jobs.append(partial(self.gcp.add_ips_to_firewall, public_ips + private_ips))
            do_parallel(
                lambda fn: fn(),
                authorize_ip_jobs,
                n=max_jobs,
                spinner=spinner,
                spinner_persist=False,
                desc="Authorizing gateways with firewalls",
            )

        for task in provision_tasks:
            self.pending_provisioner_tasks.remove(task)

        return [task.uuid for task in provision_tasks]

    def deprovision(self, max_jobs: int = 64, spinner: bool = False):
        """Deprovision all nodes. Returns UUIDs of deprovisioned VMs."""
        if not self.provisioned_vms and not self.temp_nodes:
            return []

        def deprovision_gateway_instance(server: Server):
            server.terminate_instance()
            for idx, s in self.provisioned_vms:
                if s == server:
                    del self.provisioned_vms[idx]
            if server in self.temp_nodes:
                self.temp_nodes.remove(server)

        servers = list(self.provisioned_vms.values()) + list(self.temp_nodes)
        aws_deprovisioned = any([s.provider == "aws" for s in servers])
        azure_deprovisioned = any([s.provider == "azure" for s in servers])
        gcp_deprovisioned = any([s.provider == "gcp" for s in servers])
        if azure_deprovisioned:
            logger.warning("Azure deprovisioning is very slow. Please be patient.")
        do_parallel(
            deprovision_gateway_instance,
            servers,
            n=max_jobs,
            spinner=spinner,
            spinner_persist=False,
            desc="Deprovisioning VMs",
        )

        # clean up firewall
        # todo remove firewall rules for Azure
        public_ips = [s.public_ip() for s in servers]
        # deauthorize access to private IPs for GCP VMs due to global VPC
        private_ips = [s.private_ip() for s in servers if s.provider == "gcp"]
        jobs = []
        if aws_deprovisioned:
            aws_regions = set([s.region() for s in servers if s.provider == "aws"])
            jobs.extend([partial(self.aws.remove_ips_from_security_group, r, public_ips) for r in set(aws_regions)])
        if gcp_deprovisioned:
            jobs.append(partial(self.gcp.remove_ips_from_firewall, public_ips + private_ips))
        do_parallel(lambda fn: fn(), jobs, n=max_jobs, spinner=spinner, spinner_persist=False, desc="Deauthorizing gateways from firewalls")
