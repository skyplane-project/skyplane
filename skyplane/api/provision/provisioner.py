import uuid
from dataclasses import dataclass, field
from functools import partial

from typing import Optional, Dict, Set, List, Tuple

from skyplane import compute
from skyplane.utils import logger
from skyplane.utils.fn import do_parallel
from skyplane.utils.timer import Timer


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

    def __hash__(self):
        return uuid.UUID(self.uuid).int


class Provisioner:
    def __init__(
        self,
        aws_auth: Optional[compute.AWSAuthentication] = None,
        azure_auth: Optional[compute.AzureAuthentication] = None,
        gcp_auth: Optional[compute.GCPAuthentication] = None,
        host_uuid: Optional[str] = None,
    ):
        self.aws_auth = aws_auth
        self.azure_auth = azure_auth
        self.gcp_auth = gcp_auth
        self.host_uuid = host_uuid
        self._make_cloud_providers()
        self.temp_nodes: Set[compute.Server] = set()  # temporary area to store nodes that should be terminated upon exit
        self.pending_provisioner_tasks: List[ProvisionerTask] = []
        self.provisioned_vms: Dict[str, compute.Server] = {}

        # store GCP firewall rules to be deleted upon exit
        self.gcp_firewall_rules: Set[str] = set()

    def _make_cloud_providers(self):
        self.aws = compute.AWSCloudProvider(
            key_prefix=f"skyplane{'-'+self.host_uuid.replace('-', '') if self.host_uuid else ''}", auth=self.aws_auth
        )
        self.azure = compute.AzureCloudProvider(auth=self.azure_auth)
        self.gcp = compute.GCPCloudProvider(auth=self.gcp_auth)

    def init_global(self, aws: bool = True, azure: bool = True, gcp: bool = True):
        logger.fs.info(f"[Provisioner.init_global] Initializing global resources for aws={aws}, azure={azure}, gcp={gcp}")
        jobs = []
        if aws:
            jobs.append(partial(self.aws.setup_global, attach_policy_arn="arn:aws:iam::aws:policy/AmazonS3FullAccess"))
        if azure:
            jobs.append(self.azure.create_ssh_key)
            jobs.append(self.azure.set_up_resource_group)
        if gcp:
            jobs.append(self.gcp.setup_global)
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
        logger.fs.info(f"[Provisioner.add_task] Queue {task}")
        return task.uuid

    def get_node(self, uuid: str) -> compute.Server:
        return self.provisioned_vms[uuid]

    def _provision_task(self, task: ProvisionerTask):
        with Timer() as t:
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
        logger.fs.debug(f"[Provisioner._provision_task] Provisioned {server} in {t.elapsed:.2f}s")
        self.temp_nodes.add(server)
        self.provisioned_vms[task.uuid] = server
        server.wait_for_ssh_ready()
        if task.autoterminate_minutes:
            server.enable_auto_shutdown(task.autoterminate_minutes)
        self.temp_nodes.remove(server)
        return server

    def provision(self, authorize_firewall: bool = True, max_jobs: int = 16, spinner: bool = False) -> List[str]:
        """Provision the VMs in the pending_provisioner_tasks list. Returns UUIDs of provisioned VMs."""
        if not self.pending_provisioner_tasks:
            return []

        # copy list to avoid concurrency issue
        provision_tasks = list(self.pending_provisioner_tasks)
        aws_provisioned = any([task.cloud_provider == "aws" for task in provision_tasks])
        aws_regions = set([task.region for task in provision_tasks if task.cloud_provider == "aws"])
        azure_provisioned = any([task.cloud_provider == "azure" for task in provision_tasks])
        gcp_provisioned = any([task.cloud_provider == "gcp" for task in provision_tasks])

        # configure regions
        if aws_provisioned:
            do_parallel(
                self.aws.setup_region, list(set(aws_regions)), spinner=spinner, spinner_persist=False, desc="Configuring AWS regions"
            )
            logger.fs.info(f"[Provisioner.provision] Configured AWS regions {aws_regions}")

        # provision VMs
        logger.fs.info(f"[Provisioner.provision] Provisioning {len(provision_tasks)} VMs")
        results: List[Tuple[ProvisionerTask, compute.Server]] = do_parallel(
            self._provision_task,
            provision_tasks,
            n=max_jobs,
            spinner=spinner,
            spinner_persist=spinner,
            desc="Provisioning VMs",
        )

        # configure firewall
        if authorize_firewall:
            public_ips = [s.public_ip() for _, s in results]
            # authorize access to private IPs for GCP VMs due to global VPC
            private_ips = [s.private_ip() for t, s in results if t.cloud_provider == "gcp"]
            authorize_ip_jobs = []
            if aws_provisioned:
                authorize_ip_jobs.extend([partial(self.aws.add_ips_to_security_group, r, public_ips) for r in set(aws_regions)])
            if gcp_provisioned:

                def authorize_gcp_gateways():
                    self.gcp_firewall_rules.add(self.gcp.authorize_gateways(public_ips + private_ips))

                authorize_ip_jobs.append(authorize_gcp_gateways)

            do_parallel(
                lambda fn: fn(),
                authorize_ip_jobs,
                n=max_jobs,
                spinner=spinner,
                spinner_persist=False,
                desc="Authorizing gateways with firewalls",
            )
            logger.fs.info(f"[Provisioner.provision] Authorized AWS gateways with firewalls: {public_ips}")
            logger.fs.info(f"[Provisioner.provision] Authorized GCP gateways with firewalls: {public_ips}, {private_ips}")

        for task in provision_tasks:
            self.pending_provisioner_tasks.remove(task)

        return [task.uuid for task in provision_tasks]

    def deprovision(self, deauthorize_firewall: bool = True, max_jobs: int = 64, spinner: bool = False):
        """Deprovision all nodes. Returns UUIDs of deprovisioned VMs."""
        if not self.provisioned_vms and not self.temp_nodes:
            return []

        def deprovision_gateway_instance(server: compute.Server):
            server.terminate_instance()
            idx_to_del = None
            for idx, s in list(self.provisioned_vms.items()):
                if s == server:
                    idx_to_del = idx
                    break
            if idx_to_del:
                del self.provisioned_vms[idx_to_del]
            else:
                logger.fs.warning(f"[Provisioner.deprovision] Could not find {server} in {self.provisioned_vms}")
            if server in self.temp_nodes:
                self.temp_nodes.remove(server)
            logger.fs.info(f"[Provisioner.deprovision] Terminated {server}")

        servers = list(self.provisioned_vms.values()) + list(self.temp_nodes)
        aws_deprovisioned = any([s.provider == "aws" for s in servers])
        azure_deprovisioned = any([s.provider == "azure" for s in servers])
        gcp_deprovisioned = any([s.provider == "gcp" for s in servers])
        if azure_deprovisioned:
            logger.warning("Azure deprovisioning is very slow. Please be patient.")
        logger.fs.info(f"[Provisioner.deprovision] Deprovisioning {len(servers)} VMs")
        do_parallel(
            deprovision_gateway_instance,
            servers,
            n=max_jobs,
            spinner=spinner,
            spinner_persist=False,
            desc="Deprovisioning VMs",
        )

        # clean up firewall
        if deauthorize_firewall:
            # todo remove firewall rules for Azure
            public_ips = [s.public_ip() for s in servers]
            jobs = []
            if aws_deprovisioned:
                aws_regions = set([s.region() for s in servers if s.provider == "aws"])
                jobs.extend([partial(self.aws.remove_ips_from_security_group, r, public_ips) for r in set(aws_regions)])
                logger.fs.info(f"[Provisioner.deprovision] Deauthorizing AWS gateways with firewalls: {public_ips}")
            if gcp_deprovisioned:
                jobs.extend([partial(self.gcp.remove_gateway_rule, rule) for rule in self.gcp_firewall_rules])
                logger.fs.info(f"[Provisioner.deprovision] Deauthorizing GCP gateways with firewalls: {self.gcp_firewall_rules}")
            do_parallel(
                lambda fn: fn(), jobs, n=max_jobs, spinner=spinner, spinner_persist=False, desc="Deauthorizing gateways from firewalls"
            )
