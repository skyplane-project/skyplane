import os
import uuid
import warnings
from pathlib import Path
from typing import List, Optional

from cryptography.utils import CryptographyDeprecationWarning

from skyplane.compute.gcp.gcp_network import GCPNetwork
from skyplane.compute.gcp.gcp_pricing import GCPPricing
from skyplane.utils import imports

with warnings.catch_warnings():
    warnings.filterwarnings("ignore", category=CryptographyDeprecationWarning)
    import paramiko

from skyplane import exceptions, key_root
from skyplane.compute.cloud_provider import CloudProvider
from skyplane.compute.gcp.gcp_auth import GCPAuthentication
from skyplane.compute.gcp.gcp_server import GCPServer
from skyplane.compute.server import Server, ServerState
from skyplane.utils import logger
from skyplane.utils.fn import wait_for


class GCPCloudProvider(CloudProvider):
    def __init__(self, key_root=key_root / "gcp"):
        super().__init__()
        self.auth = GCPAuthentication()
        self.network = GCPNetwork(self.auth)
        key_root.mkdir(parents=True, exist_ok=True)
        self.private_key_path = key_root / "gcp-cert.pem"
        self.public_key_path = key_root / "gcp-cert.pub"

    @property
    def name(self):
        return "gcp"

    @staticmethod
    def region_list():
        """See https://cloud.google.com/network-tiers/docs/overview#regions_supporting_standard_tier for a list of regions in the standard tier."""
        return GCPAuthentication.get_region_config()

    @staticmethod
    def region_list_standard():
        regions_with_standard = [
            "asia-east1",
            "asia-east2",
            "asia-northeast1",
            "asia-northeast3",
            "asia-south1",
            "asia-southeast1",
            "asia-southeast2",
            "australia-southeast1",
            "us-west1",
            "us-west2",
            "us-west3",
            "us-west4",
            "us-central1",
            "us-east1",
            "us-east4",
            "northamerica-northeast1",
            "southamerica-east1",
            "europe-north1",
            "europe-west1",
            "europe-west2",
            "europe-west3",
            "europe-west4",
            "europe-west6",
        ]
        availability_zones = []
        for r in GCPCloudProvider.region_list():
            parsed_region, _ = r.rsplit("-", 1)
            if parsed_region in regions_with_standard:
                availability_zones.append(r)
        return availability_zones

    @classmethod
    def get_transfer_cost(cls, src_key, dst_key, premium_tier=True):
        assert src_key.startswith("aws:")
        return GCPPricing.get_transfer_cost(src_key, dst_key, premium_tier)

    def get_instance_list(self, region) -> List[GCPServer]:
        gcp_instance_result = self.auth.get_gcp_instances(region)
        if "items" in gcp_instance_result:
            instance_list = []
            for i in gcp_instance_result["items"]:
                instance_list.append(GCPServer(f"gcp:{region}", i["name"], ssh_private_key=self.private_key_path))
            return instance_list
        else:
            return []

    def get_matching_instances(self, network_tier=None, **kwargs):
        instances: List[Server] = super().get_matching_instances(**kwargs)
        matching_instances = []
        for instance in instances:
            if network_tier is None or instance.network_tier() == network_tier:
                matching_instances.append(instance)
        return matching_instances

    def create_ssh_key(self):
        private_key_path = Path(self.private_key_path)
        if not private_key_path.exists():
            private_key_path.parent.mkdir(parents=True, exist_ok=True)
            key = paramiko.RSAKey.generate(4096)
            key.write_private_key_file(self.private_key_path, password="skyplane")
            with open(self.public_key_path, "w") as f:
                f.write(f"{key.get_name()} {key.get_base64()}\n")

    def setup_global(self):
        self.network.create_network()
        self.network.create_default_firewall_rules()

    def teardown_global(self):
        self.network.delete_network()

    @imports.inject("googleapiclient.errors", pip_extra="gcp")
    def authorize_gateways(errors, self, ips: List[str], rule_name: Optional[str] = None) -> str:
        firewall_name = f"skyplane-{uuid.uuid4().hex}" if rule_name is None else rule_name
        self.network.create_firewall_rule(firewall_name, ips, ["0-65535"], ["tcp", "udp", "icmp"])
        return firewall_name

    @imports.inject("googleapiclient.errors", pip_extra="gcp")
    def remove_gateway_rule(errors, self, firewall_name: str):
        if self.network.get_firewall_rule(firewall_name):
            self.network.delete_firewall_rule(firewall_name)

    @imports.inject("googleapiclient.errors", pip_extra="gcp")
    def provision_instance(
        errors,
        self,
        region: str,
        instance_class: str,
        disk_size: int = 32,
        use_spot_instances: bool = False,
        name: Optional[str] = None,
        tags={"skyplane": "true"},
        gcp_premium_network=False,
        gcp_vm_uname="skyplane",
    ) -> GCPServer:
        assert not region.startswith("gcp:"), "Region should be GCP region"
        if name is None:
            name = f"skyplane-gcp-{str(uuid.uuid4()).replace('-', '')}"
        compute = self.auth.get_gcp_client()
        with open(os.path.expanduser(self.public_key_path)) as f:
            pub_key = f.read()

        req_body = {
            "name": name,
            "machineType": f"zones/{region}/machineTypes/{instance_class}",
            "labels": tags,
            "disks": [
                {
                    "boot": True,
                    "autoDelete": True,
                    "initializeParams": {
                        "sourceImage": "projects/cos-cloud/global/images/family/cos-stable",
                        "diskType": f"zones/{region}/diskTypes/pd-standard",
                        "diskSizeGb": "100",
                    },
                }
            ],
            "networkInterfaces": [
                {
                    "network": "global/networks/skyplane",
                    "accessConfigs": [
                        {"name": "External NAT", "type": "ONE_TO_ONE_NAT", "networkTier": "PREMIUM" if gcp_premium_network else "STANDARD"}
                    ],
                }
            ],
            "serviceAccounts": [{"email": "default", "scopes": ["https://www.googleapis.com/auth/cloud-platform"]}],
            "metadata": {"items": [{"key": "ssh-keys", "value": f"{gcp_vm_uname}:{pub_key}\n"}]},
            "scheduling": {"onHostMaintenance": "TERMINATE", "automaticRestart": False},
            "deletionProtection": False,
        }
        # use preemtible instances if use_spot_instances is True
        if use_spot_instances:
            req_body["scheduling"]["preemptible"] = True
        try:
            result = compute.instances().insert(project=self.auth.project_id, zone=region, body=req_body).execute()
            self.auth.wait_for_operation_to_complete(region, result["name"])
        except errors.HttpError as e:
            if e.resp.status == 409:
                if "ZONE_RESOURCE_POOL_EXHAUSTED" in e.content:
                    raise exceptions.InsufficientVCPUException(f"Got ZONE_RESOURCE_POOL_EXHAUSTED in region {region}") from e
                elif "RESOURCE_EXHAUSTED" in e.content:
                    raise exceptions.InsufficientVCPUException(f"Got RESOURCE_EXHAUSTED in region {region}") from e
                elif "QUOTA_EXCEEDED" in e.content:
                    raise exceptions.InsufficientVCPUException(f"Got QUOTA_EXCEEDED in region {region}") from e
                elif "QUOTA_LIMIT" in e.content:
                    raise exceptions.InsufficientVCPUException(f"Got QUOTA_LIMIT in region {region}") from e
                else:
                    raise e

        # wait for server to reach RUNNING state
        server = GCPServer(f"gcp:{region}", name)
        try:
            wait_for(
                lambda: server.instance_state() == ServerState.RUNNING,
                timeout=120,
                interval=0.1,
                desc=f"Wait for RUNNING status on {server.uuid()}",
            )
            server.wait_for_ssh_ready()
        except:
            logger.fs.error(f"Instance {server.uuid()} did not reach RUNNING status")
            server.terminate_instance()
            raise
        server.run_command("sudo /sbin/iptables -A INPUT -j ACCEPT")
        return server
