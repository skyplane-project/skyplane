import os
import time
import uuid
from pathlib import Path
from typing import List

import googleapiclient
import warnings
from cryptography.utils import CryptographyDeprecationWarning

with warnings.catch_warnings():
    warnings.filterwarnings("ignore", category=CryptographyDeprecationWarning)
    import paramiko

from skyplane import exceptions, key_root
from skyplane.compute.azure.azure_cloud_provider import AzureCloudProvider
from skyplane.compute.cloud_providers import CloudProvider
from skyplane.compute.gcp.gcp_auth import GCPAuthentication
from skyplane.compute.gcp.gcp_server import GCPServer
from skyplane.compute.server import Server, ServerState
from skyplane.utils import logger
from skyplane.utils.fn import wait_for


class GCPCloudProvider(CloudProvider):
    def __init__(self, key_root=key_root / "gcp"):
        super().__init__()
        self.auth = GCPAuthentication()
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

    @staticmethod
    def get_transfer_cost(src_key, dst_key, premium_tier=True):
        src_provider, src = src_key.split(":")
        dst_provider, dst = dst_key.split(":")
        assert src_provider == "gcp"
        src_continent, src_region, src_zone = src.split("-")
        if dst_provider == "gcp":
            dst_continent, dst_region, dst_zone = dst.split("-")
            if src_continent == dst_continent and src_region == dst_region and src_zone == dst_zone:
                return 0.0
            elif src_continent == dst_continent and src_region == dst_region:
                return 0.01
            elif src_continent == dst_continent:
                if src_continent == "northamerica" or src_continent == "us":
                    return 0.01
                elif src_continent == "europe":
                    return 0.02
                elif src_continent == "asia":
                    return 0.05
                elif src_continent == "southamerica":
                    return 0.08
                elif src_continent == "australia":
                    return 0.08
                else:
                    raise Exception(f"Unknown continent {src_continent}")
            elif src.startswith("asia-southeast2") or src_continent == "australia":
                return 0.15
            elif dst.startswith("asia-southeast2") or dst_continent == "australia":
                return 0.15
            else:
                return 0.08
        elif dst_provider in ["aws", "azure"] and premium_tier:
            is_dst_australia = (
                (dst == "ap-southeast-2") if dst_provider == "aws" else (AzureCloudProvider.lookup_continent(dst) == "oceania")
            )
            # singapore or tokyo or osaka
            if src_continent == "asia" and (src_region == "southeast2" or src_region == "northeast1" or src_region == "northeast2"):
                return 0.19 if is_dst_australia else 0.14
            # jakarta
            elif (src_continent == "asia" and src_region == "southeast1") or (src_continent == "australia"):
                return 0.19
            # seoul
            elif src_continent == "asia" and src_region == "northeast3":
                return 0.19 if is_dst_australia else 0.147
            else:
                return 0.19 if is_dst_australia else 0.12
        elif dst_provider in ["aws", "azure"] and not premium_tier:
            if src_continent == "us" or src_continent == "europe" or src_continent == "northamerica":
                return 0.085
            elif src_continent == "southamerica" or src_continent == "australia":
                return 0.12
            elif src_continent == "asia":
                return 0.11
            else:
                raise ValueError("Unknown src_continent: {}".format(src_continent))

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

    def configure_skyplane_network(self):
        compute = self.auth.get_gcp_client()
        try:
            compute.networks().get(project=self.auth.project_id, network="skyplane").execute()
        except googleapiclient.errors.HttpError as e:
            if e.resp.status == 404:  # create network
                op = (
                    compute.networks()
                    .insert(project=self.auth.project_id, body={"name": "skyplane", "subnetMode": "auto", "autoCreateSubnetworks": True})
                    .execute()
                )
                self.wait_for_operation_to_complete("global", op["name"])
            else:
                raise e

    def configure_skyplane_firewall(self, ip="0.0.0.0/0"):
        """Configure skyplane firewall to allow SSH from all ports from all IPs (if not exists)."""
        compute = self.auth.get_gcp_client()

        def create_firewall(body, update_firewall=False):
            if update_firewall:
                op = compute.firewalls().update(project=self.auth.project_id, firewall="skyplanessh", body=fw_body).execute()
            else:
                op = compute.firewalls().insert(project=self.auth.project_id, body=fw_body).execute()
            self.wait_for_operation_to_complete("global", op["name"])

        try:
            current_firewall = compute.firewalls().get(project=self.auth.project_id, firewall="skyplanessh").execute()
        except googleapiclient.errors.HttpError as e:
            if e.resp.status == 404:
                current_firewall = None
            else:
                raise e

        fw_body = {
            "name": "skyplanessh",
            "network": "global/networks/skyplane",
            "allowed": [{"IPProtocol": "tcp", "ports": ["22"]}, {"IPProtocol": "udp", "ports": ["1-65535"]}, {"IPProtocol": "icmp"}],
            "description": "Allow all traffic from all IPs",
            "sourceRanges": [ip],
        }
        if current_firewall is None:
            create_firewall(fw_body, update_firewall=False)
            logger.fs.debug(f"[GCP] Created new firewall")

        elif current_firewall["allowed"] != fw_body["allowed"]:
            create_firewall(fw_body, update_firewall=True)
            logger.fs.debug(f"[GCP] Updated firewall")

    def delete_vpc(self, vpc_name="skyplane"):
        """
        Delete VPC. This might error our in some cases.
        """
        compute = self.auth.get_gcp_client()
        request = compute.networks().delete(project=self.auth.project_id, network=vpc_name)
        try:
            delete_vpc_response = request.execute()
            self.wait_for_operation_to_complete("global", delete_vpc_response)
        except googleapiclient.errors.HttpError as e:
            logger.fs.warn(
                f"Unable to Delete. Ensure no active firewall rules acting upon the {vpc_name} VPC. Ensure no instances provisioned in the VPC "
            )
            logger.fs.error(e)

    def add_ips_to_firewall(self, ips: List[str]):
        """
        Other than "default" VPCs start with 2 rules at 65535 priority:
         - Allow all egress
         - Block all ingress
        If you do not specify a priority when creating a rule, it is assigned a priority of 1000
        """

        def create_firewall(body, update_firewall=False):
            if update_firewall:
                op = compute.firewalls().update(project=self.auth.project_id, firewall="skyplane", body=fw_body).execute()
            else:
                op = compute.firewalls().insert(project=self.auth.project_id, body=fw_body).execute()
            self.wait_for_operation_to_complete("global", op["name"])

        compute = self.auth.get_gcp_client()
        # Let's call each firewall rule by the ip name, so it's easier to delete
        #  individual ips during concurrent transfers
        for ip in ips:
            firewall_name = "skyplane" + ip.replace(".", "")
            fw_body = {
                "name": firewall_name,  # Name should be [a-z]([-a-z0-9]*[a-z0-9]
                "network": "global/networks/skyplane",
                "allowed": [{"IPProtocol": "tcp", "ports": ["1-65535"]}],
                "description": f"Allow all TCP traffic from ip {ip}",
                "sourceRanges": [f"{ip}/32"],
            }
            try:
                current_firewall = compute.firewalls().get(project=self.auth.project_id, firewall=firewall_name).execute()
            except googleapiclient.errors.HttpError as e:
                if e.resp.status == 404:
                    current_firewall = None
                else:
                    raise e
            if current_firewall is None:
                create_firewall(fw_body, update_firewall=False)
                logger.fs.debug(f"[GCP] Created new firewall {firewall_name}")
            elif current_firewall["allowed"] != fw_body["allowed"]:
                create_firewall(fw_body, update_firewall=True)
                logger.fs.debug(f"[GCP] Updated firewall {firewall_name}")

    def remove_ips_from_firewall(self, ips: List[str]):
        compute = self.auth.get_gcp_client()
        for ip in ips:
            firewall_name = "skyplane" + ip.replace(".", "")  # Each firewall rule is called by the ip name, so it's easier to delete
            logger.fs.debug(f"[GCP] Deleting firewall rule {firewall_name}")
            try:
                compute.firewalls().delete(project=self.auth.project_id, firewall=firewall_name).execute()
            except googleapiclient.errors.HttpError as e:
                if e.resp.status == 404:  # Firewall doesnt exist. Continue
                    logger.fs.warning(f"[GCP] Unable to delete {firewall_name}, does not exist.")
                else:
                    raise e

    def get_operation_state(self, zone, operation_name):
        compute = self.auth.get_gcp_client()
        if zone == "global":
            return compute.globalOperations().get(project=self.auth.project_id, operation=operation_name).execute()
        else:
            return compute.zoneOperations().get(project=self.auth.project_id, zone=zone, operation=operation_name).execute()

    def wait_for_operation_to_complete(self, zone, operation_name, timeout=120):
        time_intervals = [0.1] * 10 + [0.2] * 10 + [1.0] * int(timeout)  # backoff
        start = time.time()
        while time.time() - start < timeout:
            operation_state = self.get_operation_state(zone, operation_name)
            if operation_state["status"] == "DONE":
                if "error" in operation_state:
                    raise Exception(operation_state["error"])
                else:
                    return operation_state
            time.sleep(time_intervals.pop(0))

    def provision_instance(
        self,
        region,
        instance_class,
        name=None,
        premium_network=False,
        uname="skyplane",
        tags={"skyplane": "true"},
        use_spot_instances: bool = False,
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
                        {"name": "External NAT", "type": "ONE_TO_ONE_NAT", "networkTier": "PREMIUM" if premium_network else "STANDARD"}
                    ],
                }
            ],
            "serviceAccounts": [{"email": "default", "scopes": ["https://www.googleapis.com/auth/cloud-platform"]}],
            "metadata": {"items": [{"key": "ssh-keys", "value": f"{uname}:{pub_key}\n"}]},
            "scheduling": {"onHostMaintenance": "TERMINATE", "automaticRestart": False},
            "deletionProtection": False,
        }
        # use preemtible instances if use_spot_instances is True
        if use_spot_instances:
            req_body["scheduling"]["preemptible"] = True
        try:
            result = compute.instances().insert(project=self.auth.project_id, zone=region, body=req_body).execute()
            self.wait_for_operation_to_complete(region, result["name"])
        except googleapiclient.errors.HttpError as e:
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
