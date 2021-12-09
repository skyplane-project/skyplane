import os
import socket
import time
from typing import List, Union
import uuid
from pathlib import Path
import googleapiclient
from loguru import logger

import paramiko

from skylark.compute.gcp.gcp_server import GCPServer, DEFAULT_GCP_PRIVATE_KEY_PATH, DEFAULT_GCP_PUBLIC_KEY_PATH
from skylark.compute.cloud_providers import CloudProvider
from skylark.compute.server import Server, ServerState


class GCPCloudProvider(CloudProvider):
    def __init__(self, gcp_project, private_key_path=DEFAULT_GCP_PRIVATE_KEY_PATH, public_key_path=DEFAULT_GCP_PUBLIC_KEY_PATH):
        super().__init__()
        self.gcp_project = gcp_project
        self.private_key_path = os.path.expanduser(private_key_path)
        self.public_key_path = os.path.expanduser(public_key_path)

    @property
    def name(self):
        return "gcp"

    @staticmethod
    def region_list():
        return [
            "us-central1-a",
            "us-east1-b",
            # "us-east4-a",
            "us-west1-a",
            # "us-west2-a",
            "southamerica-east1-a",
            "europe-north1-a",
            "europe-west1-b",
            "asia-east2-a",
        ]

    def get_instance_list(self, region) -> List[GCPServer]:
        gcp_instance_result = GCPServer.gcp_instances(self.gcp_project, region)
        if "items" in gcp_instance_result:
            instance_list = []
            for i in gcp_instance_result["items"]:
                instance_list.append(GCPServer(f"gcp:{region}", self.gcp_project, i["name"], ssh_private_key=self.private_key_path))
            return instance_list
        else:
            return []

    def get_matching_instances(self, network_tier=None, **kwargs):
        instances: List[Server] = super().get_matching_instances(**kwargs)
        matching_instances = []
        for instance in instances:
            if network_tier is None or instance.network_tier == network_tier:
                matching_instances.append(instance)
        return matching_instances

    def create_ssh_key(self):
        private_key_path = Path(self.private_key_path)
        if not private_key_path.exists():
            private_key_path.parent.mkdir(parents=True, exist_ok=True)
            key = paramiko.RSAKey.generate(4096)
            key.write_private_key_file(self.private_key_path, password="skylark")
            with open(self.public_key_path, "w") as f:
                f.write(f"{key.get_name()} {key.get_base64()}\n")

    def configure_default_network(self):
        compute = GCPServer.get_gcp_client()
        try:
            compute.networks().get(project=self.gcp_project, network="default").execute()
        except googleapiclient.errors.HttpError as e:
            if e.resp.status == 404:  # create network
                op = (
                    compute.networks()
                    .insert(project=self.gcp_project, body={"name": "default", "subnetMode": "auto", "autoCreateSubnetworks": True})
                    .execute()
                )
                self.wait_for_operation_to_complete("global", op["name"])
            else:
                raise e

    def configure_default_firewall(self, ip="0.0.0.0/0"):
        """Configure default firewall to allow access from all ports from all IPs (if not exists)."""
        compute = GCPServer.get_gcp_client()
        try:
            current_firewall = compute.firewalls().get(project=self.gcp_project, firewall="default").execute()
        except googleapiclient.errors.HttpError as e:
            if e.resp.status == 404:
                current_firewall = None
            else:
                raise e
        fw_body = {
            "name": "default",
            "allowed": [{"IPProtocol": "tcp", "ports": ["1-65535"]}, {"IPProtocol": "udp", "ports": ["1-65535"]}, {"IPProtocol": "icmp"}],
            "description": "Allow all traffic from all IPs",
            "sourceRanges": [ip],
        }
        if current_firewall is None:
            op = compute.firewalls().insert(project=self.gcp_project, body=fw_body).execute()
        else:
            op = compute.firewalls().update(project=self.gcp_project, firewall="default", body=fw_body).execute()
        self.wait_for_operation_to_complete("global", op["name"])

    def get_operation_state(self, zone, operation_name):
        compute = GCPServer.get_gcp_client()
        if zone == "global":
            return compute.globalOperations().get(project=self.gcp_project, operation=operation_name).execute()
        else:
            return compute.zoneOperations().get(project=self.gcp_project, zone=zone, operation=operation_name).execute()

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
        uname=os.environ.get("USER"),
        tags={"skylark": "true"},
    ) -> "GCPServer":
        assert not region.startswith("gcp:"), "Region should be GCP region"
        if name is None:
            name = f"skylark-gcp-{str(uuid.uuid4()).replace('-', '')}"
        compute = GCPServer.get_gcp_client("compute", "v1")
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
                        "sourceImage": "projects/ubuntu-os-cloud/global/images/family/ubuntu-1804-lts",
                        "diskType": f"zones/{region}/diskTypes/pd-standard",
                        "diskSizeGb": "100",
                    },
                }
            ],
            "networkInterfaces": [
                {
                    "network": "global/networks/default",
                    "accessConfigs": [
                        {
                            "name": "External NAT",
                            "type": "ONE_TO_ONE_NAT",
                            "networkTier": "PREMIUM" if premium_network else "STANDARD",
                        }
                    ],
                }
            ],
            "serviceAccounts": [{"email": "default", "scopes": ["https://www.googleapis.com/auth/cloud-platform"]}],
            "metadata": {"items": [{"key": "ssh-keys", "value": f"{uname}:{pub_key}\n"}]},
        }
        result = compute.instances().insert(project=self.gcp_project, zone=region, body=req_body).execute()
        self.wait_for_operation_to_complete(region, result["name"])
        server = GCPServer(f"gcp:{region}", self.gcp_project, name, ssh_private_key=self.private_key_path)
        server.wait_for_ready()
        return server
