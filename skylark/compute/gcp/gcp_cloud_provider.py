import os
import socket
from typing import List
import uuid
import googleapiclient
from loguru import logger

import paramiko

from skylark.compute.gcp.gcp_server import GCPServer, DEFAULT_GCP_PRIVATE_KEY_PATH, DEFAULT_GCP_PUBLIC_KEY_PATH
from skylark.compute.cloud_providers import CloudProvider


class GCPCloudProvider(CloudProvider):
    def __init__(self, gcp_project, private_key_path=DEFAULT_GCP_PRIVATE_KEY_PATH, public_key_path=DEFAULT_GCP_PUBLIC_KEY_PATH):
        super().__init__()
        self.gcp_project = gcp_project
        self.private_key_path = os.path.expanduser(private_key_path)
        self.public_key_path = os.path.expanduser(public_key_path)

    @staticmethod
    def region_list():
        return [
            "us-central1-a",
            # "us-east1-b",
            # "us-east4-a",
            # "us-west1-a",
            "us-west2-a",
            "southamerica-east1-a",
            "europe-north1-a",
            # "europe-west1-b",
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

    def create_ssh_key(self):
        if not os.path.exists(self.private_key_path):
            logger.info(f"Creating SSH key at {self.private_key_path}")
            key = paramiko.RSAKey.generate(4096)
            key.write_private_key_file(self.private_key_path, password=None)
            with open(self.public_key_path, "w") as f:
                f.write(f"ssh-rsa {key.get_base64()} {os.environ['USER']}@{socket.gethostname()}")

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
            compute.firewalls().insert(project=self.gcp_project, body=fw_body).execute()
        else:
            compute.firewalls().update(project=self.gcp_project, firewall="default", body=fw_body).execute()

    def provision_instance(
        self,
        region,
        instance_class,
        name=None,
        premium_network=False,
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
                    "accessConfigs": [{"name": "External NAT", "type": "ONE_TO_ONE_NAT"}],
                    "networkTier": "PREMIUM" if premium_network else "STANDARD",
                }
            ],
            "serviceAccounts": [{"email": "default", "scopes": ["https://www.googleapis.com/auth/cloud-platform"]}],
            "metadata": {"items": [{"key": "ssh-keys", "value": f"{pub_key}\n"}]},
        }
        compute.instances().insert(project=self.gcp_project, zone=region, body=req_body).execute()
        server = GCPServer(f"gcp:{region}", self.gcp_project, name, ssh_private_key=self.private_key_path)
        server.wait_for_ready()
        return server
