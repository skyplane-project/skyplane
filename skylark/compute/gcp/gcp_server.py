import os
import socket
from functools import lru_cache
import uuid

import googleapiclient.discovery
from loguru import logger
import paramiko

from skylark.compute.server import Server, ServerState
from skylark import skylark_root

DEFAULT_GCP_PRIVATE_KEY_PATH = os.path.expanduser("~/.ssh/google_compute_engine")
DEFAULT_GCP_PUBLIC_KEY_PATH = os.path.expanduser("~/.ssh/google_compute_engine.pub")


class GCPServer(Server):
    def __init__(self, region_tag, gcp_project, instance_name, ssh_private_key=DEFAULT_GCP_PRIVATE_KEY_PATH, command_log_file=None):
        super().__init__(command_log_file=command_log_file)
        self.region_tag = region_tag
        assert region_tag.split(":")[0] == "gcp", f"Region name doesn't match pattern gcp:<region> {region_tag}"
        self.gcp_region = region_tag.split(":")[1]
        self.gcp_project = gcp_project
        self.gcp_instance_name = instance_name
        self.ssh_private_key = os.path.expanduser(ssh_private_key)

    @classmethod
    def get_gcp_client(cls, service_name="compute", version="v1"):
        ns_key = f"gcp_{service_name}_v{version}"
        if not hasattr(cls.ns, ns_key):
            setattr(cls.ns, ns_key, googleapiclient.discovery.build("compute", "v1"))
        return getattr(cls.ns, ns_key)

    def gcp_instances(self):
        compute = self.get_gcp_client()
        return compute.instances().list(project=self.gcp_project, zone=self.gcp_region).execute()

    @lru_cache
    def get_gcp_instance(self):
        instances = self.gcp_instances()
        for i in instances["items"]:
            if i["name"] == self.gcp_instance_name:
                return i
        raise ValueError(f"No instance found with name {self.gcp_instance_name}")

    def get_instance_property(self, prop):
        return self.get_gcp_instance()[prop]

    @property
    def public_ip(self):
        """Get public IP for instance with GCP client"""
        return self.get_instance_property("networkInterfaces")[0]["accessConfigs"][0].get("natIP")

    @property
    def instance_class(self):
        return self.get_instance_property("machineType").split("/")[-1]

    @property
    def region(self):
        return self.gcp_region

    @property
    def instance_state(self):
        return ServerState.from_gcp_state(self.get_instance_property("status"))

    @property
    def instance_name(self):
        return self.get_instance_property("name")

    @property
    def tags(self):
        """Get labels for instance."""
        return self.get_instance_property("labels")

    def __repr__(self):
        str_repr = "GCPServer("
        str_repr += f"{self.region_tag}, "
        str_repr += f"{self.gcp_project}, "
        str_repr += f"{self.instance_name}, "
        str_repr += f"{self.command_log_file}"
        str_repr += ")"
        return str_repr

    def terminate_instance_impl(self):
        compute = self.get_gcp_client()
        compute.instances().delete(project=self.gcp_project, zone=self.gcp_region, instance=self.instance_name).execute()

    @staticmethod
    def configure_default_firewall(gcp_project):
        """Configure default firewall to allow access from all ports from all IPs (if not exists)."""
        compute = GCPServer.get_gcp_client()
        try:
            current_firewall = compute.firewalls().get(project=gcp_project, firewall="default").execute()
        except googleapiclient.errors.HttpError as e:
            if e.resp.status == 404:
                current_firewall = None
            else:
                raise e
        fw_body = {
            "name": "default",
            "allowed": [{"IPProtocol": "tcp", "ports": ["1-65535"]}, {"IPProtocol": "udp", "ports": ["1-65535"]}, {"IPProtocol": "icmp"}],
            "description": "Allow all traffic from all IPs",
            "sourceRanges": ["0.0.0.0/0"],
        }
        if current_firewall is None:
            compute.firewalls().insert(project=gcp_project, body=fw_body).execute()
        else:
            compute.firewalls().update(project=gcp_project, firewall="default", body=fw_body).execute()

    @staticmethod
    def create_ssh_key(ssh_private_key=DEFAULT_GCP_PRIVATE_KEY_PATH, ssh_public_key=DEFAULT_GCP_PUBLIC_KEY_PATH):
        ssh_private_key = os.path.expanduser(ssh_private_key)
        if not os.path.exists(ssh_private_key):
            logger.info(f"Creating SSH key at {ssh_private_key}")
            key = paramiko.RSAKey.generate(4096)
            key.write_private_key_file(ssh_private_key, password=None)
            with open(ssh_public_key, "w") as f:
                f.write(f"ssh-rsa {key.get_base64()} {os.environ['USER']}@{socket.gethostname()}")

    @staticmethod
    def provision_instance(
        region,
        gcp_project,
        instance_class,
        name=None,
        premium_network=False,
        ssh_private_key=DEFAULT_GCP_PRIVATE_KEY_PATH,
        ssh_public_key=DEFAULT_GCP_PUBLIC_KEY_PATH,
        tags={"skylark": "true"},
    ) -> "GCPServer":
        assert not region.startswith("gcp:"), "Region should be GCP region"
        if name is None:
            name = f"skylark-{str(uuid.uuid4()).replace('-', '')}"
        compute = GCPServer.get_gcp_client("compute", "v1")
        with open(os.path.expanduser(ssh_public_key)) as f:
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
        compute.instances().insert(project=gcp_project, zone=region, body=req_body).execute()
        return GCPServer(f"gcp:{region}", gcp_project, name, ssh_private_key=ssh_private_key)

    def get_ssh_client_impl(self):
        """Return paramiko client that connects to this instance."""
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        pkey = paramiko.RSAKey.from_private_key_file(self.ssh_private_key, password=None)
        uname = os.environ["USER"]  # ideally, create VM with username ubuntu
        ssh_client.connect(hostname=self.public_ip, username=uname, pkey=pkey)
        return ssh_client
