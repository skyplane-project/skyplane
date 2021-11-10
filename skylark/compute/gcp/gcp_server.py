import os
from functools import lru_cache

import googleapiclient.discovery
from loguru import logger
import paramiko

from skylark.compute.server import Server, ServerState

DEFAULT_GCP_PRIVATE_KEY_PATH = os.path.expanduser("~/.ssh/google_compute_engine")
DEFAULT_GCP_PUBLIC_KEY_PATH = os.path.expanduser("~/.ssh/google_compute_engine.pub")


class GCPServer(Server):
    def __init__(self, region_tag, gcp_project, instance_name, ssh_private_key=DEFAULT_GCP_PRIVATE_KEY_PATH, command_log_file=None):
        super().__init__(region_tag, command_log_file=command_log_file)
        assert self.region_tag.split(":")[0] == "gcp", f"Region name doesn't match pattern gcp:<region> {self.region_tag}"
        self.gcp_region = self.region_tag.split(":")[1]
        self.gcp_project = gcp_project
        self.gcp_instance_name = instance_name
        self.ssh_private_key = os.path.expanduser(ssh_private_key)

    @classmethod
    def get_gcp_client(cls, service_name="compute", version="v1"):
        ns_key = f"gcp_{service_name}_v{version}"
        if not hasattr(cls.ns, ns_key):
            setattr(cls.ns, ns_key, googleapiclient.discovery.build("compute", "v1"))
        return getattr(cls.ns, ns_key)

    @staticmethod
    def gcp_instances(gcp_project, gcp_region):
        compute = GCPServer.get_gcp_client()
        return compute.instances().list(project=gcp_project, zone=gcp_region).execute()

    @lru_cache
    def get_gcp_instance(self):
        instances = self.gcp_instances(self.gcp_project, self.gcp_region)
        if "items" in instances:
            for i in instances["items"]:
                if i["name"] == self.gcp_instance_name:
                    return i
        raise ValueError(f"No instance found with name {self.gcp_instance_name}, {instances}")

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

    def get_ssh_client_impl(self, uname=os.environ.get("USER"), ssh_key_password="skylark"):
        """Return paramiko client that connects to this instance."""
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        logger.debug(f"({self.region_tag}) ssh -i {self.ssh_private_key} {uname}@{self.public_ip}")
        ssh_client.connect(hostname=self.public_ip, username=uname, key_filename=self.ssh_private_key, passphrase=ssh_key_password, look_for_keys=False)
        return ssh_client