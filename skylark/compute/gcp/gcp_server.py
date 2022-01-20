import os
from functools import lru_cache
from pathlib import Path

import googleapiclient.discovery
import paramiko
from skylark import key_root
from skylark.compute.server import Server, ServerState
from skylark.utils.cache import ignore_lru_cache
from skylark.utils.utils import PathLike


class GCPServer(Server):
    def __init__(
        self,
        region_tag: str,
        gcp_project: str,
        instance_name: str,
        key_root: PathLike = key_root / "gcp",
        log_dir=None,
        ssh_private_key=None,
    ):
        super().__init__(region_tag, log_dir=log_dir)
        assert self.region_tag.split(":")[0] == "gcp", f"Region name doesn't match pattern gcp:<region> {self.region_tag}"
        self.gcp_region = self.region_tag.split(":")[1]
        self.gcp_project = gcp_project
        self.gcp_instance_name = instance_name
        key_root = Path(key_root)
        key_root.mkdir(parents=True, exist_ok=True)
        if ssh_private_key is None:
            self.ssh_private_key = key_root / f"gcp-cert.pem"
        else:
            self.ssh_private_key = ssh_private_key

    def uuid(self):
        return f"{self.gcp_project}:{self.region_tag}:{self.gcp_instance_name}"

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

    @ignore_lru_cache()
    def public_ip(self):
        """Get public IP for instance with GCP client"""
        return self.get_instance_property("networkInterfaces")[0]["accessConfigs"][0].get("natIP")

    @ignore_lru_cache()
    def instance_class(self):
        return self.get_instance_property("machineType").split("/")[-1]

    def region(self):
        return self.gcp_region

    def instance_state(self):
        return ServerState.from_gcp_state(self.get_instance_property("status"))

    @ignore_lru_cache()
    def instance_name(self):
        return self.get_instance_property("name")

    @ignore_lru_cache()
    def tags(self):
        """Get labels for instance."""
        return self.get_instance_property("labels")

    @ignore_lru_cache()
    def network_tier(self):
        interface = self.get_instance_property("networkInterfaces")[0]
        return interface["accessConfigs"][0]["networkTier"]

    def __repr__(self):
        return f"GCPServer(region_tag={self.region_tag}, gcp_project={self.gcp_project}, instance_name={self.gcp_instance_name})"

    def terminate_instance_impl(self):
        compute = self.get_gcp_client()
        compute.instances().delete(project=self.gcp_project, zone=self.gcp_region, instance=self.instance_name()).execute()

    def get_ssh_client_impl(self, uname=os.environ.get("USER"), ssh_key_password="skylark"):
        """Return paramiko client that connects to this instance."""
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh_client.connect(
            hostname=self.public_ip(),
            username=uname,
            key_filename=str(self.ssh_private_key),
            passphrase=ssh_key_password,
            look_for_keys=False,
            banner_timeout=200,
        )
        return ssh_client
