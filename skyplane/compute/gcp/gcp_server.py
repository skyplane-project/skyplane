from functools import lru_cache
from pathlib import Path

import paramiko

from skyplane import key_root
from skyplane.compute.gcp.gcp_auth import GCPAuthentication
from skyplane.compute.server import Server, ServerState
from skyplane.utils.fn import PathLike


class GCPServer(Server):
    def __init__(
        self,
        region_tag: str,
        instance_name: str,
        key_root: PathLike = key_root / "gcp",
        log_dir=None,
        ssh_private_key=None,
    ):
        super().__init__(region_tag, log_dir=log_dir)
        assert self.region_tag.split(":")[0] == "gcp", f"Region name doesn't match pattern gcp:<region> {self.region_tag}"
        self.gcp_region = self.region_tag.split(":")[1]
        self.auth = GCPAuthentication()
        self.gcp_instance_name = instance_name
        key_root = Path(key_root)
        key_root.mkdir(parents=True, exist_ok=True)
        if ssh_private_key is None:
            self.ssh_private_key = key_root / f"gcp-cert.pem"
        else:
            self.ssh_private_key = ssh_private_key

    def uuid(self):
        return f"{self.region_tag}:{self.gcp_instance_name}"

    @lru_cache(maxsize=1)
    def get_gcp_instance(self):
        instances = self.auth.get_gcp_instances(self.gcp_region)
        if "items" in instances:
            for i in instances["items"]:
                if i["name"] == self.gcp_instance_name:
                    return i
        raise ValueError(f"No instance found with name {self.gcp_instance_name}, {instances}")

    def get_instance_property(self, prop):
        instance = self.get_gcp_instance()
        if prop in instance:
            return instance[prop]
        else:
            return None

    def public_ip(self):
        """Get public IP for instance with GCP client"""
        return self.get_instance_property("networkInterfaces")[0]["accessConfigs"][0].get("natIP")

    def instance_class(self):
        machine_type = self.get_instance_property("machineType")
        return machine_type.split("/")[-1] if machine_type else None

    def region(self):
        return self.gcp_region

    def instance_state(self):
        return ServerState.from_gcp_state(self.get_instance_property("status"))

    def instance_name(self):
        return self.get_instance_property("name")

    def tags(self):
        """Get labels for instance."""
        return self.get_instance_property("labels") or {}

    def network_tier(self):
        interface = self.get_instance_property("networkInterfaces")[0]
        return interface["accessConfigs"][0]["networkTier"]

    def __repr__(self):
        return f"GCPServer(region_tag={self.region_tag}, instance_name={self.gcp_instance_name})"

    def terminate_instance_impl(self):
        self.auth.get_gcp_client().instances().delete(
            project=self.auth.project_id, zone=self.gcp_region, instance=self.instance_name()
        ).execute()

    def get_ssh_client_impl(self, uname="skyplane", ssh_key_password="skyplane"):
        """Return paramiko client that connects to this instance."""
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh_client.connect(
            hostname=self.public_ip(),
            username=uname,
            pkey=paramiko.RSAKey.from_private_key_file(str(self.ssh_private_key), password=ssh_key_password),
            look_for_keys=False,
            banner_timeout=200,
        )
        return ssh_client

    def get_sftp_client(self, uname="skyplane", ssh_key_password="skyplane"):
        t = paramiko.Transport((self.public_ip(), 22))
        pkey = paramiko.RSAKey.from_private_key_file(str(self.ssh_private_key), password=ssh_key_password)
        t.connect(username=uname, pkey=pkey)
        return paramiko.SFTPClient.from_transport(t)

    def open_ssh_tunnel_impl(self, remote_port, uname="skyplane", ssh_key_password="skyplane"):
        import sshtunnel

        return sshtunnel.SSHTunnelForwarder(
            (self.public_ip(), 22),
            ssh_username=uname,
            ssh_pkey=str(self.ssh_private_key),
            ssh_private_key_password=ssh_key_password,
            local_bind_address=("127.0.0.1", 0),
            remote_bind_address=("127.0.0.1", remote_port),
        )

    def get_ssh_cmd(self, uname="skyplane", ssh_key_password="skyplane"):
        # todo can we include the key password inline?
        return f"ssh -i {self.ssh_private_key} -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no {uname}@{self.public_ip()}"
