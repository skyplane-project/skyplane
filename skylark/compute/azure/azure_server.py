import os
from pathlib import Path

import paramiko
from skylark import key_root
from skylark.compute.server import Server, ServerState
from skylark.utils.cache import ignore_lru_cache
from skylark.utils.utils import PathLike

# import azure.core.exceptions
# from azure.identity import DefaultAzureCredential
# from azure.mgmt.compute import ComputeManagementClient
# from azure.mgmt.network import NetworkManagementClient
# from azure.mgmt.resource import ResourceManagementClient


class AzureServer(Server):
    def __init__(self, subscription_id: str, name: str, key_root: PathLike = key_root / "azure", log_dir=None, ssh_private_key=None):
        self.subscription_id = subscription_id
        self.name = name
        self.location = None

        resource_group = self.get_resource_group()

        self.location = resource_group.location
        region_tag = f"azure:{self.location}"

        super().__init__(region_tag, log_dir=log_dir)

        key_root = Path(key_root)
        key_root.mkdir(parents=True, exist_ok=True)
        if ssh_private_key is None:
            self.ssh_private_key = key_root / "azure_key"
        else:
            self.ssh_private_key = ssh_private_key

        self.cached_public_ip_address = None

    @staticmethod
    def vnet_name(name):
        return name + "-vnet"

    @staticmethod
    def nsg_name(name):
        return name + "-nsg"

    @staticmethod
    def subnet_name(name):
        return name + "-subnet"

    @staticmethod
    def vm_name(name):
        return name + "-vm"

    @staticmethod
    def wdisk_name(name):
        return AzureServer.vm_name(name) + "-wdisk"

    @staticmethod
    def ip_name(name):
        return AzureServer.vm_name(name) + "-ip"

    @staticmethod
    def nic_name(name):
        return AzureServer.vm_name(name) + "-nic"

    def get_resource_group(self):
        credential = DefaultAzureCredential()
        resource_client = ResourceManagementClient(credential, self.subscription_id)
        rg = resource_client.resource_groups.get(self.name)

        # Sanity checks
        assert self.location is None or rg.location == self.location
        assert rg.name == self.name
        assert rg.tags.get("skylark", None) == "true"

        return rg

    def get_virtual_machine(self):
        credential = DefaultAzureCredential()
        compute_client = ComputeManagementClient(credential, self.subscription_id)
        vm = compute_client.virtual_machines.get(self.name, AzureServer.vm_name(self.name))

        # Sanity checks
        assert vm.location == self.location
        assert vm.name == AzureServer.vm_name(self.name)

        return vm

    def is_valid(self):
        try:
            _ = self.get_virtual_machine()
            return True
        except azure.core.exceptions.ResourceNotFoundError:
            return False

    def uuid(self):
        return f"{self.subscription_id}:{self.region_tag}:{self.name}"

    def instance_state(self) -> ServerState:
        credential = DefaultAzureCredential()
        compute_client = ComputeManagementClient(credential, self.subscription_id)
        vm_instance_view = compute_client.virtual_machines.instance_view(self.name, AzureServer.vm_name(self.name))
        statuses = vm_instance_view.statuses
        for status in statuses:
            if status.code.startswith("PowerState"):
                return ServerState.from_azure_state(status.code)
        return ServerState.UNKNOWN

    @ignore_lru_cache()
    def public_ip(self):
        credential = DefaultAzureCredential()
        network_client = NetworkManagementClient(credential, self.subscription_id)
        public_ip = network_client.public_ip_addresses.get(self.name, AzureServer.ip_name(self.name))

        # Sanity checks
        assert public_ip.location == self.location
        assert public_ip.name == AzureServer.ip_name(self.name)
        return public_ip.ip_address

    @ignore_lru_cache()
    def instance_class(self):
        vm = self.get_virtual_machine()
        return vm.hardware_profile.vm_size

    def region(self):
        return self.location

    def instance_name(self):
        return self.name

    @ignore_lru_cache()
    def tags(self):
        resource_group = self.get_resource_group()
        return resource_group.tags

    def network_tier(self):
        return "PREMIUM"

    def terminate_instance_impl(self):
        credential = DefaultAzureCredential()
        resource_client = ResourceManagementClient(credential, self.subscription_id)
        _ = self.get_resource_group()  # for the sanity checks
        poller = resource_client.resource_groups.begin_delete(self.name)
        _ = poller.result()

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
