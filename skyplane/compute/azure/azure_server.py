import uuid
from pathlib import Path

import azure.core.exceptions
import paramiko
from azure.mgmt.authorization.models import RoleAssignmentCreateParameters
from azure.mgmt.authorization.models import RoleAssignmentProperties

from skyplane import key_root
from skyplane.compute.azure.azure_auth import AzureAuthentication
from skyplane.compute.server import Server, ServerState
from skyplane.utils import logger
from skyplane.utils.cache import ignore_lru_cache
from skyplane.utils.fn import PathLike, wait_for


class AzureServer(Server):
    resource_group_name = "skyplane"
    resource_group_location = "westus2"

    def __init__(
        self,
        name: str,
        key_root: PathLike = key_root / "azure",
        log_dir=None,
        ssh_private_key=None,
        assume_exists=True,
    ):
        self.auth = AzureAuthentication()
        self.name = name
        self.location = None

        if assume_exists:
            vm = self.get_virtual_machine()
            self.location = vm.location
            region_tag = f"azure:{self.location}"
        else:
            region_tag = "azure:UNKNOWN"

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
    def is_valid_vnet_name(name):
        return name.endswith("-vnet")

    @staticmethod
    def base_name_from_vnet_name(name):
        assert AzureServer.is_valid_vnet_name(name)
        return name[:-5]

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
    def is_valid_vm_name(name):
        return name.endswith("-vm")

    @staticmethod
    def base_name_from_vm_name(name):
        assert AzureServer.is_valid_vm_name(name)
        return name[:-3]

    @staticmethod
    def ip_name(name):
        return AzureServer.vm_name(name) + "-ip"

    @staticmethod
    def nic_name(name):
        return AzureServer.vm_name(name) + "-nic"

    def get_virtual_machine(self):
        compute_client = self.auth.get_compute_client()
        vm = compute_client.virtual_machines.get(AzureServer.resource_group_name, AzureServer.vm_name(self.name))

        # Sanity checks
        assert self.location is None or vm.location == self.location
        assert vm.name == AzureServer.vm_name(self.name)

        return vm

    def is_valid(self):
        try:
            vm = self.get_virtual_machine()
            return vm.provisioning_state == "Succeeded"
        except azure.core.exceptions.ResourceNotFoundError:
            return False

    def uuid(self):
        return f"{self.region_tag}:{self.name}"

    def instance_state(self) -> ServerState:
        compute_client = self.auth.get_compute_client()
        vm_instance_view = compute_client.virtual_machines.instance_view(AzureServer.resource_group_name, AzureServer.vm_name(self.name))
        statuses = vm_instance_view.statuses
        for status in statuses:
            if status.code.startswith("PowerState"):
                return ServerState.from_azure_state(status.code)
        return ServerState.UNKNOWN

    @ignore_lru_cache()
    def public_ip(self):
        network_client = self.auth.get_network_client()
        public_ip = network_client.public_ip_addresses.get(AzureServer.resource_group_name, AzureServer.ip_name(self.name))

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
        vm = self.get_virtual_machine()
        return vm.tags

    def network_tier(self):
        return "PREMIUM"

    def terminate_instance_impl(self):
        compute_client = self.auth.get_compute_client()
        network_client = self.auth.get_network_client()

        # remove any role assignments to the VM's system assigned identity
        auth_client = self.auth.get_authorization_client()
        vm = self.get_virtual_machine()
        for assignment in auth_client.role_assignments.list(filter="principalId eq '{}'".format(vm.identity.principal_id)):
            logger.fs.debug(f"Deleting role assignment {assignment.name}")
            auth_client.role_assignments.delete(
                scope=assignment.scope,
                role_assignment_name=assignment.name,
            )

        vm_poller = compute_client.virtual_machines.begin_delete(AzureServer.resource_group_name, self.vm_name(self.name))
        _ = vm_poller.result()
        nic_poller = network_client.network_interfaces.begin_delete(AzureServer.resource_group_name, self.nic_name(self.name))
        _ = nic_poller.result()
        ip_poller = network_client.public_ip_addresses.begin_delete(AzureServer.resource_group_name, self.ip_name(self.name))
        subnet_poller = network_client.subnets.begin_delete(
            AzureServer.resource_group_name, self.vnet_name(self.name), self.subnet_name(self.name)
        )
        _ = ip_poller.result()
        _ = subnet_poller.result()
        nsg_poller = network_client.network_security_groups.begin_delete(AzureServer.resource_group_name, self.nsg_name(self.name))
        _ = nsg_poller.result()
        vnet_poller = network_client.virtual_networks.begin_delete(AzureServer.resource_group_name, self.vnet_name(self.name))
        _ = vnet_poller.result()

    def authorize_subscription(self):
        # Authorize system MSI to access subscription
        auth_client = self.auth.get_authorization_client()
        subscription_scope = "/subscriptions/{}".format(self.auth.subscription_id)
        principal_id = self.get_virtual_machine().identity.principal_id

        def grant_vm_role(principal_id, scope, role_name):
            prefix = f"grant_vm_role({principal_id}, {scope.split('/')[-1]}, {role_name})"
            try:
                roles = list(auth_client.role_definitions.list(scope, filter="roleName eq '{}'".format(role_name)))
                assert len(roles) == 1, f"Got roles {roles}"
                params = RoleAssignmentCreateParameters(
                    properties=RoleAssignmentProperties(role_definition_id=roles[0].id, principal_id=principal_id)
                )
                auth_client.role_assignments.create(scope, uuid.uuid4(), params)
                return roles[0]
            except azure.core.exceptions.ResourceExistsError as e:
                logger.fs.warning(f"{prefix}: Role '{role_name}' already exists: {e}")
                return None

        r1 = grant_vm_role(principal_id, subscription_scope, "Storage Blob Data Contributor")
        r2 = grant_vm_role(principal_id, subscription_scope, "Storage Blob Data Reader")
        r3 = grant_vm_role(principal_id, subscription_scope, "Storage Blob Delegator")
        r4 = grant_vm_role(principal_id, subscription_scope, "Storage Account Contributor")

        # wait till the subscription is accessible by checking for roles
        def check_role(role):
            if role is None:
                return True
            for assignment in auth_client.role_assignments.list_for_scope(
                subscription_scope, filter="principalId eq '{}'".format(principal_id)
            ):
                if assignment.role_definition_id == role.id:
                    return True
            return False

        wait_for(lambda: all(check_role(role) for role in [r1, r2, r3, r4]), timeout=60, desc="authorize_subscription")
        logger.fs.debug(f"Authorized subscription for VM {self.name}")

    def get_ssh_client_impl(self, uname="skyplane", ssh_key_password="skyplane"):
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
        return f"ssh -i {self.ssh_private_key} {uname}@{self.public_ip()}"
