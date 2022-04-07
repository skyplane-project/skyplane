import os
import uuid
from multiprocessing import BoundedSemaphore
from pathlib import Path
from typing import List, Optional

import paramiko
from skylark import key_root
from skylark.compute.azure.azure_auth import AzureAuthentication
from skylark.compute.azure.azure_server import AzureServer
from skylark.compute.cloud_providers import CloudProvider
from azure.mgmt.authorization.models import RoleAssignmentCreateParameters, RoleAssignmentProperties
from skylark.utils import logger
from skylark.utils.utils import Timer, do_parallel

from azure.mgmt.authorization.models import RoleAssignmentCreateParameters
from azure.mgmt.compute.models import ResourceIdentityType


class AzureCloudProvider(CloudProvider):
    def __init__(self, key_root=key_root / "azure"):
        super().__init__()
        self.auth = AzureAuthentication()

        key_root.mkdir(parents=True, exist_ok=True)
        self.private_key_path = key_root / "azure_key"
        self.public_key_path = key_root / "azure_key.pub"

        self.provisioning_semaphore = BoundedSemaphore(5)

    @property
    def name(self):
        return "azure"

    @staticmethod
    def region_list():
        return [
            "australiaeast",
            "brazilsouth",
            "canadacentral",
            "centralindia",
            "eastasia",
            "eastus",
            "eastus2",
            "francecentral",
            "germanywestcentral",
            "japaneast",
            "koreacentral",
            "northcentralus",
            "northeurope",
            "norwayeast",
            "southafricanorth",
            "swedencentral",
            "switzerlandnorth",
            "uaenorth",
            "uksouth",
            "westeurope",
            "westus",
            "westus2",
            "westus3",
            # D32_v4 or D32_v5 not available:
            #   "australiacentral",
            #   "australiasoutheast",
            #   "canadaeast",
            #   "centralus",
            #   "japanwest",
            #   "southcentralus",
            #   "southeastasia",
            #   "southindia",
            #   "ukwest",
            #   "westcentralus",
            #   "westindia",
            # not available due to restrictions:
            #   "australiacentral2",
            #   "brazilsoutheast",
            #   "francesouth",
            #   "germanynorth",
            #   "jioindiacentral",
            #   "jioindiawest",
            #   "koreasouth",
            #   "norwaywest",
            #   "southafricawest",
            #   "switzerlandwest",
            #   "uaecentral",
        ]

    @staticmethod
    def lookup_continent(region: str) -> str:
        lookup_dict = {
            "oceania": {"australiaeast", "australiacentral", "australiasoutheast", "australiacentral2"},
            "asia": {
                "eastasia",
                "japaneast",
                "japanwest",
                "koreacentral",
                "koreasouth",
                "southeastasia",
                "southindia",
                "centralindia",
                "westindia",
                "jioindiacentral",
                "jioindiawest",
            },
            "north-america": {
                "canadacentral",
                "canadaeast",
                "centralus",
                "eastus",
                "eastus2",
                "northcentralus",
                "westus",
                "westus2",
                "westus3",
                "southcentralus",
                "westcentralus",
            },
            "south-america": {"brazilsouth", "brazilsoutheast"},
            "europe": {
                "francecentral",
                "germanywestcentral",
                "northeurope",
                "norwayeast",
                "swedencentral",
                "switzerlandnorth",
                "switzerlandwest",
                "westeurope",
                "uksouth",
                "ukwest",
                "francesouth",
                "germanynorth",
                "norwaywest",
            },
            "africa": {"southafricanorth", "southafricawest"},
            "middle-east": {"uaenorth", "uaecentral"},
        }
        for continent, regions in lookup_dict.items():
            if region in regions:
                return continent
        return "unknown"

    @staticmethod
    def lookup_valid_instance(region: str, instance_name: str) -> Optional[str]:
        # todo this should query the Azure API for available SKUs
        available_regions = {
            "Standard_D32_v5": [
                "australiaeast",
                "canadacentral",
                "eastus",
                "eastus2",
                "francecentral",
                "germanywestcentral",
                "japaneast",
                "koreacentral",
                "northcentralus",
                "northeurope",
                "uksouth",
                "westeurope",
                "westus",
                "westus2",
                "westus3",
            ],
            "Standard_D32_v4": [
                "australiaeast",
                "brazilsouth",
                "canadacentral",
                "centralindia",
                "eastasia",
                "eastus",
                "francecentral",
                "germanywestcentral",
                "japaneast",
                "koreacentral",
                "northcentralus",
                "northeurope",
                "norwayeast",
                "southafricanorth",
                "swedencentral",
                "switzerlandnorth",
                "uaenorth",
                "uksouth",
                "westeurope",
                "westus",
                "westus3",
            ],
        }
        if region in available_regions["Standard_D32_v5"] and instance_name == "Standard_D32_v5":
            return "Standard_D32_v5"
        elif region in available_regions["Standard_D32_v4"] and instance_name == "Standard_D32_v5":
            return "Standard_D32_v4"
        else:
            logger.error(f"Cannot confirm availability of {instance_name} in {region}")
            return instance_name

    @staticmethod
    def get_transfer_cost(src_key, dst_key, premium_tier=True):
        """Assumes <10TB transfer tier."""
        src_provider, src_region = src_key.split(":")
        dst_provider, dst_region = dst_key.split(":")
        assert src_provider == "azure"
        if not premium_tier:
            # TODO: tracked in https://github.com/parasj/skylark/issues/59
            return NotImplementedError()

        src_continent = AzureCloudProvider.lookup_continent(src_region)
        dst_continent = AzureCloudProvider.lookup_continent(dst_region)

        if dst_provider != "azure":  # internet transfer
            # From North America, Europe to any destination
            if src_continent in {"north-america", "europe"}:
                return 0.0875
            # From Asia (China excluded), Australia, MEA to any destination
            elif src_continent in {"asia", "oceania", "middle-east", "africa"}:
                return 0.12
            # From South America to any destination
            elif src_continent == "south-america":
                return 0.181
            else:
                raise ValueError(f"Unknown transfer cost for {src_key} -> {dst_key}")
        else:  # local transfer
            # intracontinental transfer
            if src_continent == dst_continent:
                # Between regions within North America, Between regions within Europe
                if src_continent in {"north-america", "europe"} and dst_continent in {"north-america", "europe"}:
                    return 0.02
                # Between regions within Asia, Between regions within Oceania, Between regions within Middle East and Africa
                elif src_continent in {"asia", "oceania", "middle-east", "africa"} and dst_continent in {
                    "asia",
                    "oceania",
                    "middle-east",
                    "africa",
                }:
                    return 0.08
                # Between regions within South America
                elif src_continent == "south-america" and dst_continent == "south-america":
                    return 0.181
                else:
                    raise ValueError(f"Unknown transfer cost for {src_key} -> {dst_key}")
            # intercontinental transfer
            else:
                # From North America to other continents, From Europe to other continents
                if src_continent in {"north-america", "europe"}:
                    return 0.05
                # From Asia to other continents, From Oceania to other continents, From Africa to other continents
                elif src_continent in {"asia", "oceania", "middle-east", "africa"}:
                    return 0.08
                # From South America to other continents
                elif src_continent == "south-america":
                    return 0.16
                else:
                    raise ValueError(f"Unknown transfer cost for {src_key} -> {dst_key}")

    def get_instance_list(self, region: str) -> List[AzureServer]:
        compute_client = self.auth.get_compute_client()
        server_list = []
        for vm in compute_client.virtual_machines.list(AzureServer.resource_group_name):
            if vm.tags.get("skylark", None) == "true" and AzureServer.is_valid_vm_name(vm.name) and vm.location == region:
                name = AzureServer.base_name_from_vm_name(vm.name)
                s = AzureServer(name)
                if s.is_valid():
                    server_list.append(s)
                else:
                    logger.warning(
                        f"Warning: malformed Azure instance {name} found and ignored. You should go to the Microsoft Azure portal, investigate this manually, and delete any orphaned resources that may be allocated."
                    )
        return server_list

    # Copied from gcp_cloud_provider.py --- consolidate later?
    def create_ssh_key(self):
        private_key_path = Path(self.private_key_path)
        if not private_key_path.exists():
            private_key_path.parent.mkdir(parents=True, exist_ok=True)
            key = paramiko.RSAKey.generate(4096)
            key.write_private_key_file(self.private_key_path, password="skylark")
            with open(self.public_key_path, "w") as f:
                f.write(f"{key.get_name()} {key.get_base64()}\n")

    def set_up_resource_group(self, clean_up_orphans=True):
        resource_client = self.auth.get_resource_client()
        if resource_client.resource_groups.check_existence(AzureServer.resource_group_name):
            # Resource group already exists.
            # Take this moment to search for orphaned resources and clean them up...
            network_client = self.auth.get_network_client()
            if clean_up_orphans:
                instances_to_terminate = []
                for vnet in network_client.virtual_networks.list(AzureServer.resource_group_name):
                    if vnet.tags.get("skylark", None) == "true" and AzureServer.is_valid_vnet_name(vnet.name):
                        name = AzureServer.base_name_from_vnet_name(vnet.name)
                        s = AzureServer(name, assume_exists=False)
                        if not s.is_valid():
                            logger.warning(f"Cleaning up orphaned Azure resources for {name}...")
                            instances_to_terminate.append(s)

                if len(instances_to_terminate) > 0:
                    logger.info(f"Cleaning up {len(instances_to_terminate)} orphaned Azure resources...")
                    do_parallel(lambda i: i.terminate_instance_impl(), instances_to_terminate)
                    logger.info("Done cleaning up orphaned Azure resources")
            return
        rg_result = resource_client.resource_groups.create_or_update(
            AzureServer.resource_group_name, {"location": AzureServer.resource_group_location, "tags": {"skylark": "true"}}
        )
        assert rg_result.name == AzureServer.resource_group_name

    # This code, along with some code in azure_server.py, is based on
    # https://github.com/ucbrise/mage-scripts/blob/main/azure_cloud.py.
    def provision_instance(self, location: str, vm_size: str, name: Optional[str] = None, uname: str = "skylark") -> AzureServer:
        assert ":" not in location, "invalid colon in Azure location"

        if name is None:
            name = f"skylark-azure-{str(uuid.uuid4()).replace('-', '')}"

        with open(os.path.expanduser(self.public_key_path)) as f:
            pub_key = f.read()

        # Prepare for making Microsoft Azure API calls
        compute_client = self.auth.get_compute_client()
        network_client = self.auth.get_network_client()

        # Use the common resource group for this instance
        resource_group = AzureServer.resource_group_name

        # TODO: On future requests to create resources, check if a resource
        # with that name already exists, and fail the operation if so

        with Timer("Creating Azure network"):
            # Create a Virtual Network for this instance
            poller = network_client.virtual_networks.begin_create_or_update(
                resource_group,
                AzureServer.vnet_name(name),
                {"location": location, "tags": {"skylark": "true"}, "address_space": {"address_prefixes": ["10.0.0.0/24"]}},
            )
            poller.result()

            # Create a Network Security Group for this instance
            # NOTE: This is insecure. We should fix this soon.
            poller = network_client.network_security_groups.begin_create_or_update(
                resource_group,
                AzureServer.nsg_name(name),
                {
                    "location": location,
                    "tags": {"skylark": "true"},
                    "security_rules": [
                        {
                            "name": name + "-allow-all",
                            "protocol": "Tcp",
                            "source_port_range": "*",
                            "source_address_prefix": "*",
                            "destination_port_range": "*",
                            "destination_address_prefix": "*",
                            "access": "Allow",
                            "priority": 300,
                            "direction": "Inbound",
                        }
                        # Azure appears to add default rules for outbound connections
                    ],
                },
            )
            nsg_result = poller.result()

            # Create a subnet for this instance with the above Network Security Group
            subnet_poller = network_client.subnets.begin_create_or_update(
                resource_group,
                AzureServer.vnet_name(name),
                AzureServer.subnet_name(name),
                {"address_prefix": "10.0.0.0/26", "network_security_group": {"id": nsg_result.id}},
            )

            # Create a public IPv4 address for this instance
            ip_poller = network_client.public_ip_addresses.begin_create_or_update(
                resource_group,
                AzureServer.ip_name(name),
                {
                    "location": location,
                    "tags": {"skylark": "true"},
                    "sku": {"name": "Standard"},
                    "public_ip_allocation_method": "Static",
                    "public_ip_address_version": "IPV4",
                },
            )

            subnet_result = subnet_poller.result()
            public_ip_result = ip_poller.result()

        with Timer("Creating Azure NIC"):
            # Create a NIC for this instance, with accelerated networking enabled
            poller = network_client.network_interfaces.begin_create_or_update(
                resource_group,
                AzureServer.nic_name(name),
                {
                    "location": location,
                    "tags": {"skylark": "true"},
                    "ip_configurations": [
                        {"name": name + "-ip", "subnet": {"id": subnet_result.id}, "public_ip_address": {"id": public_ip_result.id}}
                    ],
                    "enable_accelerated_networking": True,
                },
            )
            nic_result = poller.result()

        # Create the VM
        with Timer("Creating Azure VM"):
            with self.provisioning_semaphore:
                poller = compute_client.virtual_machines.begin_create_or_update(
                    resource_group,
                    AzureServer.vm_name(name),
                    {
                        "location": location,
                        "tags": {"skylark": "true"},
                        "hardware_profile": {"vm_size": self.lookup_valid_instance(location, vm_size)},
                        "storage_profile": {
                            # "image_reference": {
                            #     "publisher": "canonical",
                            #     "offer": "0001-com-ubuntu-server-focal",
                            #     "sku": "20_04-lts",
                            #     "version": "latest",
                            # },
                            "image_reference": {
                                "publisher": "microsoft-aks",
                                "offer": "aks",
                                "sku": "aks-engine-ubuntu-1804-202112",
                                "version": "latest",
                            },
                            "os_disk": {"create_option": "FromImage", "delete_option": "Delete"},
                        },
                        "os_profile": {
                            "computer_name": AzureServer.vm_name(name),
                            "admin_username": uname,
                            "linux_configuration": {
                                "disable_password_authentication": True,
                                "ssh": {"public_keys": [{"path": f"/home/{uname}/.ssh/authorized_keys", "key_data": pub_key}]},
                            },
                        },
                        "network_profile": {"network_interfaces": [{"id": nic_result.id}]},
                        # give VM managed identity w/ system assigned identity
                        "identity": {"type": ResourceIdentityType.system_assigned},
                    },
                )
                vm_result = poller.result()

        def grant_vm_role(scope, role_name):
            auth_client = self.auth.get_authorization_client()
            roles = list(auth_client.role_definitions.list(scope, filter="roleName eq '{}'".format(role_name)))
            assert len(roles) == 1

            auth_client.role_assignments.create(
                scope,
                uuid.uuid4(),  # Role assignment random name
                RoleAssignmentCreateParameters(
                    properties=RoleAssignmentProperties(role_definition_id=roles[0].id, principal_id=vm_result.identity.principal_id)
                ),
            )

        with Timer("Role assignment"):
            # Assign roles to system MSI, see https://docs.microsoft.com/en-us/samples/azure-samples/compute-python-msi-vm/compute-python-msi-vm/#role-assignment
            # todo only grant storage-blob-data-reader and storage-blob-data-writer for specified buckets
            scope = f"/subscriptions/{self.auth.subscription_id}"
            grant_vm_role(scope, "Storage Blob Data Contributor")

        return AzureServer(name)
