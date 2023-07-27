#
# (C) Copyright Cloudlab URV 2020
# (C) Copyright IBM Corp. 2023
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import functools
import inspect
import re
import os
import paramiko
import time
import logging
import uuid
import random
from concurrent.futures import ThreadPoolExecutor

from skyplane.compute.ibmcloud.ibm_gen2.ssh_client import SSHClient
from skyplane.compute.ibmcloud.ibm_gen2.constants import COMPUTE_CLI_MSG, CACHE_DIR
from skyplane.compute.ibmcloud.ibm_gen2.utils import load_yaml_config, dump_yaml_config, delete_yaml_config

from skyplane.utils import imports

logger = logging.getLogger(__name__)

INSTANCE_START_TIMEOUT = 180 * 2
VPC_API_VERSION = "2021-09-21"


class IBMVPCBackend:
    def __init__(self, ibm_vpc_config):
        logger.debug("Creating IBM VPC client")
        self.name = "ibm_gen2"
        self.config = ibm_vpc_config

        self.vpc_name = None
        self.vpc_data = None
        self.vpc_key = None

        self.endpoint = self.config["endpoint"]
        self.region = self.endpoint.split("//")[1].split(".")[0]
        self.cache_dir = os.path.join(CACHE_DIR, self.name)
        self.vpc_data_filename = os.path.join(self.cache_dir, self.region + "_data")
        self.custom_image = self.config.get("custom_skyplane_image")

        logger.debug(f"Setting VPC endpoint to: {self.endpoint}")

        self.workers = []

        self.iam_api_key = self.config.get("iam_api_key")
        self.vpc_cli = self.create_vpc_cli()
        # authenticator = ibm_cloud_sdk_core.authenticators(self.iam_api_key, url=self.config.get("iam_endpoint"))
        # self.vpc_cli = ibm_vpc.VpcV1(VPC_API_VERSION, authenticator=authenticator)
        # self.vpc_cli.set_service_url(self.config["endpoint"] + "/v1")

        user_agent_string = "ibm_vpc_{}".format(self.config["user_agent"])
        self.vpc_cli._set_user_agent_header(user_agent_string)

        # decorate instance public methods with except/retry logic
        decorate_instance(self.vpc_cli, vpc_retry_on_except)

        msg = COMPUTE_CLI_MSG.format("IBM VPC")
        logger.info(f"{msg} - Region: {self.region}")

    @imports.inject("ibm_vpc", "ibm_cloud_sdk_core", pip_extra="ibmcloud")
    def create_vpc_cli(ibm_vpc, ibm_cloud_sdk_core, self):
        authenticator = ibm_cloud_sdk_core.authenticators(self.iam_api_key, url=self.config.get("iam_endpoint"))
        vpc_cli = ibm_vpc.VpcV1(VPC_API_VERSION, authenticator=authenticator)
        vpc_cli.set_service_url(self.config["endpoint"] + "/v1")
        return vpc_cli

    def _load_vpc_data(self):
        """
        Loads VPC data from local cache
        """
        self.vpc_data = load_yaml_config(self.vpc_data_filename)
        print("load VPC data")
        print(self.vpc_data)

        if not self.vpc_data:
            logger.debug(f"Could not find VPC cache data in {self.vpc_data_filename}")
        elif "vpc_id" in self.vpc_data:
            self.vpc_key = self.vpc_data["vpc_id"].split("-")[2]

    def _dump_vpc_data(self):
        """
        Dumps VPC data to local cache
        """
        dump_yaml_config(self.vpc_data_filename, self.vpc_data)

    @imports.inject("ibm_cloud_sdk_core", pip_extra="ibmcloud")
    def _create_vpc(ibm_cloud_sdk_core, self):
        """
        Creates a new VPC
        """
        if "vpc_id" in self.config:
            print("vpc_id in self.config")
            return

        if "vpc_id" in self.vpc_data:
            print("vpc_id in self.vpc_data")
            try:
                self.vpc_cli.get_vpc(self.vpc_data["vpc_id"])
                self.config["vpc_id"] = self.vpc_data["vpc_id"]
                self.config["security_group_id"] = self.vpc_data["security_group_id"]
                print(self.config)
                return
            except ibm_cloud_sdk_core.ApiException:
                pass

        vpc_info = None

        host_id = str(uuid.getnode())[-6:]
        iam_id = self.iam_api_key[:4].lower()
        self.vpc_name = self.config.get("vpc_name", f"skyplane-vpc-{iam_id}-{host_id}")
        logger.debug(f"Setting VPC name to: {self.vpc_name}")

        assert re.match("^[a-z0-9-:-]*$", self.vpc_name), 'VPC name "{}" not valid'.format(self.vpc_name)

        vpcs_info = self.vpc_cli.list_vpcs().get_result()
        for vpc in vpcs_info["vpcs"]:
            if vpc["name"] == self.vpc_name:
                vpc_info = vpc

        if not vpc_info:
            logger.debug(f"Creating VPC {self.vpc_name}")
            vpc_prototype = {}
            vpc_prototype["address_prefix_management"] = "auto"
            vpc_prototype["classic_access"] = False
            vpc_prototype["name"] = self.vpc_name
            vpc_prototype["resource_group"] = {"id": self.config["resource_group_id"]}

            response = self.vpc_cli.create_vpc(**vpc_prototype)
            vpc_info = response.result

        self.config["vpc_id"] = vpc_info["id"]
        self.config["security_group_id"] = vpc_info["default_security_group"]["id"]

        # Set the prefix used for the VPC resources
        self.vpc_key = self.config["vpc_id"].split("-")[2]

        deloy_ssh_rule = True
        deploy_icmp_rule = True

        sg_rule_prototype_ssh = {}
        sg_rule_prototype_ssh["direction"] = "inbound"
        sg_rule_prototype_ssh["ip_version"] = "ipv4"
        sg_rule_prototype_ssh["protocol"] = "tcp"
        sg_rule_prototype_ssh["port_min"] = 22
        sg_rule_prototype_ssh["port_max"] = 22

        sg_rule_prototype_icmp = {}
        sg_rule_prototype_icmp["direction"] = "inbound"
        sg_rule_prototype_icmp["ip_version"] = "ipv4"
        sg_rule_prototype_icmp["protocol"] = "icmp"
        sg_rule_prototype_icmp["type"] = 8

        sg_rules = self.vpc_cli.get_security_group(self.config["security_group_id"])
        for rule in sg_rules.get_result()["rules"]:
            if all(item in rule.items() for item in sg_rule_prototype_ssh.items()):
                deloy_ssh_rule = False
            if all(item in rule.items() for item in sg_rule_prototype_icmp.items()):
                deploy_icmp_rule = False

        if deloy_ssh_rule:
            self.vpc_cli.create_security_group_rule(self.config["security_group_id"], sg_rule_prototype_ssh)
        if deploy_icmp_rule:
            self.vpc_cli.create_security_group_rule(self.config["security_group_id"], sg_rule_prototype_icmp)

    @imports.inject("ibm_cloud_sdk_core", pip_extra="ibmcloud")
    def _create_ssh_key(ibm_cloud_sdk_core, self):
        if "ssh_key_id" in self.config:
            print("ssh_key_id in self.config")
            return

        if "ssh_key_id" in self.vpc_data:
            print("ssh_key_id in self.vpc_data")
            try:
                self.vpc_cli.get_key(self.vpc_data["ssh_key_id"])
                self.config["ssh_key_id"] = self.vpc_data["ssh_key_id"]
                self.config["ssh_key_filename"] = self.vpc_data["ssh_key_filename"]
                return
            except ibm_cloud_sdk_core.ApiException:
                pass

        keyname = f"skyplane-key-{str(uuid.getnode())[-6:]}"
        filename = os.path.join("~", ".ssh", f"{keyname}.id_rsa")
        key_filename = os.path.expanduser(filename)

        key_info = None

        def _get_ssh_key():
            for key in self.vpc_cli.list_keys().result["keys"]:
                if key["name"] == keyname:
                    return key

        print(key_filename)
        if not os.path.isfile(key_filename):
            logger.debug("Generating new ssh key pair")
            os.system(f'ssh-keygen -b 2048 -t rsa -f {key_filename} -q -N ""')
            logger.debug(f"SHH key pair generated: {key_filename}")
        else:
            key_info = _get_ssh_key()

        if not key_info:
            with open(f"{key_filename}.pub", "r") as file:
                ssh_key_data = file.read()
            try:
                key_info = self.vpc_cli.create_key(
                    public_key=ssh_key_data, name=keyname, type="rsa", resource_group={"id": self.config["resource_group_id"]}
                ).get_result()
            except ibm_cloud_sdk_core.ApiException as e:
                logger.warn(e.message)
                if "Key with name already exists" in e.message:
                    self.vpc_cli.delete_key(id=_get_ssh_key()["id"])
                    key_info = self.vpc_cli.create_key(
                        public_key=ssh_key_data,
                        name=keyname,
                        type="rsa",
                        resource_group={"id": self.config["resource_group_id"]},
                    ).get_result()
                else:
                    if "Key with fingerprint already exists" in e.message:
                        logger.error("Can't register an SSH key with the same fingerprint")
                    raise e  # can't continue the configuration process without a valid ssh key

        self.config["ssh_key_id"] = key_info["id"]
        self.config["ssh_key_filename"] = key_filename

    @imports.inject("ibm_cloud_sdk_core", pip_extra="ibmcloud")
    def _create_subnet(ibm_cloud_sdk_core, self):
        """
        Creates a new subnet
        """
        if "subnet_id" in self.config:
            if "subnet_id" in self.vpc_data and self.vpc_data["subnet_id"] == self.config["subnet_id"]:
                self.config["zone_name"] = self.vpc_data["zone_name"]
            else:
                resp = self.vpc_cli.get_subnet(self.config["subnet_id"])
                self.config["zone_name"] = resp.result["zone"]["name"]
            return

        if "subnet_id" in self.vpc_data:
            try:
                self.vpc_cli.get_subnet(self.vpc_data["subnet_id"])
                self.config["subnet_id"] = self.vpc_data["subnet_id"]
                self.config["zone_name"] = self.vpc_data["zone_name"]
                return
            except ibm_cloud_sdk_core.ApiException:
                pass

        subnet_name = f"skyplane-subnet-{self.vpc_key}"
        subnet_data = None

        subnets_info = self.vpc_cli.list_subnets(resource_group_id=self.config["resource_group_id"]).get_result()
        for sn in subnets_info["subnets"]:
            if sn["name"] == subnet_name:
                subnet_data = sn

        if not subnet_data:
            logger.debug(f"Creating Subnet {subnet_name}")
            subnet_prototype = {}
            subnet_prototype["zone"] = {"name": self.region + "-1"}
            subnet_prototype["ip_version"] = "ipv4"
            subnet_prototype["name"] = subnet_name
            subnet_prototype["resource_group"] = {"id": self.config["resource_group_id"]}
            subnet_prototype["vpc"] = {"id": self.config["vpc_id"]}
            subnet_prototype["total_ipv4_address_count"] = 256
            response = self.vpc_cli.create_subnet(subnet_prototype)
            subnet_data = response.result

        self.config["subnet_id"] = subnet_data["id"]
        self.config["zone_name"] = subnet_data["zone"]["name"]

    @imports.inject("ibm_cloud_sdk_core", pip_extra="ibmcloud")
    def _create_gateway(ibm_cloud_sdk_core, self):
        """
        Crates a public gateway.
        Gateway is used by private nodes for accessing internet
        """
        if "gateway_id" in self.config:
            return

        if "gateway_id" in self.vpc_data:
            try:
                self.vpc_cli.get_public_gateway(self.vpc_data["gateway_id"])
                self.config["gateway_id"] = self.vpc_data["gateway_id"]
                return
            except ibm_cloud_sdk_core.ApiException:
                pass

        gateway_name = f"skyplane-gateway-{self.vpc_key}"
        gateway_data = None

        gateways_info = self.vpc_cli.list_public_gateways().get_result()
        for gw in gateways_info["public_gateways"]:
            if gw["vpc"]["id"] == self.config["vpc_id"]:
                gateway_data = gw

        if not gateway_data:
            logger.debug(f"Creating Gateway {gateway_name}")
            gateway_prototype = {}
            gateway_prototype["vpc"] = {"id": self.config["vpc_id"]}
            gateway_prototype["zone"] = {"name": self.config["zone_name"]}
            gateway_prototype["name"] = gateway_name
            gateway_prototype["resource_group"] = {"id": self.config["resource_group_id"]}
            response = self.vpc_cli.create_public_gateway(**gateway_prototype)
            gateway_data = response.result

        self.config["gateway_id"] = gateway_data["id"]

        # Attach public gateway to the subnet
        self.vpc_cli.set_subnet_public_gateway(self.config["subnet_id"], {"id": self.config["gateway_id"]})

    def _create_floating_ip(self, disable_recycle=True):
        """
        Creates a new floating IP address
        """

        floating_ip_data = None
        if not disable_recycle:
            floating_ips_info = self.vpc_cli.list_floating_ips().get_result()
            for fip in floating_ips_info["floating_ips"]:
                if fip["name"].startswith("skyplane") and fip["status"] == "available":
                    floating_ip_data = fip

        if not floating_ip_data:
            floating_ip_name = f"skyplane-{str(uuid.uuid1())[-4:]}-{str(random.randint(1000,9999))}"
            logger.debug(f"Creating floating IP {floating_ip_name}")
            floating_ip_prototype = {}
            floating_ip_prototype["name"] = floating_ip_name
            floating_ip_prototype["zone"] = {"name": self.config["zone_name"]}
            floating_ip_prototype["resource_group"] = {"id": self.config["resource_group_id"]}
            response = self.vpc_cli.create_floating_ip(floating_ip_prototype)
            floating_ip_data = response.result

        floating_ip = floating_ip_data["address"]
        floating_ip_id = floating_ip_data["id"]
        return floating_ip, floating_ip_id

    def add_ips_to_security_group(self, ip_address):
        for ip in ip_address:
            ip_rule = {}
            ip_rule["direction"] = "inbound"
            ip_rule["ip_version"] = "ipv4"
            ip_rule["protocol"] = "all"
            remote = {}
            remote["address"] = ip
            ip_rule["remote"] = remote

            deploy_ip_rule = True
            sg_rules = self.vpc_cli.get_security_group(self.config["security_group_id"])
            for rule in sg_rules.get_result()["rules"]:
                if all(item in rule.items() for item in ip_rule.items()):
                    deploy_ip_rule = False

            if deploy_ip_rule:
                logger.debug(f"About to create ip rule for {ip}")
                try:
                    self.vpc_cli.create_security_group_rule(self.config["security_group_id"], ip_rule)
                    logger.debug(f"Created rule for {ip_rule}")
                except Exception as e:
                    raise e

    def create_vpc_instance(self, public=True):
        """
        Creates the master VM insatnce
        """
        if "image_id" not in self.config and "image_id" in self.vpc_data:
            self.config["image_id"] = self.vpc_data["image_id"]

        if "image_id" not in self.config:
            for image in self.vpc_cli.list_images().result["images"]:
                if "ubuntu-22" in image["name"]:
                    self.config["image_id"] = image["id"]

        name = f"skyplane-ibm-vsi-{self.vpc_key}-{str(uuid.uuid4().hex[:8])}"
        vsi = IBMVPCInstance(name, self.config, self.vpc_cli, public)

        floating_ip, floating_ip_id = self._create_floating_ip()
        if public:
            vsi.public_ip = floating_ip
            vsi.floating_ip_id = floating_ip_id

        vsi.instance_id = None
        vsi.profile_name = self.config["master_profile_name"]
        vsi.delete_on_dismantle = False
        vsi.ssh_credentials.pop("password")

        instance_data = vsi.get_instance_data()
        if instance_data:
            vsi.private_ip = instance_data["primary_network_interface"]["primary_ipv4_address"]
            vsi.instance_id = instance_data["id"]

        instance_id = vsi.create(check_if_exists=True)
        if not vsi.is_ready():
            instance_id = vsi.create(check_if_exists=True)
            vsi.wait_ready()
            self.workers.append(vsi)
            instance_data = vsi.get_instance_data()

        return instance_id, vsi

    def init(self):
        """
        Initialize the VPC
        """
        logger.debug(f"Initializing IBM VPC backend ")

        self._load_vpc_data()

        self.vpc_data = {}

        # Create the VPC if not exists
        self._create_vpc()
        # Create the ssh key pair if not exists
        self._create_ssh_key()
        # Create a new subnaet if not exists
        self._create_subnet()
        # Create a new gateway if not exists
        self._create_gateway()

        self.vpc_data = {
            "instance_id": "0af1",
            "vpc_id": self.config["vpc_id"],
            "subnet_id": self.config["subnet_id"],
            "security_group_id": self.config["security_group_id"],
            "gateway_id": self.config["gateway_id"],
            "zone_name": self.config["zone_name"],
            "ssh_key_id": self.config["ssh_key_id"],
            "ssh_key_filename": self.config["ssh_key_filename"],
        }
        self._dump_vpc_data()

    @imports.inject("ibm_cloud_sdk_core", pip_extra="ibmcloud")
    def _delete_vm_instances(ibm_cloud_sdk_core, self, all=False):
        """
        Deletes all VM instances in the VPC
        """
        msg = f"Deleting all SkyPlane worker VMs in {self.vpc_name}" if self.vpc_name else "Deleting all SkyPlane worker VMs"
        logger.info(msg)

        def delete_instance(instance_info):
            ins_name, ins_id = instance_info
            try:
                logger.info(f"Deleting instance {ins_name}")
                self.vpc_cli.delete_instance(ins_id)
            except ibm_cloud_sdk_core.ApiException as err:
                if err.code == 404:
                    pass
                else:
                    raise err

        vms_prefixes = ("skyplane-ibm") if all else ("skyplane-ibm",)

        def get_instances():
            instances = set()
            instances_info = self.vpc_cli.list_instances().get_result()
            for ins in instances_info["instances"]:
                if ins["name"].startswith(vms_prefixes):
                    instances.add((ins["name"], ins["id"]))
            return instances

        deleted_instances = set()
        while True:
            instances_to_delete = set()
            for ins_to_delete in get_instances():
                if ins_to_delete not in deleted_instances:
                    instances_to_delete.add(ins_to_delete)

            if instances_to_delete:
                with ThreadPoolExecutor(len(instances_to_delete)) as executor:
                    executor.map(delete_instance, instances_to_delete)
                deleted_instances.update(instances_to_delete)
            else:
                break

        # Wait until all instances are deleted
        while get_instances():
            time.sleep(1)

    @imports.inject("ibm_cloud_sdk_core", pip_extra="ibmcloud")
    def _delete_subnet(ibm_cloud_sdk_core, self):
        """
        Deletes all VM instances in the VPC
        """
        subnet_name = f"skyplane-subnet-{self.vpc_key}"
        if "subnet_id" not in self.vpc_data:
            subnets_info = self.vpc_cli.list_subnets().get_result()

            for subn in subnets_info["subnets"]:
                if subn["name"] == subnet_name:
                    self.vpc_data["subnet_id"] = subn["id"]

        if "subnet_id" in self.vpc_data:
            logger.info(f"Deleting subnet {subnet_name}")

            try:
                self.vpc_cli.unset_subnet_public_gateway(self.vpc_data["subnet_id"])
            except ibm_cloud_sdk_core.ApiException as err:
                if err.code == 404 or err.code == 400:
                    logger.debug(err)
                else:
                    raise err

            try:
                self.vpc_cli.delete_subnet(self.vpc_data["subnet_id"])
            except ibm_cloud_sdk_core.ApiException as err:
                if err.code == 404 or err.code == 400:
                    logger.debug(err)
                else:
                    raise err

    @imports.inject("ibm_cloud_sdk_core", pip_extra="ibmcloud")
    def _delete_gateway(ibm_cloud_sdk_core, self):
        """
        Deletes the public gateway
        """
        gateway_name = f"skyplane-gateway-{self.vpc_key}"
        if "gateway_id" not in self.vpc_data:
            gateways_info = self.vpc_cli.list_public_gateways().get_result()

            for gw in gateways_info["public_gateways"]:
                if ["name"] == gateway_name:
                    self.vpc_data["gateway_id"] = gw["id"]

        if "gateway_id" in self.vpc_data:
            logger.info(f"Deleting gateway {gateway_name}")
            try:
                self.vpc_cli.delete_public_gateway(self.vpc_data["gateway_id"])
            except ibm_cloud_sdk_core.ApiException as err:
                if err.code == 404 or err.code == 400:
                    logger.debug(err)
                else:
                    raise err

    @imports.inject("ibm_cloud_sdk_core", pip_extra="ibmcloud")
    def _delete_ssh_key(ibm_cloud_sdk_core, self):
        """
        Deletes the ssh key
        """
        keyname = f"skyplane-key-{str(uuid.getnode())[-6:]}"
        filename = os.path.join("~", ".ssh", f"{keyname}.id_rsa")
        key_filename = os.path.expanduser(filename)

        if os.path.isfile(key_filename):
            os.remove(key_filename)
        if os.path.isfile(f"{key_filename}.pub"):
            os.remove(f"{key_filename}.pub")

        if "ssh_key_id" not in self.vpc_data:
            for key in self.vpc_cli.list_keys().result["keys"]:
                if key["name"] == keyname:
                    self.vpc_data["ssh_key_id"] = key["id"]

        if "ssh_key_id" in self.vpc_data:
            logger.info(f"Deleting SSH key {keyname}")
            try:
                self.vpc_cli.delete_key(id=self.vpc_data["ssh_key_id"])
            except ibm_cloud_sdk_core.ApiException as err:
                if err.code == 404 or err.code == 400:
                    logger.debug(err)
                else:
                    raise err

    @imports.inject("ibm_cloud_sdk_core", pip_extra="ibmcloud")
    def _delete_vpc(ibm_cloud_sdk_core, self):
        """
        Deletes the VPC
        """
        if "vpc_id" not in self.vpc_data:
            vpcs_info = self.vpc_cli.list_vpcs().get_result()
            for vpc in vpcs_info["vpcs"]:
                if vpc["name"] == self.vpc_name:
                    self.vpc_data["vpc_id"] = vpc["id"]

        if "vpc_id" in self.vpc_data:
            logger.info(f'Deleting VPC {self.vpc_data["vpc_id"]}')
            try:
                self.vpc_cli.delete_vpc(self.vpc_data["vpc_id"])
            except ibm_cloud_sdk_core.ApiException as err:
                if err.code == 404 or err.code == 400:
                    logger.debug(err)
                else:
                    raise err

    def clean(self, all=False):
        """
        Clean all the backend resources
        The gateway public IP and the floating IP are never deleted
        """
        logger.debug("Cleaning IBM VPC resources")

        self._delete_vm_instances(all=all)

        if all:
            self._load_vpc_data()
            self._delete_subnet()
            self._delete_gateway()
            self._delete_ssh_key()
            self._delete_vpc()

            delete_yaml_config(self.vpc_data_filename)

    def clear(self, job_keys=None):
        """
        Delete all the workers
        """
        # clear() is automatically called after get_result()
        self.dismantle(include_master=False)

    def dismantle(self, include_master=True):
        """
        Stop all worker VM instances
        """
        if len(self.workers) > 0:
            with ThreadPoolExecutor(min(len(self.workers), 48)) as ex:
                ex.map(lambda worker: worker.stop(), self.workers)
            self.workers = []

    def get_instance(self, name, **kwargs):
        """
        Returns a VM class instance.
        Does not creates nor starts a VM instance
        """
        instance = IBMVPCInstance(name, self.config, self.vpc_cli)

        for key in kwargs:
            if hasattr(instance, key):
                setattr(instance, key, kwargs[key])

        return instance

    def get_node_status(self, node_id):
        """returns True if a node is either not recorded or not in any valid status."""
        try:
            node = self.vpc_cli.get_instance(node_id).get_result()
            return node["status"]
        except Exception as e:
            raise e


class IBMVPCInstance:
    def __init__(self, name, ibm_vpc_config, ibm_vpc_client=None, public=False):
        """
        Initialize a IBMVPCInstance instance
        VMs can have master role, this means they will have a public IP address
        """
        self.name = name.lower()
        self.config = ibm_vpc_config

        self.delete_on_dismantle = self.config["delete_on_dismantle"]
        self.profile_name = self.config["worker_profile_name"]

        self.vpc_cli = None
        self.vpc_cli = ibm_vpc_client or self._create_vpc_client()
        self.public = public

        self.ssh_client = None
        self.instance_id = None
        self.instance_data = None
        self.private_ip = None
        self.public_ip = None
        self.floating_ip_id = None
        self.home_dir = "/root"

        self.ssh_credentials = {
            "username": self.config["ssh_username"],
            "password": self.config["ssh_password"],
            "key_filename": self.config.get("ssh_key_filename", "~/.ssh/id_rsa"),
        }
        self.validated = False

    def __str__(self):
        ip = self.public_ip if self.public else self.private_ip

        if ip is None or ip == "0.0.0.0":
            return f"VM instance {self.name}"
        else:
            return f"VM instance {self.name} ({ip})"

    @imports.inject("ibm_vpc", "ibm_cloud_sdk_core", pip_extra="ibmcloud")
    def _create_vpc_client(ibm_vpc, ibm_cloud_sdk_core, self):
        """
        Creates an IBM VPC python-sdk instance
        """
        authenticator = ibm_cloud_sdk_core.authenticators.IAMAuthenticator(
            self.config.get("iam_api_key"), url=self.config.get("iam_endpoint")
        )
        ibm_vpc_client = ibm_vpc.VpcV1(VPC_API_VERSION, authenticator=authenticator)
        ibm_vpc_client.set_service_url(self.config["endpoint"] + "/v1")

        # decorate instance public methods with except/retry logic
        decorate_instance(self.vpc_cli, vpc_retry_on_except)

        return ibm_vpc_client

    def get_ssh_client(self):
        """
        Creates an ssh client against the VM only if the Instance is the master
        """

        if not self.validated and self.public and self.instance_id:
            # validate that private ssh key in ssh_credentials is a pair of public key on instance
            key_filename = self.ssh_credentials["key_filename"]
            key_filename = os.path.abspath(os.path.expanduser(key_filename))

            if not os.path.exists(key_filename):
                raise Exception(f"Private key file {key_filename} doesn't exist")

            initialization_data = self.vpc_cli.get_instance_initialization(self.instance_id).get_result()

            private_res = paramiko.RSAKey(filename=key_filename).get_base64()
            names = []
            for k in initialization_data["keys"]:
                public_res = self.vpc_cli.get_key(k["id"]).get_result()["public_key"].split(" ")[1]
                if public_res == private_res:
                    self.validated = True
                    break
                else:
                    names.append(k["name"])

            if not self.validated:
                raise Exception(f"No public key from keys: {names} on master {self} not a pair for private ssh key {key_filename}")

        if self.private_ip or self.public_ip:
            if not self.ssh_client:
                self.ssh_client = SSHClient(self.public_ip or self.private_ip, self.ssh_credentials)

        return self.ssh_client

    def del_ssh_client(self):
        """
        Deletes the ssh client
        """
        if self.ssh_client:
            try:
                self.ssh_client.close()
            except Exception:
                pass
            self.ssh_client = None

    def is_ready(self):
        """
        Checks if the VM instance is ready to receive ssh connections
        """
        login_type = "password" if "password" in self.ssh_credentials and not self.public else "publickey"
        try:
            self.get_ssh_client().run_remote_command("id")
        except Exception as err:
            logger.debug(f"SSH to {self.public_ip if self.public else self.private_ip} failed ({login_type}): {err}")
            self.del_ssh_client()
            return False
        return True

    def wait_ready(self, timeout=INSTANCE_START_TIMEOUT):
        """
        Waits until the VM instance is ready to receive ssh connections
        """
        logger.debug(f"Waiting {self} to become ready")

        start = time.time()

        if self.public:
            self.get_public_ip()
        else:
            self.get_private_ip()

        while time.time() - start < timeout:
            if self.is_ready():
                start_time = round(time.time() - start, 2)
                logger.debug(f"{self} ready in {start_time} seconds")
                return True
            time.sleep(5)

        raise TimeoutError(f"Readiness probe expired on {self}")

    @imports.inject("ibm_cloud_sdk_core", pip_extra="ibmcloud")
    def _create_instance(ibm_cloud_sdk_core, self, user_data):
        """
        Creates a new VM instance
        """
        logger.debug("Creating new VM instance {}".format(self.name))

        security_group_identity_model = {"id": self.config["security_group_id"]}
        subnet_identity_model = {"id": self.config["subnet_id"]}
        primary_network_interface = {"name": "eth0", "subnet": subnet_identity_model, "security_groups": [security_group_identity_model]}

        boot_volume_data = {
            "capacity": self.config["boot_volume_capacity"],
            "name": f"{self.name}-{str(uuid.uuid4())[:4]}-boot",
            "profile": {"name": self.config["boot_volume_profile"]},
        }

        boot_volume_attachment = {"delete_volume_on_instance_delete": True, "volume": boot_volume_data}

        key_identity_model = {"id": self.config["ssh_key_id"]}

        instance_prototype = {}
        instance_prototype["name"] = self.name
        instance_prototype["keys"] = [key_identity_model]
        instance_prototype["profile"] = {"name": self.profile_name}
        instance_prototype["resource_group"] = {"id": self.config["resource_group_id"]}
        instance_prototype["vpc"] = {"id": self.config["vpc_id"]}
        instance_prototype["image"] = {"id": self.config["image_id"]}
        instance_prototype["zone"] = {"name": self.config["zone_name"]}
        instance_prototype["boot_volume_attachment"] = boot_volume_attachment
        instance_prototype["primary_network_interface"] = primary_network_interface

        if user_data:
            instance_prototype["user_data"] = user_data

        try:
            resp = self.vpc_cli.create_instance(instance_prototype)
        except ibm_cloud_sdk_core.ApiException as err:
            if err.code == 400 and "already exists" in err.message:
                return self.get_instance_data()
            elif err.code == 400 and "over quota" in err.message:
                logger.debug(f"Create VM instance {self.name} failed due to quota limit")
            else:
                logger.debug("Create VM instance {} failed with status code {}: {}".format(self.name, str(err.code), err.message))
            raise err

        logger.debug("VM instance {} created successfully ".format(self.name))

        return resp.result

    def _attach_floating_ip(self, fip, fip_id, instance):
        """
        Attach a floating IP address to VM
        """

        # logger.debug('Attaching floating IP {} to VM instance {}'.format(fip, instance['id']))

        # we need to check if floating ip is not attached already. if not, attach it to instance
        instance_primary_ni = instance["primary_network_interface"]

        if instance_primary_ni["primary_ipv4_address"] and instance_primary_ni["id"] == fip_id:
            # floating ip already atteched. do nothing
            logger.debug("Floating IP {} already attached to eth0".format(fip))
        else:
            self.vpc_cli.add_instance_network_interface_floating_ip(instance["id"], instance["network_interfaces"][0]["id"], fip_id)

    def _delete_floating_ip(self, fip_id):
        response = self.vpc_cli.delete_floating_ip(id=fip_id)

    def get_instance_data(self):
        """
        Returns the instance information
        """
        # if self.instance_id:
        #    self.instance_data = self.vpc_cli.get_instance(self.instance_id).get_result()

        instances_data = self.vpc_cli.list_instances(name=self.name).get_result()
        if len(instances_data["instances"]) > 0:
            self.instance_data = instances_data["instances"][0]

        return self.instance_data

    def get_instance_id(self):
        """
        Returns the instance ID
        """
        if not self.instance_id:
            instance_data = self.get_instance_data()
            if instance_data:
                self.instance_id = instance_data["id"]
            else:
                logger.debug(f"VM instance {self.name} does not exists")
        return self.instance_id

    def get_private_ip(self):
        """
        Requests the private IP address
        """
        while not self.private_ip or self.private_ip == "0.0.0.0":
            time.sleep(1)
            instance_data = self.get_instance_data()
            self.private_ip = instance_data["primary_network_interface"]["primary_ipv4_address"]

        return self.private_ip

    def get_public_ip(self):
        """
        Requests the public IP address
        """
        return self.public_ip

    def create(self, check_if_exists=False, user_data=None):
        """
        Creates a new VM instance
        """
        instance = None
        vsi_exists = True if self.instance_id else False

        if check_if_exists and not vsi_exists:
            logger.debug("Checking if VM instance {} already exists".format(self.name))
            instances_data = self.get_instance_data()
            if instances_data:
                logger.debug("VM instance {} already exists".format(self.name))
                vsi_exists = True
                self.instance_id = instances_data["id"]

        if not vsi_exists:
            instance = self._create_instance(user_data=user_data)
            self.instance_id = instance["id"]
        else:
            self.start()

        if self.public and instance:
            self._attach_floating_ip(self.public_ip, self.floating_ip_id, instance)

        return self.instance_id

    @imports.inject("ibm_cloud_sdk_core", pip_extra="ibmcloud")
    def start(ibm_cloud_sdk_core, self):
        """
        Starts the VM instance
        """
        logger.debug("Starting VM instance {}".format(self.name))

        try:
            self.vpc_cli.create_instance_action(self.instance_id, "start")
        except ibm_cloud_sdk_core.ApiException as err:
            if err.code == 404:
                pass
            else:
                raise err

        logger.debug("VM instance {} started successfully".format(self.name))

    @imports.inject("ibm_cloud_sdk_core", pip_extra="ibmcloud")
    def _delete_instance(ibm_cloud_sdk_core, self):
        """
        Deletes the VM instacne and the associated volume
        """
        logger.debug("Deleting VM instance {}".format(self.name))
        try:
            self.vpc_cli.delete_instance(self.instance_id)
        except ibm_cloud_sdk_core.ApiException as err:
            if err.code == 404:
                pass
            else:
                raise err
        self.instance_id = None
        self.private_ip = None
        self.del_ssh_client()

    @imports.inject("ibm_cloud_sdk_core", pip_extra="ibmcloud")
    def _stop_instance(ibm_cloud_sdk_core, self):
        """
        Stops the VM instance
        """
        logger.debug("Stopping VM instance {}".format(self.name))
        try:
            self.vpc_cli.create_instance_action(self.instance_id, "stop")
        except ibm_cloud_sdk_core.ApiException as err:
            if err.code == 404:
                pass
            else:
                raise err

    def stop(self):
        """
        Stops the VM instance
        """
        if self.delete_on_dismantle:
            self._delete_instance()
        else:
            self._stop_instance()

    def delete(self):
        """
        Deletes the VM instance
        """
        self._delete_instance()
        self._delete_floating_ip(self.floating_ip_id)

    def validate_capabilities(self):
        """
        Validate hardware/os requirments specified in backend config
        """
        if self.config.get("singlesocket"):
            cmd = "lscpu -p=socket|grep -v '#'"
            res = self.get_ssh_client().run_remote_command(cmd)
            sockets = set()
            for char in res:
                if char != "\n":
                    sockets.add(char)
            if len(sockets) != 1:
                raise Exception(f"Not using single CPU socket as specified, using {len(sockets)} sockets instead")


RETRIABLE = [
    "list_vpcs",
    "create_vpc",
    "get_security_group",
    "create_security_group_rule",
    "list_public_gateways",
    "create_public_gateway",
    "list_subnets",
    "create_subnet",
    "set_subnet_public_gateway",
    "list_floating_ips",
    "create_floating_ip",
    "get_instance",
    "delete_instance",
    "list_instances",
    "list_instance_network_interface_floating_ips",
    "delete_floating_ip",
    "delete_subnet",
    "delete_public_gateway",
    "delete_vpc",
    "get_instance_initialization",
    "get_key",
    "create_instance",
    "add_instance_network_interface_floating_ip",
    "get_instance",
    "create_instance_action",
    "delete_instance",
]


def decorate_instance(instance, decorator):
    for name, func in inspect.getmembers(instance, inspect.ismethod):
        if name in RETRIABLE:
            setattr(instance, name, decorator(func))
    return instance


@imports.inject("ibm_cloud_sdk_core", pip_extra="ibmcloud")
def vpc_retry_on_except(ibm_cloud_sdk_core, func):
    RETRIES = 3
    SLEEP_FACTOR = 1.5
    MAX_SLEEP = 30

    IGNORED_404_METHODS = ["delete_instance", "delete_public_gateway", "delete_vpc", "create_instance_action"]

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        sleep_time = 1

        def _sleep_or_raise(sleep_time, err):
            if i < RETRIES - 1:
                time.sleep(sleep_time)
                logger.warning((f"Got exception {err}, retrying for the {i} time, left retries {RETRIES - 1 -i}"))
                return min(sleep_time * SLEEP_FACTOR, MAX_SLEEP)
            else:
                raise err

        for i in range(RETRIES):
            try:
                return func(*args, **kwargs)
            except ibm_cloud_sdk_core.ApiException as err:
                if func.__name__ in IGNORED_404_METHODS and err.code == 404:
                    logger.debug((f"Got exception {err} when trying to invoke {func.__name__}, ignoring"))
                else:
                    sleep_time = _sleep_or_raise(sleep_time, err)
            except Exception as err:
                sleep_time = _sleep_or_raise(sleep_time, err)

    return wrapper
