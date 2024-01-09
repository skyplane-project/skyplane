# (C) Copyright Samsung SDS. 2023

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
import os
from typing import List, Optional
import uuid
from pathlib import Path

from skyplane.compute.key_utils import generate_keypair

from skyplane import exceptions
from skyplane.compute.scp.scp_auth import SCPAuthentication
from skyplane.compute.scp.scp_server import SCPServer
from skyplane.compute.scp.scp_network import SCPNetwork
from skyplane.compute.cloud_provider import CloudProvider
from skyplane.compute.server import key_root
from skyplane.utils import logger
from skyplane.utils.fn import do_parallel


class SCPCloudProvider(CloudProvider):
    def __init__(
        self,
        key_prefix: str = "skyplane",
        key_root=key_root / "scp",
        auth: Optional[SCPAuthentication] = None,
        network: Optional[SCPNetwork] = None,
    ):
        super().__init__()
        self.key_name = key_prefix
        self.auth = auth if auth else SCPAuthentication()
        self.network = network if network else SCPNetwork(self.auth)
        key_root.mkdir(parents=True, exist_ok=True)
        self.private_key_path = key_root / "scp_key"
        self.public_key_path = key_root / "scp_key.pub"

    @property
    def name(self):
        return "scp"

    @staticmethod
    def region_list() -> List[str]:
        return SCPAuthentication.get_region_config()

    @classmethod
    def get_transfer_cost(cls, src_key, dst_key, premium_tier=True):
        assert src_key.startswith("scp:")
        dst_provider, dst_region = dst_key.split(":")

        return 0.077

    def create_ssh_key(self):
        public_key_path = Path(self.public_key_path)
        private_key_path = Path(self.private_key_path)
        if not private_key_path.exists():
            private_key_path.parent.mkdir(parents=True, exist_ok=True)
            generate_keypair(public_key_path, private_key_path)

    def get_init_script(self):
        cmd_st = "mkdir -p ~/.ssh/; touch ~/.ssh/authorized_keys;"
        with open(os.path.expanduser(self.public_key_path)) as f:
            pub_key = f.read()
        cmd = "echo '{}' &>>~/.ssh/authorized_keys;".format(pub_key)
        cmd_ed = "chmod 644 ~/.ssh/authorized_keys; chmod 700 ~/.ssh/; "
        cmd_ed += "mkdir -p ~/.scp; "

        return cmd_st + cmd + cmd_ed

    def get_instance_list(self, region) -> List[SCPServer]:
        try:
            service_zone_id = self.network.get_service_zone_id(region)
            servergroups = self.network.list_server_group()
            servergroups = [sg for sg in servergroups if sg["serviceZoneId"] == service_zone_id]

            instances = []
            for sg in servergroups:
                instances += self.network.list_server_group_instance(sg["serverGroupId"])

            instancelist = []
            validstate = ["RUNNING", "STOPPED", "STOPPING", "STARTING"]
            for instance in instances:
                if instance["virtualServerState"] in validstate:
                    instancelist.append(
                        {"virtualServerId": instance["virtualServerId"], "virtualServerName": instance["virtualServerName"]}
                    )

            return [
                SCPServer(f"scp:{region}", virtualServerName=i["virtualServerName"], virtualServerId=i["virtualServerId"])
                for i in instancelist
            ]
        except Exception as e:
            raise exceptions.SkyplaneException(f"Failed to get instance list: {e}")

    def setup_global(self, **kwargs):
        pass

    def setup_region(self, region: str):
        self.network.make_vpc(region)

    def provision_instance(
        self,
        region: str,
        instance_class: str,
        disk_size: int = 100,
        use_spot_instances: bool = False,
        name: Optional[str] = None,
        tags={"skyplane": "true"},
        instance_os: str = "ubuntu",
    ) -> SCPServer:
        assert not region.startswith("scp:"), "Region should be SCP region"
        if name is None:
            name = f"skyplane-scp-{uuid.uuid4().hex[:8]}"
        url = f"/virtual-server/v3/virtual-servers"

        service_zone_id = self.network.get_service_zone_id(region)

        vpc_name = self.network.define_vpc_name(region)
        vpc_id = self.network.list_vpcs(service_zone_id, vpc_name)[0]["vpcId"]
        init_script = self.get_init_script()
        subnet_id = self.network.list_subnets(vpc_id, None)[0]["subnetId"]
        security_group_id = self.network.list_security_groups(vpc_id, None)[0]["securityGroupId"]
        servergroup = tags.get("servergroup", "default")
        image_id = "IMAGE-mX_5UKOJqriGnWZ2GXEbEg"  # Ubuntu 22.04

        req_body = {
            "availabilityZoneName": "AZ1" if region == "KOREA-WEST-MAZ-SCP-B001" else None,
            "blockStorage": {
                "blockStorageName": "Skyplane-Block-Storage",
                "diskSize": disk_size,
            },
            "imageId": image_id,
            "initialScript": {
                "encodingType": "plain",
                "initialScriptShell": "bash",
                "initialScriptType": "text",
                "initialScriptContent": init_script,
            },
            "nic": {
                "natEnabled": "true",
                # "publicIpId" : "PUBLIC_IP-xxxxxx",
                "subnetId": subnet_id,
            },
            "osAdmin": {"osUserId": "root", "osUserPassword": "test123$"},
            "securityGroupIds": [security_group_id],
            "serverGroupId": servergroup,
            "serverType": instance_class,
            "serviceZoneId": service_zone_id,
            "tags": [
                {"tagKey": "skyplane", "tagValue": "true"},
                {"tagKey": "skyplaneclientid", "tagValue": tags.get("skyplaneclientid", "unknown")},
            ],
            "virtualServerName": name,
        }
        try:
            # self.network.scp_client.random_wait()
            response = self.network.scp_client._post(url, req_body)
            # print(response)
            virtualServerId = response["resourceId"]

            # wait for server editing
            create_completion_condition = lambda: self.network.get_vs_details(virtualServerId)["virtualServerState"] == "EDITING"
            self.network.scp_client.wait_for_completion(
                f"[{region}:{name}:{instance_class}] Creating SCP Skyplane Virtual Server", create_completion_condition
            )

            # wait for virtual server ip
            create_completion_condition = lambda: self.network.get_vs_details(virtualServerId).get("ip") is not None
            self.network.scp_client.wait_for_completion(
                f"[{region}:{name}:{instance_class}] Creating SCP Skyplane Virtual Server ip", create_completion_condition
            )

            # Add firewall rule
            self.network.add_firewall_22_rule(region, servergroup, virtualServerId)

            # wait for server running
            create_completion_condition = lambda: self.network.get_vs_details(virtualServerId)["virtualServerState"] == "RUNNING"
            self.network.scp_client.wait_for_completion(
                f"[{region}:{name}:{instance_class}] Running SCP Skyplane Virtual Server", create_completion_condition
            )

            server = SCPServer(f"scp:{region}", virtualServerName=name, virtualServerId=virtualServerId, vpc_id=vpc_id)

        except KeyboardInterrupt:
            logger.warning(f"keyboard interrupt. You may need to use the deprovision command to terminate instance {name}.")
            # wait for server running
            create_completion_condition = lambda: self.network.get_vs_details(virtualServerId)["virtualServerState"] == "RUNNING"
            self.network.scp_client.wait_for_completion(
                f"[{region}:{name}:{instance_class}] Running SCP Skyplane Virtual Server", create_completion_condition
            )
            server = SCPServer(f"scp:{region}", virtualServerName=name, virtualServerId=virtualServerId, vpc_id=vpc_id)
            server.terminate_instance_impl()
            raise
        except Exception as e:
            # Not found any cluster by requested Scale
            if "404" in str(e):
                raise Exception(
                    f"Failed to provision the instance : due to insufficient cluster capacity compared to the requested scale, {e}. Check the SCP console for more details."
                )
            else:
                raise Exception(f"Exception occurred during provision instance: {e}. Check the SCP console for more details.")
        return server

    def add_firewall_rule_all(self, region, private_ips, vpcids):
        self.network.add_firewall_rule_all(region, private_ips, vpcids)

    def remove_gateway_rule(self, server: SCPServer):
        ip = server.private_ip()
        igw = self.network.get_igw(server.vpc_id)
        firewall = self.network.get_firewallId(igw)

        ruleIds = self.network.list_firewall_rules(firewall)

        ruleDetails = [self.network.get_firewall_rule_details(firewall, id) for id in ruleIds]
        # for d in ruleDetails: print(d)
        ruleDetails = [detail for detail in ruleDetails if "ruleId" in detail]  # HACK, some times wrong responses

        rule_source = [detail["ruleId"] for detail in ruleDetails if ip in detail["sourceIpAddresses"]]
        rule_dest = [detail["ruleId"] for detail in ruleDetails if ip in detail["destinationIpAddresses"]]
        rule_ids = list(set(rule_source + rule_dest))
        print(f"remove_gateway_rule : rule_ids - {rule_ids}")
        self.network.delete_firewall_rules(server.region(), firewall, rule_ids)

    def remove_gateway_rule_region(self, region, private_ips, vpcids):
        igw = self.network.get_igw(vpcids[0])
        firewallId = self.network.get_firewallId(igw)

        ruleIds = self.network.list_firewall_rules(firewallId)

        # For each private IP, check if it appears in the source or destination IPs of ruleIds' details.
        ruleDetails = [self.network.get_firewall_rule_details(firewallId, id) for id in ruleIds]

        rule_ids = list(
            set(
                detail["ruleId"]
                for ip in private_ips
                for detail in ruleDetails
                if ip in detail["sourceIpAddresses"] or ip in detail["destinationIpAddresses"]
            )
        )

        self.network.delete_firewall_rules(region, firewallId, rule_ids)

    def remove_gateway_rule_all(self, instances):
        # Get all regions
        regions = list(set([instance.region() for instance in instances]))

        # Get all private IPs and VPC IDs for each region, then remove gateway rules for each region
        for region in regions:
            private_ips = [instance.private_ip() for instance in instances if instance.region() == region]
            vpcids = [instance.vpc_id for instance in instances if instance.region() == region]
            self.remove_gateway_rule_region(region, private_ips, vpcids)

    def teardown_global(self):
        args = []
        for region in self.region_list():
            service_zone_id = self.network.get_service_zone_id(region)
            vpc_name = self.network.define_vpc_name(region)
            skyplane_vpc = self.network.find_vpc_id(service_zone_id, vpc_name)
            if skyplane_vpc:
                args.append((region, skyplane_vpc))

        if len(args) > 0:
            do_parallel(lambda x: self.network.delete_vpc(*x), args, desc="Removing VPCs", spinner=True, spinner_persist=True)
