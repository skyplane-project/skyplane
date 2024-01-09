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
import time
from typing import List, Optional
import uuid

from skyplane.compute.scp.scp_auth import SCPAuthentication
from skyplane.compute.scp.scp_utils import SCPClient
from skyplane.utils import logger


class SCPNetwork:
    def __init__(self, auth: SCPAuthentication, vpc_name="skyplane", sg_name="skyplane"):
        self.auth = auth
        self.scp_client = SCPClient()

    def get_service_zone_id(self, region: str) -> str:
        zones = SCPAuthentication.get_zones()
        for zone in zones:
            if zone["serviceZoneName"] == region:
                return zone["serviceZoneId"]
        raise ValueError(f"Region {region} not found in SCP")

    def get_service_zoneName(self, zoneId: str) -> str:
        zones = SCPAuthentication.get_zones()
        for zone in zones:
            if zone["serviceZoneId"] == zoneId:
                return zone["serviceZoneName"]
        raise ValueError(f"ZoneId {zoneId} not found in SCP")

    def get_vpc_info(self, vpcId):
        url = f"/vpc/v2/vpcs?vpcId={vpcId}"
        response = self.scp_client._get(url)
        return response[0]["serviceZoneId"], response[0]["vpcName"]

    def list_vpcs(self, service_zone_id, vpc_name):
        url = f"/vpc/v2/vpcs?serviceZoneId={service_zone_id}"

        response = self.scp_client._get(url)
        vpcs = [result for result in response if result["vpcName"] == vpc_name]

        if len(vpcs) == 0:
            return None
        else:
            return vpcs

    def get_vpc_detail(self, vpcId):
        url = f"/vpc/v2/vpcs/{vpcId}"
        response = self.scp_client._getDetail(url)
        return response

    def delete_vpc(self, region, vpcId):
        service_zone_id, vpc_name = self.get_vpc_info(vpcId)
        # delete subnets
        subnets = self.list_subnets(vpcId, subnetId=None)
        for subnet in subnets:
            self.delete_subnet(region, subnet["subnetId"], vpcId)

        # delete igw - firewall first
        igws = self.list_igws(vpcId)
        for igw in igws:
            firewallId = self.get_firewallId(igw["internetGatewayId"])
            self.delete_firewall_rules(region, firewallId, None)
            self.delete_igw(region, igw["internetGatewayId"], vpcId)

        # delete security groups
        sgs = self.list_security_groups(vpcId, securityGroupName=None)
        for sg in sgs:
            self.delete_security_group(region, sg["securityGroupId"], sg["securityGroupName"])

        # delete vpc
        url = f"/vpc/v2/vpcs/{vpcId}"
        try:
            self.scp_client._delete(url)
            delete_completion_condition = lambda: not self.list_vpcs(service_zone_id, vpc_name)
            self.scp_client.wait_for_completion(f"[{region}] Deleting SCP Skyplane VPC", delete_completion_condition)

        except Exception as e:
            logger.error(e)
            raise e
        # return response

    def create_vpc(self, region, service_zone_id, vpc_name):
        url = "/vpc/v3/vpcs"
        req_body = {"serviceZoneId": service_zone_id, "vpcName": vpc_name, "vpcDescription": "Skyplane VPC"}
        vpcId = None
        try:
            response = self.scp_client._post(url, req_body)
            vpcId = response["resourceId"]
            create_completion_condition = lambda: self.get_vpc_detail(vpcId)["vpcState"] == "ACTIVE"
            self.scp_client.wait_for_completion(f"[{region}] Creating SCP Skyplane VPC", create_completion_condition)
        except Exception as e:
            logger.error(e)
            raise e
        return vpcId

    def list_igws(self, vpcId: str):
        url = f"/internet-gateway/v2/internet-gateways"
        response = self.scp_client._get(url)
        igws = [result for result in response if result["vpcId"] == vpcId]
        return igws

    def delete_igw(self, region, igwId: str, vpcId: str):
        url = f"/internet-gateway/v2/internet-gateways/{igwId}"
        try:
            self.scp_client._delete(url)
            delete_completion_condition = lambda: not self.list_igws(vpcId)
            self.scp_client.wait_for_completion(f"[{region}] Deleting SCP Skyplane IGW", delete_completion_condition)
        except Exception as e:
            logger.error(e)
            raise e

    def create_igw(self, region, service_zone_id, vpcId: str):
        url = "/internet-gateway/v2/internet-gateways"
        req_body = {
            "firewallEnabled": True,
            "serviceZoneId": service_zone_id,
            "vpcId": vpcId,
            "InternetGatewayDescription": "Default Internet Gateway for Skyplane VPC",
        }
        try:
            response = self.scp_client._post(url, req_body)
            igwId = response["resourceId"]
            return igwId
        except Exception as e:
            logger.error(e)
            raise e

    def check_created_igw(self, region, igwId: str, vpcId: str):
        create_completion_condition = lambda: self.list_igws(vpcId) and self.list_igws(vpcId)[0].get("internetGatewayState") == "ATTACHED"
        self.scp_client.wait_for_completion(f"[{region}] Creating SCP Skyplane Internet Gateway", create_completion_condition)

    def list_subnets(self, vpcId: str, subnetId: Optional[str] = None):
        if subnetId is None:
            url = f"/subnet/v2/subnets?vpcId={vpcId}"
        else:
            url = f"/subnet/v2/subnets?subnetId={subnetId}"

        response = self.scp_client._get(url)
        # subnet = [result for result in response if result['vpcId'] == vpcId]
        return response

    def create_subnet(self, region, vpc_name, vpcId: str):
        url = "/subnet/v2/subnets"
        req_body = {
            "subnetCidrBlock": "192.168.0.0/24",
            "subnetName": vpc_name + "Sub",
            "subnetType": "PUBLIC",
            "vpcId": vpcId,
            "subnetDescription": "Default Subnet for Skyplane VPC",
        }
        try:
            response = self.scp_client._post(url, req_body)
            subnetId = response["resourceId"]
            return subnetId

        except Exception as e:
            logger.error(e)
            raise e

    def check_created_subnet(self, region, subnetId: str, vpcId: str):
        create_completion_condition = lambda: self.list_subnets(vpcId, subnetId)[0]["subnetState"] == "ACTIVE"
        self.scp_client.wait_for_completion(f"[{region}] Creating SCP Skyplane subnet", create_completion_condition)

    def delete_subnet(self, region, subnetId: str, vpcId: str):
        url = f"/subnet/v2/subnets/{subnetId}"
        try:
            response = self.scp_client._delete(url)
            delete_completion_condition = lambda: not self.list_subnets(vpcId, subnetId)
            self.scp_client.wait_for_completion(f"[{region}] Deleting SCP Skyplane subnet", delete_completion_condition)
        except Exception as e:
            logger.error(e)
            raise e
        return response

    def delete_security_group(self, region, securityGroupId: str, securityGroupName: str):
        url = f"/security-group/v2/security-groups/{securityGroupId}"
        try:
            self.scp_client._delete(url)
            delete_completion_condition = lambda: not self.list_security_groups(None, securityGroupName)
            self.scp_client.wait_for_completion(f"[{region}] Deleting SCP Skyplane Security Group", delete_completion_condition)
        except Exception as e:
            logger.error(e)
            raise e

    def list_security_groups(self, vpcId, securityGroupName: Optional[str] = None):
        if securityGroupName is None:
            url = f"/security-group/v2/security-groups?vpcId={vpcId}"
        else:
            url = f"/security-group/v2/security-groups?securityGroupName={securityGroupName}"
        response = self.scp_client._get(url)
        return response

    def create_security_group(self, region, service_zone_id, vpcId: str):
        url = "/security-group/v3/security-groups"
        req_body = {
            "loggable": False,
            "securityGroupName": "SkyplaneSecuGroup",
            "serviceZoneId": service_zone_id,
            "vpcId": vpcId,
            "securityGroupDescription": "Default Security Group for Skyplane VPC",
        }
        try:
            response = self.scp_client._post(url, req_body)
            securityGroupId = response["resourceId"]
            create_completion_condition = (
                lambda: self.list_security_groups(vpcId, req_body["securityGroupName"])[0]["securityGroupState"] == "ACTIVE"
            )
            self.scp_client.wait_for_completion(f"[{region}] Creating SCP Skyplane Security Group", create_completion_condition)
            return securityGroupId

        except Exception as e:
            logger.error(e)
            raise e

    def find_vpc_id(self, service_zone_id, vpc_name):
        vpc_content = self.list_vpcs(service_zone_id, vpc_name)
        if vpc_content is None:
            return None

        vpc_list = [item["vpcId"] for item in vpc_content]
        return vpc_list[0]

    def find_valid_vpc(self, service_zone_id, vpc_name):
        vpc_content = self.list_vpcs(service_zone_id, vpc_name)
        if vpc_content is None:
            return None

        vpc_list = [item["vpcId"] for item in vpc_content if item["vpcState"] == "ACTIVE"]
        for vpc_id in vpc_list:
            igw_list = self.list_igws(vpc_id)
            igw_list = [igw for igw in igw_list if igw["internetGatewayState"] == "ATTACHED"]
            subnet_list = self.list_subnets(vpc_id)
            subnet_list = [subnet for subnet in subnet_list if subnet["subnetState"] == "ACTIVE" and subnet["subnetType"] == "PUBLIC"]
            if len(igw_list) > 0 and len(subnet_list) > 0:
                return vpc_id

        return None

    def define_vpc_name(self, region: str):
        parts = region.split("-")
        return "Skyplane" + parts[0][0:2] + parts[1] + parts[2]

    def make_vpc(self, region: str):
        service_zone_id = self.get_service_zone_id(region)

        vpc_name = self.define_vpc_name(region)

        # find matching valid VPC
        skyplane_vpc = self.find_valid_vpc(service_zone_id, vpc_name)

        try:
            if skyplane_vpc is None:
                # create VPC
                skyplane_vpc = self.create_vpc(region, service_zone_id, vpc_name)

                # create Subnet
                subnet = self.create_subnet(region, vpc_name, skyplane_vpc)

                # create Internet Gateway - firewall, routing table creation is done by SCP
                igw = self.create_igw(region, service_zone_id, skyplane_vpc)

                # create security group
                sg = self.create_security_group(region, service_zone_id, skyplane_vpc)
                self.create_security_group_in_rule(region, sg)
                self.create_security_group_out_rule(region, sg)

                # create firewall rules
                self.check_created_igw(region, igw, skyplane_vpc)
                self.check_created_subnet(region, subnet, skyplane_vpc)

            return skyplane_vpc
        except Exception as e:
            logger.error(e)
            raise e

    def delete_security_group_rules(self, region, securityGroupId: str, ruleIds):
        url = f"/security-group/v2/security-groups/{securityGroupId}/rules"
        req_body = {"ruleDeletionType": "PARTIAL", "ruleIds": ruleIds}
        try:
            self.scp_client._delete(url, req_body)
            delete_completion_condition = lambda: all(not self.list_security_group_rules(securityGroupId, rule) for rule in ruleIds)
            self.scp_client.wait_for_completion(f"[{region}] Deleting SCP Skyplane Security Group Rule", delete_completion_condition)
        except Exception as e:
            logger.error(e)
            raise e

    def list_security_group_rules(self, securityGroupId: str, ruleId: str):
        url = f"/security-group/v2/security-groups/{securityGroupId}/rules"
        response = self.scp_client._get(url)
        if ruleId is not None:
            result = [result for result in response if result["ruleId"] == ruleId]
        else:  # list all ruleIds
            result = [rule["ruleId"] for rule in response]
        return result

    def create_security_group_in_rule(self, region, securityGroupId: str):
        url = f"/security-group/v2/security-groups/{securityGroupId}/rules"
        req_body = {
            "ruleDirection": "IN",
            "services": [
                {
                    "serviceType": "TCP_ALL",
                }
            ],
            "sourceIpAddresses": ["0.0.0.0/0"],
            "ruleDescription": "Skyplane Security Group In rule(ssh)",
        }
        try:
            response = self.scp_client._post(url, req_body)
            ruleId = response["resourceId"]
            create_completion_condition = lambda: self.list_security_group_rules(securityGroupId, ruleId)[0]["ruleState"] == "ACTIVE"
            self.scp_client.wait_for_completion(f"[{region}] Creating SCP Skyplane Security Group In Rule", create_completion_condition)
            return ruleId

        except Exception as e:
            logger.error(e)
            raise e

    def create_security_group_out_rule(self, region, securityGroupId: str):
        url = f"/security-group/v2/security-groups/{securityGroupId}/rules"
        req_body = {
            "ruleDirection": "OUT",
            "services": [
                {
                    "serviceType": "TCP_ALL",
                }
            ],
            "destinationIpAddresses": ["0.0.0.0/0"],
            "ruleDescription": "Skyplane Security Group Out rule",
        }
        try:
            response = self.scp_client._post(url, req_body)
            ruleId = response["resourceId"]
            create_completion_condition = lambda: self.list_security_group_rules(securityGroupId, ruleId)[0]["ruleState"] == "ACTIVE"
            self.scp_client.wait_for_completion(f"[{region}] Creating SCP Skyplane Security Group Out Rule", create_completion_condition)
            return ruleId
        except Exception as e:
            logger.error(e)
            raise e

    def delete_firewall_rules(self, region, firewallId: str, ruleIds, retry_limit=10):
        url = f"/firewall/v2/firewalls/{firewallId}/rules"

        if ruleIds is None:
            ruleIds = self.list_firewall_rules(firewallId, None)

        req_body = {"ruleDeletionType": "PARTIAL", "ruleIds": ruleIds}
        retries = 0
        while retries < retry_limit:
            try:
                # print(f"try delete_firewall_rules: req_body - [{region}] retries : {retries} \n {req_body}")
                response = self.scp_client._delete(url, req_body)
                # delete_completion_condition = lambda: all(not self.list_firewall_rules(firewallId, rule) for rule in ruleIds)
                # self.scp_client.wait_for_completion(f"[{region}] Deleting SCP Skyplane Firewall Rules", delete_completion_condition)
                return response
            except Exception as e:
                # error code 500 retry
                if "500" in e.args[0]:
                    retries += 1
                    # print(f"Error deleting firewall rules(): {e}")
                    logger.fs.debug(f"Error deleting firewall rules(): {e}")
                    if retries == retry_limit:
                        logger.error(e)
                        raise e
                    time.sleep(5)
                else:
                    logger.error(e)
                    raise e

    def get_firewallId(self, igwId: str):
        url = f"/firewall/v2/firewalls?objectId={igwId}"
        result = self.scp_client._get(url)[0]["firewallId"]
        return result

    def get_firewall_rule_details(self, firewallId: str, ruleId: str):
        url = f"/firewall/v2/firewalls/{firewallId}/rules/{ruleId}"
        response = self.scp_client._getDetail(url)
        return response

    def list_firewall_rules(self, firewallId: str, ruleId=None):
        url = f"/firewall/v2/firewalls/{firewallId}/rules?size=1000"
        response = self.scp_client._get(url)
        if ruleId is None:
            result = [rule["ruleId"] for rule in response]
        else:
            result = [result for result in response if result["ruleId"] == ruleId]
        return result

    def add_firewall_22_rule(self, region, servergroup, virtualServerId, retry_limit=5):
        # check rule exist
        def check_rule_exist(server_id, firewall_id):
            ip = self.get_vs_details(server_id)["ip"]
            rules = self.list_firewall_rules(firewall_id, None)
            for rule in rules:
                rule_details = self.get_firewall_rule_details(firewall_id, rule)
                if rule_details["ruleDirection"] == "IN" and rule_details["ruleDescription"] == "Skyplane Firewall ssh rule":
                    if ip in rule_details["destinationIpAddresses"]:
                        # print(f"Pass - {region}-{servergroup} : Already exists firewall rule")
                        return True

        servers = self.list_server_group_instance(servergroup)
        try:
            private_ips = [self.get_vs_details(server["virtualServerId"])["ip"] for server in servers]
        except Exception as e:
            # print(f"Pass - {region}-{servergroup} : Waiting for all servers to be Editing status")
            return e

        vpcId = self.get_vs_details(virtualServerId)["vpcId"]
        igwId = self.list_igws(vpcId)[0]["internetGatewayId"]
        firewallId = self.get_firewallId(igwId)

        url = f"/firewall/v2/firewalls/{firewallId}/rules"
        req_body = {
            "sourceIpAddresses": ["0.0.0.0/0"],
            "destinationIpAddresses": private_ips,
            "services": [{"serviceType": "TCP", "serviceValue": "22"}],
            "ruleDirection": "IN",
            "ruleAction": "ALLOW",
            "isRuleEnabled": True,
            "ruleLocationType": "LAST",
            "ruleDescription": "Skyplane Firewall ssh rule",
        }
        retries = 0
        while retries < retry_limit:
            try:
                if not check_rule_exist(virtualServerId, firewallId):
                    response = self.scp_client.post(url, req_body)
                    ruleId = response["resourceId"]
                    create_completion_condition = lambda: self.get_firewall_rule_details(firewallId, ruleId)["ruleState"] == "ACTIVE"
                    self.scp_client.wait_for_completion(
                        f"[{region}:{private_ips}] Creating SCP Skyplane Firewall 22 Rule", create_completion_condition
                    )
                    return ruleId
                else:
                    return None
            except Exception as e:
                if "500" in e.args[0]:
                    # print(f"Pass - Error creating firewall in rule(): {e}")
                    logger.fs.debug(f"Pass - Error creating firewall in rule(): {e}")
                    return None
                else:
                    retries += 1
                    # print(f"Error creating firewall in rule(): {e}")
                    logger.fs.debug(f"Error creating firewall in rule(): {e}")
                    if retries == retry_limit:
                        logger.fs.error(e)
                        raise e
            time.sleep(1)

    def add_firewall_rule(self, region: str, virtualServerIds: Optional[List[str]] = None):
        for virtualServerId in virtualServerIds:
            try:
                vpcId = self.get_vs_details(virtualServerId)["vpcId"]
                ip = self.get_vs_details(virtualServerId)["ip"]

                igwId = self.list_igws(vpcId)[0]["internetGatewayId"]
                firewallId = self.get_firewallId(igwId)

                self.create_firewall_in_rule(region, firewallId, ip)
                self.create_firewall_out_rule(region, firewallId, ip)
            except Exception as e:
                logger.error(e)
                raise e

    def add_firewall_rule_all(self, region, private_ips, vpcids):
        vpcId = vpcids[0]
        try:
            # igwId = self.list_igws(vpcId)[0]['internetGatewayId']
            igwId = self.get_igw(vpcId)
            firewallId = self.get_firewallId(igwId)

            self.create_firewall_in_rule(region, firewallId, private_ips)
            self.create_firewall_out_rule(region, firewallId, private_ips)
        except Exception as e:
            logger.error(e)
            raise e

    def get_igw(self, vpc_Id: str):
        return self.list_igws(vpc_Id)[0]["internetGatewayId"]

    def create_firewall_in_rule(self, region, firewallId, internalIp, retry_limit=60):
        url = f"/firewall/v2/firewalls/{firewallId}/rules"
        req_body = {
            "sourceIpAddresses": ["0.0.0.0/0"],
            "destinationIpAddresses": internalIp,
            "services": [{"serviceType": "TCP_ALL"}],
            "ruleDirection": "IN",
            "ruleAction": "ALLOW",
            "isRuleEnabled": True,
            "ruleLocationType": "LAST",
            "ruleDescription": "Skyplane Firewall In rule",
        }
        retries = 0
        while retries < retry_limit:
            try:
                response = self.scp_client._post(url, req_body)
                # print(response)
                # time.sleep(3)
                ruleId = response["resourceId"]
                create_completion_condition = lambda: self.get_firewall_rule_details(firewallId, ruleId)["ruleState"] == "ACTIVE"
                self.scp_client.wait_for_completion(f"[{region}] Creating SCP Skyplane Firewall In Rule", create_completion_condition)
                return ruleId
            except Exception as e:
                retries += 1
                print(f"Error creating firewall in rule(): {e}")
                if retries == retry_limit:
                    logger.error(e)
                    raise e
            time.sleep(2)

    def create_firewall_out_rule(self, region, firewallId, internalIp, retry_limit=60):
        url = f"/firewall/v2/firewalls/{firewallId}/rules"
        req_body = {
            "sourceIpAddresses": internalIp,
            "destinationIpAddresses": ["0.0.0.0/0"],
            "services": [{"serviceType": "TCP_ALL"}],
            "ruleDirection": "OUT",
            "ruleAction": "ALLOW",
            "isRuleEnabled": True,
            "ruleLocationType": "FIRST",
            "ruleDescription": "Skyplane Firewall Out rule",
        }
        retries = 0
        while retries < retry_limit:
            try:
                response = self.scp_client._post(url, req_body)
                ruleId = response["resourceId"]
                create_completion_condition = lambda: self.get_firewall_rule_details(firewallId, ruleId)["ruleState"] == "ACTIVE"
                self.scp_client.wait_for_completion(f"[{region}] Creating SCP Skyplane Firewall Out Rule", create_completion_condition)
                return ruleId
            except Exception as e:
                retries += 1
                print(f"Error creating firewall out rule(): {e}")
                if retries == retry_limit:
                    logger.error(e)
                    raise e
            time.sleep(2)

    def list_instances(self, instanceId):
        if instanceId is None:
            url = f"/virtual-server/v2/virtual-servers"
        else:
            try:
                instanceName = self.get_vs_details(instanceId)["virtualServerName"]
            except Exception as e:
                if "404" in e.args[0]:
                    return None
            url = f"/virtual-server/v2/virtual-servers?virtualServerName={instanceName}"

        result = self.scp_client._get(url)
        if len(result) == 0:
            return None
        else:
            return result

    def start_instance(self, instanceId: str):
        url = f"/virtual-server/v2/virtual-servers/{instanceId}/start"
        try:
            response = self.scp_client._post(url, req_body={})
            instanceId = response["resourceId"]
            completion_condition = lambda: self.list_instances(instanceId)[0]["virtualServerState"] == "RUNNING"
            self.scp_client.wait_for_completion(f" Starting SCP Skyplane Virtual Server", completion_condition)
        except Exception as e:
            logger.error(e)
            raise e

    def stop_instance(self, instanceId: str):
        url = f"/virtual-server/v2/virtual-servers/{instanceId}/stop"
        try:
            response = self.scp_client._post(url, req_body={})
            instanceId = response["resourceId"]
            completion_condition = lambda: self.list_instances(instanceId)[0]["virtualServerState"] == "STOPPED"
            self.scp_client.wait_for_completion(f" Stopping SCP Skyplane Virtual Server", completion_condition)
        except Exception as e:
            logger.error(e)
            raise e

    def terminate_instance(self, instanceId: str):
        url = f"/virtual-server/v2/virtual-servers/{instanceId}"
        try:
            response = self.scp_client._delete(url, req_body={})
            instanceId = response["resourceId"]
            # completion_condition = lambda: self.list_instances(instanceId) is None
            # self.scp_client.wait_for_completion(f" Terminating SCP Skyplane Virtual Server", completion_condition)
        except Exception as e:
            logger.error(e)
            raise e

    def get_nic_details(self, virtualServerId):
        url = f"/virtual-server/v2/virtual-servers/{virtualServerId}/nics"
        response = self.scp_client._get(url)
        return response

    def get_vs_details(self, virtualServerId):
        url = f"/virtual-server/v3/virtual-servers/{virtualServerId}"
        response = self.scp_client._getDetail(url)
        return response

    def create_server_group(self, region):
        url = f"/server-group/v2/server-groups"
        serviceZoneId = self.get_service_zone_id(region)
        req_body = {
            "serverGroupName": f"skyplane-{uuid.uuid4().hex[:8]}",
            "serviceZoneId": serviceZoneId,
            "serviceFor": "VIRTUAL_SERVER",
            "servicedGroupFor": "COMPUTE",
        }
        response = self.scp_client._post(url, req_body)
        serverGroupId = response["serverGroupId"]
        return serverGroupId

    def list_server_group(self):
        url = f"/server-group/v2/server-groups"
        response = self.scp_client._get(url)
        return response

    def delete_server_group(self, serverGroupId):
        url = f"/server-group/v2/server-groups/{serverGroupId}"
        if len(self.list_server_group_instance(serverGroupId)) == 0:
            response = self.scp_client._delete(url)
            return response

    def detail_server_group(self, serverGroupId):
        url = f"/server-group/v2/server-groups/{serverGroupId}"
        response = self.scp_client._getDetail(url)
        return response

    def list_server_group_instance(self, serverGroupId):
        url = f"/virtual-server/v2/virtual-servers?serverGroupId={serverGroupId}"
        response = self.scp_client._get(url)
        return response
