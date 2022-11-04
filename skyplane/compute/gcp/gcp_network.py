from typing import List

from skyplane.compute.gcp.gcp_auth import GCPAuthentication
from skyplane.utils import imports, logger


class GCPNetwork:
    def __init__(self, auth: GCPAuthentication, vpc_name="skyplane"):
        self.auth = auth
        self.vpc_name = vpc_name

    @imports.inject("googleapiclient.errors", pip_extra="gcp")
    def create_network(errors, self):
        compute = self.auth.get_gcp_client()
        try:
            compute.networks().get(project=self.auth.project_id, network=self.vpc_name).execute()
        except errors.HttpError as e:
            if e.resp.status == 404:  # create network
                op = (
                    compute.networks()
                    .insert(project=self.auth.project_id, body={"name": self.vpc_name, "subnetMode": "auto", "autoCreateSubnetworks": True})
                    .execute()
                )
                self.auth.wait_for_operation_to_complete("global", op["name"])
            else:
                raise e

    @imports.inject("googleapiclient.errors", pip_extra="gcp")
    def get_network(errors, self):
        compute = self.auth.get_gcp_client()
        try:
            return compute.networks().get(project=self.auth.project_id, network=self.vpc_name).execute()
        except errors.HttpError as e:
            if e.resp.status == 404:
                return None
            else:
                raise e

    @imports.inject("googleapiclient.errors", pip_extra="gcp")
    def delete_network(errors, self):
        """
        Delete VPC. This might error our in some cases.
        """
        compute = self.auth.get_gcp_client()
        request = compute.networks().delete(project=self.auth.project_id, network=self.vpc_name)
        try:
            delete_vpc_response = request.execute()
            self.auth.wait_for_operation_to_complete("global", delete_vpc_response)
        except errors.HttpError as e:
            logger.fs.warn(
                f"Unable to delete network. Ensure no active firewall rules acting upon the {self.vpc_name} VPC. Ensure no instances provisioned in the VPC "
            )
            logger.fs.error(e)

    @imports.inject("googleapiclient.errors", pip_extra="gcp")
    def get_firewall_rule(errors, self, firewall_rule_name):
        compute = self.auth.get_gcp_client()
        try:
            return compute.firewalls().get(project=self.auth.project_id, firewall=firewall_rule_name).execute()
        except errors.HttpError as e:
            if e.resp.status == 404:
                return None
            else:
                raise e

    @imports.inject("googleapiclient.errors", pip_extra="gcp")
    def create_firewall_rule(
        errors, self, rule_name: str, ip_ranges: List[str], ports: List[str], protocols: List[str], priority: int = 1000
    ):
        compute = self.auth.get_gcp_client()
        existing_rule = self.get_firewall_rule(rule_name)
        allowed = [{"IPProtocol": p, "ports": ports} for p in protocols if p != "icmp"] + [
            {"IPProtocol": p} for p in protocols if p == "icmp"
        ]
        if existing_rule:  # add new ip ranges, ports and protocols to existing rule
            existing_rule["sourceRanges"] = list(set(existing_rule["sourceRanges"] + ip_ranges))
            existing_rule["allowed"] = existing_rule["allowed"] + allowed
            op = compute.firewalls().update(project=self.auth.project_id, firewall=rule_name, body=existing_rule).execute()
            self.auth.wait_for_operation_to_complete("global", op["name"])
            logger.fs.debug(f"Updating firewall rule {rule_name}")
        else:  # create new rule
            body = {
                "name": rule_name,
                "network": f"global/networks/{self.vpc_name}",
                "sourceRanges": ip_ranges,
                "allowed": allowed,
                "priority": priority,
            }
            op = compute.firewalls().insert(project=self.auth.project_id, body=body).execute()
            self.auth.wait_for_operation_to_complete("global", op["name"])
            logger.fs.debug(f"Creating firewall rule {rule_name} with {body=}")

    @imports.inject("googleapiclient.errors", pip_extra="gcp")
    def delete_firewall_rule(errors, self, rule_name: str):
        compute = self.auth.get_gcp_client()
        try:
            op = compute.firewalls().delete(project=self.auth.project_id, firewall=rule_name).execute()
            self.auth.wait_for_operation_to_complete("global", op["name"])
        except errors.HttpError as e:
            if e.resp.status == 404:
                return None
            else:
                raise e

    @imports.inject("googleapiclient.errors", pip_extra="gcp")
    def create_default_firewall_rules(errors, self):
        self.create_firewall_rule(
            "skyplane-default-allow-internal",
            ["10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16"],
            ["0-65535"],
            ["tcp", "udp", "icmp"],
            priority=65533,
        )
        self.create_firewall_rule(
            "skyplane-default-allow-ssh",
            ["0.0.0.0/0"],
            ["22"],
            ["tcp"],
            priority=65533,
        )
        self.create_firewall_rule(
            "skyplane-default-allow-icmp",
            ["0.0.0.0/0"],
            [],
            ["icmp"],
            priority=65533,
        )
