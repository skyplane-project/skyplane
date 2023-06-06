from skyplane.gateway.gateway_program import (
    GatewayProgram,
    GatewaySend,
    GatewayWriteLocal,
    GatewayWriteObjectStore,
    GatewayGenData,
    GatewayReadObjectStore,
)
from typing import List, Dict, Optional


class TopologyPlanGateway:

    """
    Represents a gateway in the topology plan.
    """

    def __init__(self, region_tag: str, gateway_id: str, gateway_vm: Optional[str]):
        self.region_tag = region_tag
        self.gateway_id = gateway_id
        self.gateway_vm = gateway_vm
        self.gateway_program = None

        # ip addresses
        self.private_ip_address = None
        self.public_ip_address = None

    @property
    def provider(self):
        """Get the provider of the gateway"""
        return self.region_tag.split(":")[0]

    @property
    def region(self):
        """Get the region of the gateway"""
        return self.region_tag.split(":")[1]

    @property
    def vm_type(self):
        """Get the vm type of the gateway"""
        return self.gateway_vm

    def set_private_ip_address(self, private_ip_address: str):
        """Set the IP address of the gateway (not determined until provisioning is complete)"""
        self.private_ip_address = private_ip_address

    def set_public_ip_address(self, public_ip_address: str):
        """Set the public IP address of the gateway (not determined until provisioning is complete)"""
        self.public_ip_address = public_ip_address

    def set_gateway_program(self, gateway_program: GatewayProgram):
        """Set the gateway program for the gateway"""
        self.gateway_program = gateway_program

    def write_operators(self) -> List[GatewayWriteObjectStore]:
        """Get all write operators in the gateway program"""
        # TODO: include other write operator types
        return [op for op in self.gateway_program.get_operators() if isinstance(op, GatewayWriteObjectStore)]


class TopologyPlan:
    """
    The TopologyPlan constains a list of gateway programs corresponding to each gateway in the dataplane.
    """

    def __init__(self, src_region_tag: str, dest_region_tags: List[str], cost_per_gb: float = 0.0):
        self.src_region_tag = src_region_tag
        self.dest_region_tags = dest_region_tags
        self.gateways = {}
        self.cost_per_gb = cost_per_gb

    @property
    def regions(self) -> List[str]:
        """Get all regions in the topology plan"""
        return list(set([gateway.region for gateway in self.gateways.values()]))

    @property
    def region_tags(self) -> List[str]:
        """Get all region tags in the topology plan"""
        return list(set([gateway.region_tag for gateway in self.gateways.values()]))

    def add_gateway(self, region_tag: str, vm_type: Optional[str] = None):
        """Create gateway in specified region"""
        gateway_id = region_tag + str(len([gateway for gateway in self.gateways.values() if gateway.region_tag == region_tag]))
        assert gateway_id not in self.gateways, f"Gateway id {gateway_id} in {self.gateways}"
        gateway = TopologyPlanGateway(region_tag, gateway_id, vm_type)
        self.gateways[gateway_id] = gateway
        return gateway

    def get_region_gateways(self, region_tag: str):
        """Get all gateways in a region"""
        return [gateway for gateway in self.gateways.values() if gateway.region_tag == region_tag]

    def get_gateways(self) -> List[TopologyPlanGateway]:
        """Get all gateways"""
        return list(self.gateways.values())

    def get_gateway(self, gateway_id: str) -> TopologyPlanGateway:
        return self.gateways[gateway_id]

    def set_gateway_program(self, region_tag: str, gateway_program: GatewayProgram):
        """Update all gateways in a region with specified gateway program"""
        for gateway in self.get_region_gateways(region_tag):
            gateway.set_gateway_program(gateway_program)

    def set_ip_addresses(self, gateway_id: str, private_ip_address: str, public_ip_address: str):
        """Set IP address of a gateway"""
        self.gateways[gateway_id].set_private_ip_address(private_ip_address)
        self.gateways[gateway_id].set_public_ip_address(public_ip_address)

    def generate_gateway_program(self, region_tag: str):
        """Generate gateway program for all gateways in a region"""
        # TODO: eventually let gateways in same region have different programs
        for gateway in self.get_region_gateways(region_tag):
            assert gateway.gateway_program is not None, f"Gateway program for {region_tag} has not been set"
            return gateway.gateway_program.to_json()

    def get_outgoing_paths(self, gateway_id: str):
        """Get all outgoing paths from a gateway"""
        outgoing_paths = {}
        for operator in self.gateways[gateway_id].gateway_program.get_operators():
            if isinstance(operator, GatewaySend):
                # get id of gateway that operator is sending to
                assert (
                    operator.target_gateway_id in self.gateways
                ), f"Gateway {operator.target_gateway_id} not found in gateway list {self.gateways}"
                outgoing_paths[operator.target_gateway_id] = operator.num_connections
        return outgoing_paths

    def get_gateway_program_json(self, gateway_id: str):
        """Get gateway program for a gateway"""
        return self.gateways[gateway_id].gateway_program.to_json()

    def get_gateway_info_json(self):
        """Return JSON mapping between gateway ids to public ip, public ip, provider, and region"""
        gateway_info = {}
        for gateway in self.gateways.values():
            gateway_info[gateway.gateway_id] = {
                "private_ip_address": gateway.private_ip_address,
                "public_ip_address": gateway.public_ip_address,
                "region": gateway.region,
                "provider": gateway.provider,
            }
        return gateway_info

    def sink_instances(self, region_tag: Optional[str] = None) -> Dict[str, List[TopologyPlanGateway]]:
        """Return list of gateways that have a sink operator (GatewayWriteObjectStore, GatewayWriteLocal)"""
        nodes = {}
        for gateway in self.gateways.values():
            for operator in gateway.gateway_program.get_operators():
                # dont include if wrong region
                if isinstance(operator, GatewayWriteObjectStore):
                    if region_tag is not None and operator.bucket_region != region_tag:
                        continue
                if isinstance(operator, GatewayWriteLocal):
                    if region_tag is not None and gateway.region_tag != region_tag:
                        continue

                if isinstance(operator, GatewayWriteObjectStore) or isinstance(operator, GatewayWriteLocal):
                    if gateway.region_tag not in nodes:
                        nodes[gateway.region_tag] = []
                    nodes[gateway.region_tag].append(gateway)
                    break
        return nodes

    def source_instances(self):
        """Return list of gateways that have a source operator (GatewayReadObjectStore, GatewayReadLocal, GatewayGenData)"""
        nodes = []
        for gateway in self.gateways.values():
            for operator in gateway.gateway_program.get_operators():
                if isinstance(operator, GatewayReadObjectStore) or isinstance(operator, GatewayGenData):
                    nodes.append(gateway)
                    break

        return nodes

    def per_region_count(self) -> Dict[str, int]:
        """Return number of gateways VMs per region"""
        counts = {}
        for node in self.get_gateways():
            counts[node.region_tag] = counts.get(node.region_tag, 0) + 1
        return counts

    def to_dict(self):
        return {gateway_id: gateway.gateway_program.to_dict() for gateway_id, gateway in self.gateways.items()}
