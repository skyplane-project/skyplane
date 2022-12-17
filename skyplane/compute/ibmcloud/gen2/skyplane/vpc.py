from typing import Any, Dict

import inquirer
from skyplane.compute.ibmcloud.gen2.vpc import VPCConfig

REQUIRED_RULES = {'outbound_tcp_all': 'selected security group is missing rule permitting outbound TCP access\n', 'outbound_udp_all': 'selected security group is missing rule permitting outbound UDP access\n', 'inbound_tcp_sg': 'selected security group is missing rule permiting inbound tcp traffic inside selected security group\n',
                  'inbound_tcp_22': 'selected security group is missing rule permiting inbound traffic to tcp port 22 required for ssh\n', 'inbound_tcp_6379': 'selected security group is missing rule permiting inbound traffic to tcp port 6379 required for Redis\n', 'inbound_tcp_8265': 'selected security group is missing rule permiting inbound traffic to tcp port 8265 required to access Ray Dashboard\n'}


def validate_security_group(ibm_vpc_client, sec_group_id):
    errors = validate_security_group_rules(ibm_vpc_client, sec_group_id)

    if errors:
        for val in errors.values():
            print(f"\033[91m{val}\033[0m")

        questions = [
            inquirer.List('answer',
                          message='Selected security group is missing required rules, see error above, update with required rules?',
                          choices=['yes', 'no'], default='yes'
                          ), ]

        answers = inquirer.prompt(questions, raise_keyboard_interrupt=True)

        if answers['answer'] == 'yes':
            add_rules_to_security_group(ibm_vpc_client, sec_group_id, errors)
        else:
            exit(1)

        # just in case, validate again the updated/created security group
        errors = validate_security_group_rules(ibm_vpc_client, sec_group_id)
        if not errors:
            return
        else:
            print(f'Something failed during security group rules update/create, please update the required rules manually using ibmcli or web ui and try again')
            exit(1)
    else:
        return


def validate_security_group_rules(ibm_vpc_client, sg_id):
    required_rules = REQUIRED_RULES.copy()

    sg = ibm_vpc_client.get_security_group(sg_id).get_result()

    for rule in sg['rules']:

        # check outbound rules that are not associated with a specific IP address range 
        if rule['direction'] == 'outbound' and rule['remote'] == {'cidr_block': '0.0.0.0/0'}:
            if rule['protocol'] == 'all':
                # outbound is fine!
                required_rules.pop('outbound_tcp_all', None)
                required_rules.pop('outbound_udp_all', None)
            elif rule['protocol'] == 'tcp':
                required_rules.pop('outbound_tcp_all', None)
            elif rule['protocol'] == 'udp':
                required_rules.pop('outbound_udp_all', None)
        
        # Check inbound rules 
        elif rule['direction'] == 'inbound':
            # check rules that are not associated with a specific IP address range
            if rule['remote'] == {'cidr_block': '0.0.0.0/0'}:
                # we interested only in all or tcp protocols
                if rule['protocol'] == 'all':
                    # there a rule permitting all traffic
                    required_rules.pop('inbound_tcp_sg', None)
                    required_rules.pop('inbound_tcp_22', None)
                    required_rules.pop('inbound_tcp_6379', None)
                    required_rules.pop('inbound_tcp_8265', None)

                elif rule['protocol'] == 'tcp':
                    if rule['port_min'] == 1 and rule['port_max'] == 65535:
                        # all ports are open
                        required_rules.pop('inbound_tcp_sg', None)
                        required_rules.pop('inbound_tcp_22', None)
                        required_rules.pop('inbound_tcp_6379', None)
                        required_rules.pop('inbound_tcp_8265', None)
                    else:
                        port_min = rule['port_min']
                        port_max = rule['port_max']
                        if port_min <= 22 and port_max >= 22:
                            required_rules.pop('inbound_tcp_22', None)
                        elif port_min <= 6379 and port_max >= 6379:
                            required_rules.pop('inbound_tcp_6379', None)
                        elif port_min <= 8265 and port_max >= 8265:
                            required_rules.pop('inbound_tcp_8265', None)

            # rule regards private traffic within the VSIs associated with the security group  
            elif rule['remote'].get('id') == sg['id']:
                # validate that inbound traffic inside group available
                if rule['protocol'] == 'all' or rule['protocol'] == 'tcp':
                    required_rules.pop('inbound_tcp_sg', None)

    return required_rules


def add_rules_to_security_group(ibm_vpc_client, sg_id, missing_rules):
    add_rule_msgs = {
        'outbound_tcp_all': f'Add rule to open all outbound TCP ports in selected security group {sg_id}',
        'outbound_udp_all': f'Add rule to open all outbound UDP ports in selected security group {sg_id}',
        'inbound_tcp_sg': f'Add rule to open inbound tcp traffic inside selected security group {sg_id}',
        'inbound_tcp_22': f'Add rule to open inbound tcp port 22 required for SSH in selected security group {sg_id}',
        'inbound_tcp_6379': f'Add rule to open inbound tcp port 6379 required for Redis in selected security group {sg_id}',
        'inbound_tcp_8265': f'Add rule to open inbound tcp port 8265 required to access Ray Dashboard in selected security group {sg_id}'}

    for missing_rule in missing_rules.keys():
        q = [
            inquirer.List('answer',
                          message=add_rule_msgs[missing_rule],
                          choices=['yes', 'no'],
                          default='yes')
        ]

        answers = inquirer.prompt(q, raise_keyboard_interrupt=True)
        if answers['answer'] == 'yes':
            security_group_rule_prototype_model = build_security_group_rule_prototype_model(
                missing_rule, sg_id=sg_id)
            ibm_vpc_client.create_security_group_rule(
                sg_id, security_group_rule_prototype_model).get_result()
        else:
            return False
    return True


def build_security_group_rule_prototype_model(missing_rule, sg_id=None):
    direction, protocol, port = missing_rule.split('_')
    remote = {"cidr_block": "0.0.0.0/0"}

    try:
        port = int(port)
        port_min = port
        port_max = port
    except:
        port_min = 1
        port_max = 65535

        # only valid if security group already exists
        if port == 'sg':
            if not sg_id:
                return None
            remote = {'id': sg_id}

    return {
        'direction': direction,
        'ip_version': 'ipv4',
        'protocol': protocol,
        'remote': remote,
        'port_min': port_min,
        'port_max': port_max
    }


class SkyVPCConfig(VPCConfig):

    def __init__(self, base_config: Dict[str, Any]) -> None:
        super().__init__(base_config)

        self.vpc_name = 'skyplane-vpc'
        self.sg_rules = REQUIRED_RULES
        
        if base_config.get('available_node_types'):
            for available_node_type in self.base_config['available_node_types']:
                self.defaults['vpc_id'] = base_config['available_node_types'][available_node_type]['node_config'].get('vpc_id')
                break        
        
    def update_config(self, vpc_obj, zone_obj, subnet_id):
        sec_group_id = vpc_obj['default_security_group']['id']

        validate_security_group(self.ibm_vpc_client, sec_group_id)

        self.base_config['provider']['zone_name'] = zone_obj['name']

        node_config = {
            'vpc_id': vpc_obj['id'],
            'resource_group_id': vpc_obj['resource_group']['id'],
            'security_group_id': sec_group_id,
            'subnet_id': subnet_id
        }

        if self.base_config.get('available_node_types'):
            for available_node_type in self.base_config['available_node_types']:
                self.base_config['available_node_types'][available_node_type]['node_config'].update(
                    node_config)
        else:
            self.base_config['available_node_types'] = {
                'ray_head_default': {'node_config': node_config}}
