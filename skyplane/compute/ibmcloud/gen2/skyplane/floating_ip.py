from skyplane.compute.ibmcloud.gen2.config_builder import ConfigBuilder
from typing import Any, Dict

from skyplane.compute.ibmcloud.gen2.utils import get_option_from_list

class FloatingIpConfig(ConfigBuilder):
    
    def run(self) -> Dict[str, Any]:
        head_ip = None
        floating_ips = self.ibm_vpc_client.list_floating_ips().get_result()['floating_ips']
        # filter away floating-ips that are already bound, or belong to a zone different than the one chosen by user  
        free_floating_ips = [ip for ip in floating_ips if not ip.get('target') and self.base_config['provider']['zone_name']==ip['zone']['name']] 

        if free_floating_ips:
            ALLOCATE_NEW_FLOATING_IP = 'Allocate new floating ip'
            head_ip_obj = get_option_from_list("Choose head ip", free_floating_ips, choice_key='address', do_nothing=ALLOCATE_NEW_FLOATING_IP)
            if head_ip_obj and (head_ip_obj != ALLOCATE_NEW_FLOATING_IP):
                head_ip = head_ip_obj['address']
                
            if self.base_config.get('available_node_types'):
                for available_node_type in self.base_config['available_node_types']:
                    if head_ip:
                        self.base_config['available_node_types'][available_node_type]['node_config']['head_ip'] = head_ip
                    else:
                        self.base_config['available_node_types'][available_node_type]['node_config'].pop('head_ip', None)
            else:
                self.base_config['available_node_types']['ray_head_default']['node_config']['head_ip'] = head_ip

        return self.base_config
    
    def create_default(self):
        NotImplementedError("This backend doesn't support it yet")
