import time

from ibm_cloud_sdk_core import ApiException
from ibm_cloud_sdk_core.authenticators import IAMAuthenticator
from ibm_vpc import VpcV1


def delete_config(config):

    # parse config
    if 'provider' in config:
        from skyplane.compute.ibmcloud.gen2.skyplane import parse_config
    else:
        raise Exception('Config file not supported')
    
    vpc_config = parse_config(config)
    
    authenticator = IAMAuthenticator(vpc_config['iam_api_key'],
                                     url=vpc_config.get('iam_endpoint'))
    ibm_vpc_client = VpcV1('2021-01-19', authenticator=authenticator)
    ibm_vpc_client.set_service_url(vpc_config['endpoint'] + '/v1')
    
    # find and delete all vpc vsis
    instances_info = ibm_vpc_client.list_instances(vpc_id=vpc_config['vpc_id']).get_result()
    for ins in instances_info['instances']:
        # delete floating ips
        print('Deleting instance {}'.format(ins['name']))
        
        interface_id = ins['network_interfaces'][0]['id']
        fips = ibm_vpc_client.list_instance_network_interface_floating_ips(
                    ins['id'], interface_id).get_result()['floating_ips']
        if fips:
            fip = fips[0]['id']
            ibm_vpc_client.delete_floating_ip(fip)
        
        # delete instance        
        ibm_vpc_client.delete_instance(ins['id'])

    time.sleep(5)
    
    # delete subnet
    print('Deleting subnet')
    try:
        ibm_vpc_client.delete_subnet(vpc_config['subnet_id'])
    except ApiException as e:
        if e.code == 404:
            pass
        else:
            raise e

        time.sleep(25)

    # delete gateway?
    print('Deleting gateway')
    gateways = ibm_vpc_client.list_public_gateways().get_result()
    for gw in gateways['public_gateways']:
        if gw['vpc']['id'] == vpc_config['vpc_id']:
            ibm_vpc_client.delete_public_gateway(gw['id'])

    time.sleep(15)
    
    # delete ssh key
    try:
        ibm_vpc_client.delete_key(id=vpc_config['key_id'])
    except ApiException as e:
        if e.code == 404:
            pass
        else:
            raise e

        time.sleep(5)
    
    # delete vpc
    print('Deleting VPC')
    try:
        ibm_vpc_client.delete_vpc(vpc_config['vpc_id'])
    except ApiException as e:
        if e.code == 404:
            pass
        else:
            raise e
