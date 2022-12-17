import logging
from typing import Any, Dict
from ibm_cloud_sdk_core.authenticators import IAMAuthenticator
from ibm_platform_services import ResourceControllerV2, ResourceManagerV2
from ibm_vpc import VpcV1
from ibm_watson import IAMTokenManager
import threading
import time
import sys
from skyplane.compute.ibmcloud.gen2.utils import CACHE, find_default, get_option_from_list

logger = logging.getLogger(__name__)


def update_decorator(f):
    def foo(*args, **kwargs):
        result = f(*args, **kwargs)
        update_config = getattr(args[0], 'update_config')
        if not result:
            return args[0].base_config
        if isinstance(result, tuple):
            update_config(*result)
        else:
            update_config(result)
        return args[0].base_config

    return foo


class ConfigBuilder:
    """
    Interface for building IBM Cloud config files for Ray
    """
    iam_api_key, ibm_vpc_client, resource_service_client, resource_controller_service, compute_iam_endpoint, region = None, None, None, None, None, None

    def __init__(self, base_config: Dict[str, Any]) -> None:

        self.defaults = {}
        if not ConfigBuilder.iam_api_key:
            if 'ibm' in base_config and 'iam_api_key' in base_config['ibm']:
                ConfigBuilder.iam_api_key = base_config['ibm']['iam_api_key']
            elif 'provider' in base_config and 'iam_api_key' in base_config['provider']:
                ConfigBuilder.iam_api_key = base_config['provider']['iam_api_key']

        if not ConfigBuilder.ibm_vpc_client and ConfigBuilder.iam_api_key:
            authenticator = IAMAuthenticator(ConfigBuilder.iam_api_key, url=ConfigBuilder.compute_iam_endpoint)
            ConfigBuilder.ibm_vpc_client = VpcV1(
                '2021-01-19', authenticator=authenticator)
            ConfigBuilder.resource_service_client = ResourceManagerV2(
                authenticator=authenticator)
            ConfigBuilder.resource_controller_service = ResourceControllerV2(
                authenticator=authenticator)

        self.init_clients(ConfigBuilder.iam_api_key, ConfigBuilder.compute_iam_endpoint)

        self.base_config = base_config

    def init_clients(self, iam_api_key, iam_endpoint=None):
        authenticator = IAMAuthenticator(iam_api_key, url=iam_endpoint)
        self.ibm_vpc_client = ConfigBuilder.ibm_vpc_client
        self.resource_service_client = ResourceManagerV2(authenticator=authenticator)
        self.resource_controller_service = ResourceControllerV2(authenticator=authenticator)


    """Interacts with user to get all required parameters"""

    def run(self, config) -> Dict[str, Any]:
        """Return updated config dictionary that can be dumped to config file

        Run interactive questionnaire
        """
        raise NotImplementedError

    """Updates specified config dictionary"""

    def update_config(self, *args) -> Dict[str, Any]:
        """Updates config dictionary that can be dumped to config file"""
        return self.base_config

    def get_resources(self, resource_type=None):
        """
        :param resource_type: str of the following possible values: ['service_instance','resource_instance']
        :return: resources belonging to a specific resource group, filtered by provided resource_type
        """

        if 'resource_group_id' not in CACHE:
            self.select_resource_group()

        @spinner
        def _get_resources():
            res = self.resource_controller_service.list_resource_instances(
                resource_group_id=CACHE['resource_group_id'], type=resource_type).get_result()
            resource_instances = res['resources']

            while res['next_url']:
                start = res['next_url'].split('start=')[1]
                res = self.resource_controller_service.list_resource_instances(
                    resource_group_id=CACHE['resource_group_id'], type=resource_type,
                    start=start).get_result()

                resource_instances.extend(res['resources'])
            return resource_instances

        return _get_resources()

    def select_resource_group(self):
        """returns resource group id of a resource group the user will be prompted to pick.
        stores result in CACHE['resource_group_id'] for further usage"""

        @spinner
        def get_resource_groups():
            return self.resource_service_client.list_resource_groups().get_result()['resources']

        res_group_objects = get_resource_groups()

        default = find_default(self.defaults, res_group_objects, id='resource_group_id')
        res_group_obj = get_option_from_list("Select resource group", res_group_objects, default=default)

        CACHE['resource_group_id'] = res_group_obj['id']  # cache group resource id for later use in storage

        return res_group_obj['id']

    def get_oauth_token(self):
        """:returns a temporary authentication token required by various IBM cloud APIs """

        iam_token_manager = IAMTokenManager(apikey=self.base_config['ibm']['iam_api_key'], url=ConfigBuilder.compute_iam_endpoint)
        return iam_token_manager.get_token()

    @update_decorator
    def verify(self, base_config):
        pass


class Spinner(threading.Thread):

    def __init__(self, *args, **kwargs):
        super(Spinner, self).__init__(*args, **kwargs)
        self.sttop = False

    def stop(self):
        self.sttop = True

    def stopped(self):
        return self.sttop

    def run(self):
        while True:
            if self.stopped():
                sys.stdout.write('\b')
                sys.stdout.flush()
                return

            for cursor in '\\|/-':
                time.sleep(0.1)
                sys.stdout.write('\r{}'.format(cursor))
                sys.stdout.flush()


def spinner(f):
    def foo(*args, **kwargs):
        s = Spinner()
        s.daemon = True
        s.start()
        result = f(*args, **kwargs)

        s.stop()
        s.join()

        return result

    return foo
