from typing import List, Optional, Union

from skylark.compute.server import Server, ServerState
from skylark.utils.utils import do_parallel


class CloudProvider:

    logging_enabled = True # For Dozzle
    log_viewer_port = 8888

    @property
    def name(self):
        raise NotImplementedError

    @staticmethod
    def region_list():
        raise NotImplementedError

    @staticmethod
    def get_transfer_cost(src_key, dst_key, premium_tier=True):
        if src_key == dst_key:
            return 0.0
        src_provider, _ = src_key.split(":")
        if src_provider == "aws":
            from skylark.compute.aws.aws_cloud_provider import AWSCloudProvider

            return AWSCloudProvider.get_transfer_cost(src_key, dst_key, premium_tier)
        elif src_provider == "gcp":
            from skylark.compute.gcp.gcp_cloud_provider import GCPCloudProvider

            return GCPCloudProvider.get_transfer_cost(src_key, dst_key, premium_tier)
        elif src_provider == "azure":
            from skylark.compute.azure.azure_cloud_provider import AzureCloudProvider

            return AzureCloudProvider.get_transfer_cost(src_key, dst_key, premium_tier)
        else:
            raise NotImplementedError

    def get_instance_list(self, region) -> List[Server]:
        raise NotImplementedError

    def get_matching_instances(
        self,
        region: Optional[str] = None,
        instance_type: Optional[str] = None,
        state: Optional[Union[ServerState, List[ServerState]]] = None,
        tags={"skylark": "true"},
    ) -> List[Server]:
        if isinstance(region, str):
            results = [(region, self.get_instance_list(region))]
        elif region is None:
            results = do_parallel(self.get_instance_list, self.region_list(), n=-1)

        matching_instances = []
        for _, instances in results:
            for instance in instances:
                if not (instance_type is None or instance_type == instance.instance_class()):
                    continue
                if not all(instance.tags().get(k, "") == v for k, v in tags.items()):
                    continue
                if not (
                    state is None or (isinstance(state, list) and instance.instance_state() in state) or instance.instance_state == state
                ):
                    continue
                matching_instances.append(instance)
        return matching_instances
