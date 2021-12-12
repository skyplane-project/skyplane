import threading
from typing import List, Union

from loguru import logger

from skylark.compute.server import Server, ServerState


class CloudProvider:
    ns = threading.local()

    @property
    def name():
        raise NotImplementedError

    @staticmethod
    def region_list(self):
        raise NotImplementedError

    @staticmethod
    def get_transfer_cost(src_key, dst_key):
        if src_key == dst_key:
            return 0.0
        src_provider, _ = src_key.split(":")
        if src_provider == "aws":
            from skylark.compute.aws.aws_cloud_provider import AWSCloudProvider

            return AWSCloudProvider.get_transfer_cost(src_key, dst_key)
        elif src_provider == "gcp":
            from skylark.compute.gcp.gcp_cloud_provider import GCPCloudProvider

            return GCPCloudProvider.get_transfer_cost(src_key, dst_key)
        else:
            raise NotImplementedError

    def get_instance_list(self, region) -> List[Server]:
        raise NotImplementedError

    def get_matching_instances(
        self, region=None, instance_type=None, state: Union[ServerState, List[ServerState]] = None, tags={"skylark": "true"}
    ) -> List[Server]:
        if isinstance(region, str):
            region = [region]
        elif region is None:
            region = self.region_list()

        matching_instances = []
        for r in region:
            instances = self.get_instance_list(r)
            for instance in instances:
                if not (instance_type is None or instance_type == instance.instance_class):
                    continue
                if not (
                    state is None or (isinstance(state, list) and instance.instance_state in state) or instance.instance_state == state
                ):
                    continue
                if not all(instance.tags.get(k, "") == v for k, v in tags.items()):
                    continue
                matching_instances.append(instance)
        return matching_instances
