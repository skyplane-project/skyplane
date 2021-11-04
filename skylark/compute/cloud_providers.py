import threading
from typing import List

from skylark.compute.server import Server


class CloudProvider:
    ns = threading.local()

    def __init__(self):
        self.instance_prefix = "skylark"

    @property
    def region_list(self):
        raise NotImplementedError

    def get_instance_list(self, region) -> List[Server]:
        raise NotImplementedError

    def get_matching_instances(self, region=None, instance_type=None, state=None, tags={"skylark": "true"}):
        if isinstance(region, str):
            region = [region]
        elif region is None:
            region = self.region_list

        matching_instances = []
        for r in region:
            instances = self.get_instance_list(r)
            for instance in instances:
                if not (instance_type is None or instance_type == instance.instance_class):
                    continue
                if not (state is None or state == instance.instance_state):
                    continue
                if not all(instance.tags.get(k, "") == v for k, v in tags.items()):
                    continue
                matching_instances.append(instance)
        return matching_instances
