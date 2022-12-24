from typing import Any, Dict
from skyplane.compute.ibmcloud.gen2.config_builder import ConfigBuilder, update_decorator, spinner
from skyplane.compute.ibmcloud.gen2.utils import find_obj


class SkyProfileConfig(ConfigBuilder):
    def __init__(self, base_config: Dict[str, Any]) -> None:
        super().__init__(base_config)
        if self.base_config.get("available_node_types"):
            for available_node_type in self.base_config["available_node_types"]:
                self.defaults["profile_name"] = self.base_config["available_node_types"][available_node_type]["node_config"].get(
                    "instance_profile_name"
                )
                break

    def update_config(self, profile_name):

        # cpu number based on profile
        cpu_num = int(profile_name.split("-")[1].split("x")[0])

        if self.base_config.get("available_node_types"):
            for available_node_type in self.base_config["available_node_types"]:
                self.base_config["available_node_types"][available_node_type]["node_config"]["instance_profile_name"] = profile_name
                self.base_config["available_node_types"][available_node_type]["resources"]["CPU"] = cpu_num
        else:
            self.base_config["available_node_types"]["ray_head_default"]["node_config"]["instance_profile_name"] = profile_name
            self.base_config["available_node_types"]["ray_head_default"]["resources"]["CPU"] = cpu_num

    @update_decorator
    def verify(self, base_config):
        profile_name = self.defaults["profile_name"]
        instance_profile_objects = self.ibm_vpc_client.list_instance_profiles().get_result()["profiles"]
        profile = find_obj(instance_profile_objects, "dummy", obj_name=profile_name)
        if not profile:
            raise Exception(f"Specified profile {profile_name} not found in the profile list {instance_profile_objects}")
        return profile_name

    @update_decorator
    def create_default(self):
        return "bx2-2x8"
