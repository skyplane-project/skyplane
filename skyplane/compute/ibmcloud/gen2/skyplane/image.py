from typing import Any, Dict
from skyplane.compute.ibmcloud.gen2.config_builder import ConfigBuilder, update_decorator
from skyplane.compute.ibmcloud.gen2.utils import find_obj


class SkyImageConfig(ConfigBuilder):
    def __init__(self, base_config: Dict[str, Any]) -> None:
        super().__init__(base_config)

        if self.base_config.get("available_node_types"):
            for available_node_type in self.base_config["available_node_types"]:
                self.defaults["image_id"] = self.base_config["available_node_types"][available_node_type]["node_config"].get("image_id")
                break

    def update_config(self, image_id, minimum_provisioned_size, custom_image):
        # minimum_provisioned_size will be used once non default image used
        if self.base_config.get("available_node_types"):
            for available_node_type in self.base_config["available_node_types"]:
                self.base_config["available_node_types"][available_node_type]["node_config"]["image_id"] = image_id
                self.base_config["available_node_types"][available_node_type]["node_config"][
                    "boot_volume_capacity"
                ] = minimum_provisioned_size
        else:
            self.base_config["available_node_types"]["ray_head_default"]["node_config"]["image_id"] = image_id
            self.base_config["available_node_types"]["ray_head_default"]["node_config"]["boot_volume_capacity"] = minimum_provisioned_size

        # if custom image, all setup commands should be removed
        # if custom_image:
        #     self.base_config['setup_commands'] = ['rm -f ~/.ray/tags.json']

    @update_decorator
    def verify(self, base_config):
        image_id = self.defaults["image_id"]
        image_objects = self.ibm_vpc_client.list_images().get_result()["images"]
        if image_id:
            image_obj = find_obj(image_objects, "dummy", obj_id=image_id)
        else:
            # find first occurrence
            image_obj = next((obj for obj in image_objects if "ibm-ubuntu-20-04-" in obj["name"]), None)

        return image_obj["id"], image_obj["minimum_provisioned_size"], image_obj["owner_type"] == "user"

    @update_decorator
    def create_default(self):
        image_objects = self.ibm_vpc_client.list_images().get_result()["images"]

        image_obj = next((image for image in image_objects if "ibm-ubuntu-20-04-" in image["name"]), None)

        print(f'Selected \033[92mUbuntu\033[0m 20.04 VM image, {image_obj["name"]}')
        return image_obj["id"], image_obj["minimum_provisioned_size"], image_obj["owner_type"] == "user"
