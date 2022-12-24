from typing import Any, Dict
from skyplane.compute.ibmcloud.gen2.utils import get_region_by_endpoint
from skyplane.compute.ibmcloud.gen2.config_builder import ConfigBuilder, update_decorator, spinner


class SkyEndpointConfig(ConfigBuilder):
    def __init__(self, base_config: Dict[str, Any]) -> None:
        super().__init__(base_config)

        base_endpoint = self.base_config["provider"].get("endpoint")
        try:
            # when endpoint was provided directly by user instead of selecting it
            # we just set it
            if base_endpoint:
                ConfigBuilder.ibm_vpc_client.set_service_url(base_endpoint + "/v1")
                self.ibm_vpc_client.set_service_url(base_endpoint + "/v1")

            self.defaults["region"] = get_region_by_endpoint(base_endpoint) if base_endpoint else None
        except Exception:
            pass

    @spinner
    def _get_regions_objects(self):
        return ConfigBuilder.ibm_vpc_client.list_regions().get_result()["regions"]

    def update_config(self, endpoint):
        self.base_config["provider"]["endpoint"] = endpoint
        self.base_config["provider"]["region"] = ConfigBuilder.region

    @update_decorator
    def create_default(self):
        # update global ibm_vpc_client to selected endpoint
        regions_objects = self._get_regions_objects()

        # currently hardcoded for us-south
        region_obj = next((r for r in regions_objects if r["name"] == "us-south"), None)

        ConfigBuilder.ibm_vpc_client.set_service_url(region_obj["endpoint"] + "/v1")
        ConfigBuilder.region = region_obj["name"]

        return region_obj["endpoint"]
