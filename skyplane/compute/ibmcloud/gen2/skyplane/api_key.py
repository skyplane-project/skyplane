from typing import Any, Dict
from skyplane.compute.ibmcloud.gen2.config_builder import ConfigBuilder
from skyplane.compute.ibmcloud.gen2.utils import verify_iam_api_key


class SkyApiKeyConfig(ConfigBuilder):
    def __init__(self, base_config: Dict[str, Any]) -> None:
        super().__init__(base_config)
        self.base_config.pop("ibm", None)
        self.defaults["api_key"] = self.base_config["provider"]["iam_api_key"] if self.base_config.setdefault("provider", {}) else None

    def update_config(self, iam_api_key, compute_iam_endpoint=None):
        self.base_config["provider"]["iam_api_key"] = iam_api_key

        if compute_iam_endpoint:
            self.base_config["provider"]["iam_endpoint"] = compute_iam_endpoint

        self.base_config.pop("ibm", None)
        return self.base_config

    def verify(self, base_config):
        api_key = base_config["provider"]["iam_api_key"]

        verify_iam_api_key(None, api_key)
        ConfigBuilder.iam_api_key = api_key

        return base_config
