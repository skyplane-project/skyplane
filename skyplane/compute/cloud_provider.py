import functools

from typing import List, Optional, Union

from skyplane.compute.server import Server, ServerState
from skyplane.utils.fn import do_parallel
from skyplane.utils import logger


class CloudProvider:
    logging_enabled = True  # For Dozzle
    log_viewer_port = 8888

    @property
    def name(self):
        raise NotImplementedError

    @staticmethod
    def region_list():
        raise NotImplementedError

    @staticmethod
    @functools.lru_cache(maxsize=None)
    def get_transfer_cost(src_key, dst_key, premium_tier=True):
        src_provider, _ = src_key.split(":")
        if src_key == dst_key or src_provider == "cos":
            return 0.0
        elif src_provider == "cloudflare":
            return 0.0  # TODO: fix this for other clouds
        if src_provider == "aws":
            from skyplane.compute.aws.aws_cloud_provider import AWSCloudProvider

            return AWSCloudProvider.get_transfer_cost(src_key, dst_key, premium_tier)
        elif src_provider == "gcp":
            from skyplane.compute.gcp.gcp_cloud_provider import GCPCloudProvider

            return GCPCloudProvider.get_transfer_cost(src_key, dst_key, premium_tier)
        elif src_provider == "azure":
            from skyplane.compute.azure.azure_cloud_provider import AzureCloudProvider

            return AzureCloudProvider.get_transfer_cost(src_key, dst_key, premium_tier)
        elif src_provider == "ibmcloud":
            logger.warning("IBM cloud costs are not yet supported.")
            return 0
        elif src_provider == "hdfs":
            from skyplane.compute.gcp.gcp_cloud_provider import GCPCloudProvider

            return GCPCloudProvider.get_transfer_cost(f"gcp:{_}", dst_key, premium_tier)
        else:
            raise ValueError(f"Unknown provider {src_provider}")

    def get_instance_list(self, region) -> List[Server]:
        raise NotImplementedError

    def get_matching_instances(
        self,
        region: Optional[str] = None,
        instance_type: Optional[str] = None,
        state: Optional[Union[ServerState, List[ServerState]]] = None,
        tags={"skyplane": "true"},
    ) -> List[Server]:
        if isinstance(region, str):
            results = [self.get_instance_list(region)]
        elif region is None:
            n = -1 if self.name != "azure" else 16  # avoid rate limit for azure
            results = do_parallel(self.get_instance_list, self.region_list(), n=n, return_args=False)

        matching_instances = []
        for instances in results:
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

    def provision_instance(
        self,
        region: str,
        instance_class: str,
        disk_size: int = 32,
        use_spot_instances: bool = False,
        name: Optional[str] = None,
        tags={"skyplane": "true"},
        **kwargs,
    ) -> Server:
        raise NotImplementedError

    def setup_global(self, **kwargs):
        pass

    def setup_region(self, region: str):
        pass

    def teardown_global(self):
        pass
