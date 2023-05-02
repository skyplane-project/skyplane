import functools
import os
from pathlib import Path


__config_root__ = Path("~/.skyplane").expanduser()
aws_config_path = __config_root__ / "aws_config"
aws_quota_path = __config_root__ / "aws_quota"
azure_config_path = __config_root__ / "azure_config"
azure_quota_path = __config_root__ / "azure_quota"
azure_standardDv5_quota_path = __config_root__ / "azure_standardDv5_quota"
azure_sku_path = __config_root__ / "azure_sku_mapping"
gcp_config_path = __config_root__ / "gcp_config"
ibmcloud_config_path = __config_root__ / "ibmcloud_config"
gcp_quota_path = __config_root__ / "gcp_quota"


@functools.lru_cache(maxsize=None)
def load_config_path():
    if "SKYPLANE_CONFIG" in os.environ:
        path = Path(os.environ["SKYPLANE_CONFIG"]).expanduser()
    else:
        path = __config_root__ / "config"
    path.parent.mkdir(exist_ok=True)
    return path


@functools.lru_cache(maxsize=None)
def load_cloud_config(path):
    from skyplane.config import SkyplaneConfig

    if path.exists():
        return SkyplaneConfig.load_config(path)
    else:
        return SkyplaneConfig.default_config()


config_path = load_config_path()
cloud_config = load_cloud_config(config_path)
host_uuid_path = __config_root__ / "host_uuid"
