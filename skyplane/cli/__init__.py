import functools
import os
from pathlib import Path

from skyplane import __config_root__
from skyplane.config import SkyplaneConfig


@functools.lru_cache
def load_config_path():
    if "SKYPLANE_CONFIG" in os.environ:
        return Path(os.environ["SKYPLANE_CONFIG"]).expanduser()
    else:
        return __config_root__ / "config"


@functools.lru_cache
def load_cloud_config(path):
    if path.exists():
        return SkyplaneConfig.load_config(path)
    else:
        return SkyplaneConfig.default_config()


config_path = load_config_path()
cloud_config = load_cloud_config(config_path)
