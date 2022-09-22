import os
from pathlib import Path

from skyplane.config import SkyplaneConfig
from skyplane.gateway_version import gateway_version

# version
__version__ = "0.2.0"

# paths
skyplane_root = Path(__file__).parent.parent
config_root = Path("~/.skyplane").expanduser()
config_root.mkdir(exist_ok=True)

if "SKYPLANE_CONFIG" in os.environ:
    config_path = Path(os.environ["SKYPLANE_CONFIG"]).expanduser()
else:
    config_path = config_root / "config"

aws_config_path = config_root / "aws_config"
azure_config_path = config_root / "azure_config"
azure_sku_path = config_root / "azure_sku_mapping"
gcp_config_path = config_root / "gcp_config"

key_root = config_root / "keys"
tmp_log_dir = Path("/tmp/skyplane")
tmp_log_dir.mkdir(exist_ok=True)

# definitions
KB = 1024
MB = 1024 * 1024
GB = 1024 * 1024 * 1024


def format_bytes(bytes: int):
    if bytes < KB:
        return f"{bytes}B"
    elif bytes < MB:
        return f"{bytes / KB:.2f}KB"
    elif bytes < GB:
        return f"{bytes / MB:.2f}MB"
    else:
        return f"{bytes / GB:.2f}GB"


if config_path.exists():
    cloud_config = SkyplaneConfig.load_config(config_path)
else:
    cloud_config = SkyplaneConfig.default_config()
is_gateway_env = os.environ.get("SKYPLANE_IS_GATEWAY", None) == "1"

# load gateway docker image version
def gateway_docker_image():
    return "public.ecr.aws/s6m1p0n8/skyplane:" + gateway_version
