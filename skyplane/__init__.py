from pathlib import Path

from skyplane.api.client import SkyplaneClient
from skyplane.api.dataplane import Dataplane
from skyplane.api.transfer_config import TransferConfig
from skyplane.api.auth_config import AWSConfig, AzureConfig, GCPConfig

# version
__version__ = "0.2.1"

# paths
__root__ = Path(__file__).parent.parent
__config_root__ = Path("~/.skyplane").expanduser()
__config_root__.mkdir(exist_ok=True)

__all__ = [
    "__root__",
    "__config_root__",
    "__version__",
    "SkyplaneClient",
    "Dataplane",
    "TransferConfig",
    "AWSConfig",
    "AzureConfig",
    "GCPConfig",
]
