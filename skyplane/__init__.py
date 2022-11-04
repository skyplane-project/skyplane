from pathlib import Path

from skyplane import compute
from skyplane.api.auth_config import AWSConfig, AzureConfig, GCPConfig
from skyplane.api.client import SkyplaneClient
from skyplane.api.dataplane import Dataplane
from skyplane.api.transfer_config import TransferConfig

__version__ = "0.2.1"
__root__ = Path(__file__).parent.parent
__all__ = [
    "__version__",
    "__root__",
    # modules
    "compute",
    # API
    "SkyplaneClient",
    "Dataplane",
    "TransferConfig",
    "AWSConfig",
    "AzureConfig",
    "GCPConfig",
]
