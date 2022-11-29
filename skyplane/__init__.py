from pathlib import Path

from skyplane import compute
from skyplane.api.client import SkyplaneClient
from skyplane.api.config import TransferConfig, AWSConfig, AzureConfig, GCPConfig
from skyplane.api.provision.dataplane import Dataplane

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
