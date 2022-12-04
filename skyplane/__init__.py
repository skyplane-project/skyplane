from pathlib import Path

from skyplane import compute, exceptions
from skyplane.api.client import SkyplaneClient
from skyplane.api.config import TransferConfig, AWSConfig, AzureConfig, GCPConfig
from skyplane.api.dataplane import Dataplane
from skyplane.api.tracker import TransferHook

__version__ = "0.2.1"
__root__ = Path(__file__).parent.parent
__all__ = [
    "__version__",
    "__root__",
    # modules
    "compute",
    "exceptions",
    # API
    "SkyplaneClient",
    "Dataplane",
    "TransferConfig",
    "AWSConfig",
    "AzureConfig",
    "GCPConfig",
    "TransferHook",
]
