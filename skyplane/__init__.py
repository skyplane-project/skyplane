from pathlib import Path

# version
__version__ = "0.2.1"

# paths
__root__ = Path(__file__).parent.parent
__config_root__ = Path("~/.skyplane").expanduser()
__config_root__.mkdir(exist_ok=True)
tmp_log_dir = Path("/tmp/skyplane")

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
