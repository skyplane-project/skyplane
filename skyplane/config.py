import configparser
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from skyplane.exceptions import BadConfigException

_FLAG_TYPES = {
    "autoconfirm": bool,
    "bbr": bool,
    "compress": bool,
    "encrypt_e2e": bool,
    "encrypt_socket_tls": bool,
    "verify_checksums": bool,
    "multipart_enabled": bool,
    "multipart_min_threshold_mb": int,
    "multipart_min_size_mb": int,
    "multipart_max_chunks": int,
    "num_connections": int,
    "max_instances": int,
    "autoshutdown_minutes": int,
    "aws_use_spot_instances": bool,
    "azure_use_spot_instances": bool,
    "gcp_use_spot_instances": bool,
    "aws_instance_class": str,
    "azure_instance_class": str,
    "gcp_instance_class": str,
    "gcp_use_premium_network": bool,
    "usage_stats": bool,
    "gcp_service_account_name": str,
}

_DEFAULT_FLAGS = {
    "autoconfirm": False,
    "bbr": True,
    "compress": True,
    "encrypt_e2e": True,
    "encrypt_socket_tls": False,
    "verify_checksums": True,
    "multipart_enabled": True,
    "multipart_min_threshold_mb": 128,
    "multipart_min_size_mb": 8,
    "multipart_max_chunks": 9990,  # AWS limit is 10k chunks
    "num_connections": 32,
    "max_instances": 1,
    "autoshutdown_minutes": 15,
    "aws_use_spot_instances": False,
    "azure_use_spot_instances": False,
    "gcp_use_spot_instances": False,
    "aws_instance_class": "m5.8xlarge",
    "azure_instance_class": "Standard_D32_v5",
    "gcp_instance_class": "n2-standard-32",
    "gcp_use_premium_network": True,
    "usage_stats": True,
    "gcp_service_account_name": "skyplane-manual",
}


def _map_type(value, val_type):
    if val_type is bool:
        if value.lower() in ["true", "yes", "1"]:
            return True
        elif value.lower() in ["false", "no", "0"]:
            return False
        else:
            raise ValueError(f"Invalid boolean value: {value}")
    else:
        return val_type(value)


@dataclass
class SkyplaneConfig:
    aws_enabled: bool
    azure_enabled: bool
    gcp_enabled: bool
    azure_principal_id: Optional[str] = None
    azure_subscription_id: Optional[str] = None
    azure_tenant_id: Optional[str] = None
    azure_client_id: Optional[str] = None
    azure_client_secret: Optional[str] = None
    gcp_project_id: Optional[str] = None
    anon_clientid: Optional[str] = None

    @staticmethod
    def default_config() -> "SkyplaneConfig":
        return SkyplaneConfig(
            aws_enabled=False,
            azure_enabled=False,
            gcp_enabled=False,
            anon_clientid=None,
        )

    @staticmethod
    def load_config(path) -> "SkyplaneConfig":
        """Load from a config file."""
        path = Path(path)
        config = configparser.ConfigParser()
        if not path.exists():
            raise FileNotFoundError(f"Config file not found: {path}")
        config.read(path)

        anon_clientid = None
        if "client" in config and "anon_clientid" in config["client"]:
            anon_clientid = config.get("client", "anon_clientid")

        aws_enabled = False
        if "aws" in config:
            if "aws_enabled" in config["aws"]:
                aws_enabled = config.getboolean("aws", "aws_enabled")

        azure_enabled = False
        azure_subscription_id = None
        azure_tenant_id = None
        azure_client_id = None
        azure_client_secret = None
        azure_principal_id = None
        if "azure" in config:
            if "azure_enabled" in config["azure"]:
                azure_enabled = config.getboolean("azure", "azure_enabled")
            if "subscription_id" in config["azure"]:
                azure_subscription_id = config.get("azure", "subscription_id")
            if "tenant_id" in config["azure"]:
                azure_tenant_id = config.get("azure", "tenant_id")
            if "client_id" in config["azure"]:
                azure_client_id = config.get("azure", "client_id")
            if "client_secret" in config["azure"]:
                azure_client_secret = config.get("azure", "client_secret")
            if "principal_id" in config["azure"]:
                azure_principal_id = config.get("azure", "principal_id")

        gcp_enabled = False
        gcp_project_id = None
        if "gcp" in config:
            if "gcp_enabled" in config["gcp"]:
                gcp_enabled = config.getboolean("gcp", "gcp_enabled")
            if "project_id" in config["gcp"]:
                gcp_project_id = config.get("gcp", "project_id")

        skyplane_config = SkyplaneConfig(
            aws_enabled=aws_enabled,
            azure_enabled=azure_enabled,
            gcp_enabled=gcp_enabled,
            anon_clientid=anon_clientid,
            azure_principal_id=azure_principal_id,
            azure_subscription_id=azure_subscription_id,
            azure_tenant_id=azure_tenant_id,
            azure_client_id=azure_client_id,
            azure_client_secret=azure_client_secret,
            gcp_project_id=gcp_project_id,
        )

        if "flags" in config:
            for flag_name in _FLAG_TYPES:
                if flag_name in config["flags"]:
                    skyplane_config.set_flag(flag_name, config["flags"][flag_name])

        return skyplane_config

    def to_config_file(self, path):
        path = Path(path)
        config = configparser.ConfigParser()
        if path.exists():
            config.read(os.path.expanduser(path))

        if "aws" not in config:
            config.add_section("aws")
        config.set("aws", "aws_enabled", str(self.aws_enabled))

        if "azure" not in config:
            config.add_section("azure")
        config.set("azure", "azure_enabled", str(self.azure_enabled))

        if self.azure_subscription_id:
            config.set("azure", "subscription_id", self.azure_subscription_id)
        if self.azure_tenant_id:
            config.set("azure", "tenant_id", self.azure_tenant_id)
        if self.azure_client_id:
            config.set("azure", "client_id", self.azure_client_id)
        if self.azure_client_secret:
            config.set("azure", "client_secret", self.azure_client_secret)
        if self.azure_principal_id:
            config.set("azure", "principal_id", self.azure_principal_id)

        if "gcp" not in config:
            config.add_section("gcp")
        config.set("gcp", "gcp_enabled", str(self.gcp_enabled))

        if self.gcp_project_id:
            config.set("gcp", "project_id", self.gcp_project_id)

        if "client" not in config:
            config.add_section("client")
        if self.anon_clientid:
            config.set("client", "anon_clientid", self.anon_clientid)

        if "flags" not in config:
            config.add_section("flags")

        for flag_name in _FLAG_TYPES:
            val = getattr(self, f"flag_{flag_name}", None)
            if val is not None:
                config.set("flags", flag_name, str(val))
            else:
                if "flags" in config and flag_name in config["flags"]:
                    config.remove_option("flags", flag_name)

        with path.open("w") as f:
            config.write(f)

    def valid_flags(self):
        return list(_FLAG_TYPES.keys())

    def get_flag(self, flag_name):
        if flag_name not in self.valid_flags():
            raise KeyError(f"Invalid flag: {flag_name}")
        return getattr(self, f"flag_{flag_name}", _DEFAULT_FLAGS[flag_name])

    def set_flag(self, flag_name, value: Optional[Any]):
        if flag_name not in self.valid_flags():
            raise KeyError(f"Invalid flag: {flag_name}")
        if value is not None:
            setattr(self, f"flag_{flag_name}", _map_type(value, _FLAG_TYPES.get(flag_name, str)))
        else:
            setattr(self, f"flag_{flag_name}", None)

    def check_config(self):
        valid_config = True
        if self.anon_clientid is None:
            valid_config = False
        if self.azure_enabled and (self.azure_client_id is None or self.azure_principal_id is None or self.azure_subscription_id is None):
            valid_config = False
        if self.gcp_enabled and self.gcp_project_id is None:
            valid_config = False
        if not valid_config:
            raise BadConfigException("Invalid configuration")
