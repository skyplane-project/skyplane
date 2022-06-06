import configparser
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

_FLAG_TYPES = {
    "autoconfirm": bool,
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
    azure_subscription_id: Optional[str] = None
    gcp_project_id: Optional[str] = None

    # skyplane flags
    flag_autoconfirm: bool = False

    @staticmethod
    def default_config() -> "SkyplaneConfig":
        return SkyplaneConfig(
            aws_enabled=False,
            azure_enabled=False,
            gcp_enabled=False,
            azure_subscription_id=None,
            gcp_project_id=None,
            flag_autoconfirm=False,
        )

    @staticmethod
    def load_config(path) -> "SkyplaneConfig":
        """Load from a config file."""
        path = Path(path)
        config = configparser.ConfigParser()
        if not path.exists():
            raise FileNotFoundError(f"Config file not found: {path}")
        config.read(path)

        aws_enabled = False
        if "aws" in config:
            if "aws_enabled" in config["aws"]:
                aws_enabled = config.getboolean("aws", "aws_enabled")

        azure_enabled = False
        azure_subscription_id = None
        if "azure" in config:
            if "azure_enabled" in config["azure"]:
                azure_enabled = config.getboolean("azure", "azure_enabled")
            if "subscription_id" in config["azure"]:
                azure_subscription_id = config.get("azure", "subscription_id")

        gcp_enabled = False
        gcp_project_id = None
        if "gcp" in config:
            if "gcp_enabled" in config["gcp"]:
                gcp_enabled = config.getboolean("gcp", "gcp_enabled")
            if "project_id" in config["gcp"]:
                gcp_project_id = config.get("gcp", "project_id")

        flag_autoconfirm = False
        if "flags" in config:
            if "autoconfirm" in config["flags"]:
                flag_autoconfirm = config.getboolean("flags", "autoconfirm")

        return SkyplaneConfig(
            aws_enabled=aws_enabled,
            azure_enabled=azure_enabled,
            gcp_enabled=gcp_enabled,
            azure_subscription_id=azure_subscription_id,
            gcp_project_id=gcp_project_id,
            flag_autoconfirm=flag_autoconfirm,
        )

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

        if "gcp" not in config:
            config.add_section("gcp")
        config.set("gcp", "gcp_enabled", str(self.gcp_enabled))

        if self.gcp_project_id:
            config.set("gcp", "project_id", self.gcp_project_id)

        if "flags" not in config:
            config.add_section("flags")
        config.set("flags", "autoconfirm", str(self.flag_autoconfirm))

        with path.open("w") as f:
            config.write(f)

    def valid_flags(self):
        return list(_FLAG_TYPES.keys())

    def get_flag(self, flag_name):
        if flag_name not in self.valid_flags():
            raise KeyError(f"Invalid flag: {flag_name}")
        return getattr(self, f"flag_{flag_name}")

    def set_flag(self, flag_name, value):
        if flag_name not in self.valid_flags():
            raise KeyError(f"Invalid flag: {flag_name}")
        setattr(self, f"flag_{flag_name}", _map_type(value, _FLAG_TYPES.get(flag_name, str)))
