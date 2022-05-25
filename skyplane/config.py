import configparser
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


@dataclass
class SkyplaneConfig:
    aws_enabled: bool
    azure_enabled: bool
    gcp_enabled: bool
    azure_subscription_id: Optional[str] = None
    gcp_project_id: Optional[str] = None

    @staticmethod
    def default_config() -> "SkyplaneConfig":
        return SkyplaneConfig(
            aws_enabled=False,
            azure_enabled=False,
            gcp_enabled=False,
            azure_subscription_id=None,
            gcp_project_id=None,
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

        return SkyplaneConfig(
            aws_enabled=aws_enabled,
            azure_enabled=azure_enabled,
            gcp_enabled=gcp_enabled,
            azure_subscription_id=azure_subscription_id,
            gcp_project_id=gcp_project_id,
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

        with path.open("w") as f:
            config.write(f)
