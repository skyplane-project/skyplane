from dataclasses import dataclass
import os
from pathlib import Path
from typing import Optional
import subprocess
from skylark.compute.azure.azure_auth import AzureAuthentication
from skylark.compute.gcp.gcp_auth import GCPAuthentication

from skylark.utils import logger
import configparser

from skylark import config_path


@dataclass
class SkylarkConfig:
    is_inferred_credential: bool = False
    azure_subscription_id: Optional[str] = None
    gcp_project_id: Optional[str] = None

    @staticmethod
    def load_config(path) -> "SkylarkConfig":
        """Load from a config file."""
        path = Path(config_path)
        config = configparser.ConfigParser()
        if not path.exists():
            logger.error(f"Config file not found: {path}")
            raise FileNotFoundError(f"Config file not found: {path}")
        config.read(path)

        azure_subscription_id = None
        if "azure" in config and "subscription_id" in config["azure"]:
            azure_subscription_id = config.get("azure", "subscription_id")

        gcp_project_id = None
        if "gcp" in config and "project_id" in config["gcp"]:
            gcp_project_id = config.get("gcp", "project_id")

        return SkylarkConfig(
            is_inferred_credential=False,
            azure_subscription_id=azure_subscription_id,
            gcp_project_id=gcp_project_id,
        )

    @staticmethod
    def load_infer_cli() -> "SkylarkConfig":
        """Attempt to infer configuration using cloud CLIs."""
        azure_subscription_id = AzureAuthentication.infer_subscription_id()
        gcp_project_id = GCPAuthentication.infer_project_id()
        return SkylarkConfig(
            is_inferred_credential=True,
            azure_subscription_id=azure_subscription_id,
            gcp_project_id=gcp_project_id,
        )

    def to_config_file(self, path):
        assert not self.is_inferred_credential, "Cannot write inferred config to file"
        path = Path(path)
        config = configparser.ConfigParser()
        if path.exists():
            config.read(os.path.expanduser(path))

        if self.azure_subscription_id:
            if "azure" not in config:
                config.add_section("azure")
            config.set("azure", "subscription_id", self.azure_subscription_id)

        if self.gcp_project_id:
            if "gcp" not in config:
                config.add_section("gcp")
            config.set("gcp", "project_id", self.gcp_project_id)

        with path.open("w") as f:
            config.write(f)
