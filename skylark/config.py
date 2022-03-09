from dataclasses import dataclass
import functools
import os
from pathlib import Path
from typing import Optional

from azure.common.credentials import DefaultAzureCredential
from skylark.utils import logger
import configparser

from skylark import config_path



@dataclass
class SkylarkConfig:
    aws_config_mode: str = "disabled"  # disabled, iam_manual
    aws_access_key_id: Optional[str] = None  # iam_manual
    aws_secret_access_key: Optional[str] = None  # iam_manual

    azure_config_mode: str = "disabled"  # disabled, cli_auto
    azure_subscription_id: Optional[str] = None

    gcp_config_mode: str = "disabled"  # disabled or service_account_file
    gcp_project_id: Optional[str] = None
    gcp_application_credentials_file: Optional[str] = None  # service_account_file

    @property
    def aws_enabled(self) -> bool:
        return self.aws_config_mode != "disabled"
    
    @property
    def azure_enabled(self) -> bool:
        return self.azure_config_mode != "disabled"
    
    @property
    def gcp_enabled(self) -> bool:
        return self.gcp_config_mode != "disabled"

    @staticmethod
    def load_from_config_file(path=config_path) -> "SkylarkConfig":
        path = Path(config_path)
        config = configparser.ConfigParser()
        if not path.exists():
            logger.error(f"Config file not found: {path}")
            raise FileNotFoundError(f"Config file not found: {path}")
        config.read(path)

        if "aws" in config:
            aws_access_key_id = config.get("aws", "access_key_id")
            aws_secret_access_key = config.get("aws", "secret_access_key")
        else:
            aws_enabled = False
            aws_access_key_id = None
            aws_secret_access_key = None

        if "azure" in config:
            azure_enabled = True
            azure_tenant_id = config.get("azure", "tenant_id")
            azure_client_id = config.get("azure", "client_id")
            azure_client_secret = config.get("azure", "client_secret")
            azure_subscription_id = config.get("azure", "subscription_id")
        else:
            azure_enabled = False
            azure_tenant_id = None
            azure_client_id = None
            azure_client_secret = None
            azure_subscription_id = None

        if "gcp" in config:
            gcp_enabled = True
            gcp_project_id = config.get("gcp", "project_id")
            gcp_application_credentials_file = config.get("gcp", "application_credentials_file")
        else:
            gcp_enabled = False
            gcp_project_id = None
            gcp_application_credentials_file = None

        return SkylarkConfig(
            aws_enabled=aws_enabled,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            azure_enabled=azure_enabled,
            azure_tenant_id=azure_tenant_id,
            azure_client_id=azure_client_id,
            azure_client_secret=azure_client_secret,
            azure_subscription_id=azure_subscription_id,
            gcp_enabled=gcp_enabled,
            gcp_project_id=gcp_project_id,
            gcp_application_credentials_file=gcp_application_credentials_file,
        )

    def to_config_file(self, path=config_path):
        path = Path(path)
        config = configparser.ConfigParser()
        if path.exists():
            config.read(os.path.expanduser(path))

        if "aws" not in config:
            config.add_section("aws")
        config.set("aws", "config_mode", self.aws_config_mode)
        if self.aws_config_mode == "iam_manual":
            config.set("aws", "access_key_id", self.aws_access_key_id)
            config.set("aws", "secret_access_key", self.aws_secret_access_key)

        if "azure" not in config:
            config.add_section("azure")
        config.set("azure", "config_mode", self.azure_config_mode)
        if self.azure_config_mode == "cli_auto":
            config.set("azure", "subscription_id", self.azure_subscription_id)
        elif self.azure_config_mode == "ad_manual":
            config.set("azure", "tenant_id", self.azure_tenant_id)
            config.set("azure", "client_id", self.azure_client_id)
            config.set("azure", "client_secret", self.azure_client_secret)
            config.set("azure", "subscription_id", self.azure_subscription_id)

        if "gcp" not in config:
            config.add_section("gcp")
        config.set("gcp", "config_mode", self.gcp_config_mode)
        if self.gcp_config_mode == "service_account_file":
            config.set("gcp", "project_id", self.gcp_project_id)
            config.set("gcp", "application_credentials_file", self.gcp_application_credentials_file)

        with path.open("w") as f:
            config.write(f)
