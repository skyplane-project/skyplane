from dataclasses import dataclass
import functools
import os
from pathlib import Path
from typing import Optional

from skylark.utils import logger
import configparser

from skylark import config_path


def load_config():
    raise NotImplementedError()


@dataclass
class SkylarkConfig:
    aws_enabled: bool = False
    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None

    azure_enabled: bool = False
    azure_tenant_id: Optional[str] = None
    azure_client_id: Optional[str] = None
    azure_client_secret: Optional[str] = None
    azure_subscription_id: Optional[str] = None

    gcp_enabled: bool = False
    gcp_project_id: Optional[str] = None
    gcp_application_credentials_file: Optional[str] = None

    @staticmethod
    def load() -> "SkylarkConfig":
        if config_path.exists():
            config = SkylarkConfig.load_from_config_file(config_path)
        else:
            config = SkylarkConfig()

        # set environment variables
        if config.gcp_enabled:
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = config.gcp_application_credentials_file

        return config

    @staticmethod
    @functools.lru_cache
    def load_from_config_file(path=config_path) -> "SkylarkConfig":
        path = Path(config_path)
        config = configparser.ConfigParser()
        if not path.exists():
            logger.error(f"Config file not found: {path}")
            raise FileNotFoundError(f"Config file not found: {path}")
        config.read(path)

        if "aws" in config:
            aws_enabled = True
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

        if self.aws_enabled:
            if "aws" not in config:
                config.add_section("aws")
            config.set("aws", "access_key_id", self.aws_access_key_id)
            config.set("aws", "secret_access_key", self.aws_secret_access_key)
        else:
            config.remove_section("aws")

        if self.azure_enabled:
            if "azure" not in config:
                config.add_section("azure")
            config.set("azure", "tenant_id", self.azure_tenant_id)
            config.set("azure", "client_id", self.azure_client_id)
            config.set("azure", "client_secret", self.azure_client_secret)
            config.set("azure", "subscription_id", self.azure_subscription_id)
        else:
            config.remove_section("azure")

        if self.gcp_enabled:
            if "gcp" not in config:
                config.add_section("gcp")
            config.set("gcp", "project_id", self.gcp_project_id)
            config.set("gcp", "application_credentials_file", self.gcp_application_credentials_file)
        else:
            config.remove_section("gcp")

        with path.open("w") as f:
            config.write(f)
