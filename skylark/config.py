import json
import os
from skylark import config_file

from skylark.utils import logger


def load_config():
    if config_file.exists():
        try:
            with config_file.open("r") as f:
                config = json.load(f)
            if "aws_access_key_id" in config:
                os.environ["AWS_ACCESS_KEY_ID"] = config["aws_access_key_id"]
            if "aws_secret_access_key" in config:
                os.environ["AWS_SECRET_ACCESS_KEY"] = config["aws_secret_access_key"]
            if "azure_tenant_id" in config:
                os.environ["AZURE_TENANT_ID"] = config["azure_tenant_id"]
            if "azure_client_id" in config:
                os.environ["AZURE_CLIENT_ID"] = config["azure_client_id"]
            if "azure_client_secret" in config:
                os.environ["AZURE_CLIENT_SECRET"] = config["azure_client_secret"]
            if "gcp_application_credentials_file" in config:
                os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = config["gcp_application_credentials_file"]

            return config
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding config file: {e}")
            raise e
    return {}
