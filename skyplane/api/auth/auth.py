from skyplane.api.auth.auth_config import AuthenticationConfig
from typing import Optional


class Auth:
    def __init__(
        self,
        aws: Optional[AuthenticationConfig] = None,
        gcp: Optional[AuthenticationConfig] = None,
        azure: Optional[AuthenticationConfig] = None,
    ):
        self.aws_auth_config = aws
        self.gcp_auth_config = gcp
        self.azure_auth_config = azure

    def load_config(path: str):
        raise NotImplementedError
