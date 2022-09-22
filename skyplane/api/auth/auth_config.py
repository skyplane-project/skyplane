from typing import Type, Optional

from skyplane.compute.aws.aws_auth_provider import AWSAuthenticationProvider
from skyplane.compute.cloud_auth_provider import CloudAuthenticationProvider


class AuthenticationConfig:
    def make_auth_provider(self) -> Type[CloudAuthenticationProvider]:
        raise NotImplementedError


class AWSAuthenticationConfig(AuthenticationConfig):
    def __init__(self, access_key_id: Optional[str], secret_access_key: Optional[str]):
        self.access_key_id = access_key_id
        self.secret_access_key = secret_access_key

    def make_auth_provider(self) -> AWSAuthenticationProvider:
        return AWSAuthenticationProvider(access_key=self.access_key_id, secret_key=self.secret_access_key)
