import os
from pathlib import Path

from skyplane import exceptions as skyplane_exceptions
from skyplane.compute.aws.aws_auth import AWSAuthentication
from skyplane.compute.server import key_root
from skyplane.utils import logger


class AWSKeyManager:
    """Stores SSH keys for access to AWS VMs."""

    def __init__(self, auth: AWSAuthentication, local_key_dir: Path = key_root / "aws"):
        self.auth = auth
        self.local_key_dir = local_key_dir

    def key_exists_aws(self, aws_region: str, key_name: str) -> bool:
        """Checks if a key exists in AWS."""
        ec2_client = self.auth.get_boto3_client("ec2", aws_region)
        return key_name in set(p["KeyName"] for p in ec2_client.describe_key_pairs()["KeyPairs"])

    def key_exists_local(self, key_name: str) -> bool:
        """Checks if a key exists locally."""
        return (self.local_key_dir / f"{key_name}.pem").exists()

    def make_key(self, aws_region: str, key_name: str) -> Path:
        """Creates a key in AWS and stores it locally."""
        if self.key_exists_aws(aws_region, key_name):
            logger.error(f"Key {key_name} already exists in AWS region {aws_region}")
            raise skyplane_exceptions.PermissionsException(
                f"Key {key_name} already exists in AWS region {aws_region}, please delete it first or use a different key name."
            )
        if self.key_exists_local(key_name):
            logger.error(f"Key {key_name} already exists locally")
            raise skyplane_exceptions.PermissionsException(
                f"Key {key_name} already exists locally, please delete it first or use a different key name."
            )
        ec2 = self.auth.get_boto3_resource("ec2", aws_region)
        local_key_file = self.local_key_dir / f"{key_name}.pem"
        local_key_file.parent.mkdir(parents=True, exist_ok=True)
        logger.fs.debug(f"[AWS] Creating keypair {key_name} in {aws_region}")
        key_pair = ec2.create_key_pair(KeyName=key_name, KeyType="rsa")
        with local_key_file.open("w") as f:
            key_str = key_pair.key_material
            if not key_str.endswith("\n"):
                key_str += "\n"
            f.write(key_str)
        os.chmod(local_key_file, 0o600)
        return local_key_file

    def delete_key(self, aws_region: str, key_name: str):
        """Deletes a key from AWS and locally."""
        if self.key_exists_aws(aws_region, key_name):
            ec2 = self.auth.get_boto3_resource("ec2", aws_region)
            logger.fs.debug(f"[AWS] Deleting keypair {key_name} in {aws_region}")
            ec2.KeyPair(key_name).delete()
        if self.key_exists_local(key_name):
            (self.local_key_dir / f"{key_name}.pem").unlink()

    def get_key(self, key_name: str) -> Path:
        """Returns path to local keyfile."""
        return self.local_key_dir / f"{key_name}.pem"

    def ensure_key_exists(self, aws_region: str, key_name: str, delete_remote: bool = True) -> Path:
        """Ensures that a key exists in AWS and locally, creating it if necessary. Raise an exception if it's on AWS and not locally."""
        local_exists, remote_exists = self.key_exists_local(key_name), self.key_exists_aws(aws_region, key_name)
        if local_exists and remote_exists:
            return self.get_key(key_name)
        elif not local_exists and not remote_exists:
            return self.make_key(aws_region, key_name)
        elif local_exists and not remote_exists:
            local_key_path = self.get_key(key_name)
            logger.warning(f"Key {key_name} exists locally but not in AWS region {aws_region}. Moving the local key {local_key_path}.bak")
            local_key_path.rename(local_key_path.with_suffix(".pem.bak"))
            return self.make_key(aws_region, key_name)
        else:
            if delete_remote:
                logger.warning(f"Key {key_name} exists in AWS region {aws_region} but not locally. Deleting the remote key.")
                self.delete_key(aws_region, key_name)
                return self.make_key(aws_region, key_name)
            else:
                raise skyplane_exceptions.PermissionsException(
                    f"Key {key_name} exists in AWS region {aws_region} but not locally. Please delete the key from AWS or move it locally."
                )
