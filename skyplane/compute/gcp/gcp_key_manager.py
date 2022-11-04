from pathlib import Path

from skyplane import exceptions as skyplane_exceptions
from skyplane.compute.key_utils import generate_keypair
from skyplane.compute.server import key_root
from skyplane.utils import logger


class GCPKeyManager:
    """Stores SSH keys for access to GCP VMs."""

    def __init__(self, local_key_dir: Path = key_root / "gcp"):
        self.local_key_dir = local_key_dir

    def get_private_key(self, key_name: str) -> Path:
        """Returns path to local keyfile."""
        return self.local_key_dir / f"{key_name}.pem"

    def get_public_key(self, key_name: str) -> Path:
        """Returns path to local public keyfile."""
        return self.local_key_dir / f"{key_name}.pub"

    def key_exists_local(self, key_name: str) -> bool:
        """Checks if a key exists locally."""
        private_key_exists = self.get_private_key(key_name).exists() and self.get_private_key(key_name).is_file()
        public_key_exists = self.get_public_key(key_name).exists() and self.get_public_key(key_name).is_file()
        return private_key_exists and public_key_exists

    def make_key_local(self, key_name: str) -> Path:
        """Creates a key locally. GCP does not require registering keys other than in instance metadata."""
        if self.key_exists_local(key_name):
            logger.error(f"Key {key_name} already exists locally")
            raise skyplane_exceptions.PermissionsException(
                f"Key {key_name} already exists locally, please delete it first or use a different key name."
            )
        local_key_file_pem, local_key_file_pub = self.get_private_key(key_name), self.get_public_key(key_name)
        logger.fs.debug(f"[GCP] Creating local keypair {key_name}")
        self.local_key_dir.mkdir(parents=True, exist_ok=True)
        generate_keypair(local_key_file_pub, local_key_file_pem)
        return local_key_file_pem

    def delete_key_local(self, key_name: str):
        """Deletes a key locally."""
        if self.key_exists_local(key_name):
            (self.local_key_dir / f"{key_name}.pem").unlink(missing_ok=True)
            (self.local_key_dir / f"{key_name}.pub").unlink(missing_ok=True)

    def ensure_key_exists(self, key_name: str) -> Path:
        """Ensures that a key exists locally, creating it if necessary."""
        if self.key_exists_local(key_name):
            return self.get_private_key(key_name)
        else:
            return self.make_key_local(key_name)
