from os import chmod
from pathlib import Path

from cryptography.hazmat.backends import default_backend as crypto_default_backend
from cryptography.hazmat.primitives import serialization as crypto_serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from skyplane.utils.fn import PathLike


def generate_keypair(pubkey_path: PathLike, pem_path: PathLike):
    key = rsa.generate_private_key(
        backend=crypto_default_backend(),
        public_exponent=65537,
        key_size=4096,
    )
    private_key = key.private_bytes(
        crypto_serialization.Encoding.PEM, crypto_serialization.PrivateFormat.TraditionalOpenSSL, crypto_serialization.NoEncryption()
    )
    public_key = key.public_key().public_bytes(crypto_serialization.Encoding.OpenSSH, crypto_serialization.PublicFormat.OpenSSH)
    Path(pubkey_path).write_bytes(public_key)
    Path(pem_path).write_bytes(private_key)
    chmod(pem_path, 0o600)
