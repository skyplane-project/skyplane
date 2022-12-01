import re
from pathlib import Path
from typing import Optional, Tuple

from skyplane.utils import logger


def parse_path(path: str) -> Tuple[str, Optional[str], Optional[str]]:
    def is_plausible_local_path(path_test: str):
        path_test = Path(path_test)
        if path_test.exists():
            return True
        if path_test.is_dir():
            return True
        if path_test.parent.exists():
            return True
        return False

    if path.startswith("s3://") or path.startswith("gs://"):
        provider, parsed = path[:2], path[5:]
        if len(parsed) == 0:
            logger.error(f"Invalid S3 path: '{path}'", fg="red", err=True)
            raise ValueError(f"Invalid S3 path: '{path}'")
        bucket, *keys = parsed.split("/", 1)
        key = keys[0] if len(keys) > 0 else ""
        provider = "aws" if provider == "s3" else "gcp"
        return provider, bucket, key
    elif (path.startswith("https://") or path.startswith("http://")) and "blob.core.windows.net" in path:
        # Azure blob storage
        regex = re.compile(r"https?://([^/]+).blob.core.windows.net/([^/]+)/?(.*)")
        match = regex.match(path)
        if match is None:
            raise ValueError(f"Invalid Azure path: {path}")
        account, container, blob_path = match.groups()
        return "azure", f"{account}/{container}", blob_path
    elif path.startswith("azure://"):
        regex = re.compile(r"azure://([^/]+)/([^/]+)/?(.*)")
        match = regex.match(path)
        if match is None:
            raise ValueError(f"Invalid Azure path: {path}")
        account, container, blob_path = match.groups()
        return "azure", f"{account}/{container}", blob_path if blob_path else ""
    elif path.startswith("hdfs://"):
        regex = re.compile(r"hdfs://([^/]+)/?(.*)")
        match = regex.match(path)
        if match is None:
            raise ValueError(f"Invalid HDFS path: {path}")
        host, path = match.groups()
        return "hdfs", host, path
    else:
        if not is_plausible_local_path(path):
            logger.warning(f"Local path '{path}' does not exist")
        return "local", None, path
