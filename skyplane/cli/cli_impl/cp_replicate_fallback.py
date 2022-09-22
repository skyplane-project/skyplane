from typing import Optional
from skyplane.cli.common import parse_path
import os


def fallback_cmd_local_cp(src_path: str, dest_path: str, recursive: bool) -> str:
    return f"cp {src_path} {dest_path}" if not recursive else f"cp -r {src_path} {dest_path}"


def fallback_cmd_local_sync(src_path: str, dest_path: str) -> str:
    return f"rsync -av {src_path} {dest_path}"


def fallback_cmd_s3_cp(src_path: str, dest_path: str, recursive: bool) -> str:
    return f"aws s3 cp {src_path} {dest_path}" if not recursive else f"aws s3 cp --recursive {src_path} {dest_path}"


def fallback_cmd_s3_sync(src_path, dest_path):
    return f"aws s3 sync --no-follow-symlinks {src_path} {dest_path}"


def fallback_cmd_gcp_cp(src_path: str, dest_path: str) -> str:
    return f"gsutil -m cp {src_path} {dest_path}"


def fallback_cmd_gcp_sync(src_path, dest_path):
    return f"gsutil -m rsync -r {src_path} {dest_path}"


def fallback_cmd_azure_cp(src_path: str, dest_path: str, recursive: bool) -> str:
    return f"azcopy cp {src_path} {dest_path}" if not recursive else f"azcopy cp --recursive=true {src_path} {dest_path}"


def fallback_cmd_azure_sync(src_path, dest_path):
    return f"azcopy sync {src_path} {dest_path}"


def replicate_onprem_cp_cmd(src, dst, recursive=True) -> Optional[str]:
    provider_src, _, _ = parse_path(src)
    provider_dst, _, _ = parse_path(dst)

    # local -> local
    if provider_src == "local" and provider_dst == "local":
        return fallback_cmd_local_cp(src, dst, recursive)
    # local -> s3 or s3 -> local
    elif (provider_src == "local" and provider_dst == "aws") or (provider_src == "aws" and provider_dst == "local"):
        return fallback_cmd_s3_cp(src, dst, recursive)
    # local -> gcp or gcp -> local
    elif (provider_src == "local" and provider_dst == "gcp") or (provider_src == "gcp" and provider_dst == "local"):
        return fallback_cmd_gcp_cp(src, dst)
    # local -> azure or azure -> local
    elif (provider_src == "local" and provider_dst == "azure") or (provider_src == "azure" and provider_dst == "local"):
        return fallback_cmd_azure_cp(src, dst, recursive)
    # unsupported fallback
    else:
        return None


def replicate_onprem_sync_cmd(src, dst) -> Optional[str]:
    provider_src, _, _ = parse_path(src)
    provider_dst, _, _ = parse_path(dst)

    # local -> local
    if provider_src == "local" and provider_dst == "local":
        return fallback_cmd_local_sync(src, dst)
    # local -> s3 or s3 -> local
    elif (provider_src == "local" and provider_dst == "aws") or (provider_src == "aws" and provider_dst == "local"):
        return fallback_cmd_s3_sync(src, dst)
    # local -> gcp or gcp -> local
    elif (provider_src == "local" and provider_dst == "gcp") or (provider_src == "gcp" and provider_dst == "local"):
        return fallback_cmd_gcp_sync(src, dst)
    # local -> azure or azure -> local
    elif (provider_src == "local" and provider_dst == "azure") or (provider_src == "azure" and provider_dst == "local"):
        return fallback_cmd_azure_sync(src, dst)
    # unsupported fallback
    else:
        return None


def replicate_small_cp_cmd(src, dst, recursive=True) -> Optional[str]:
    provider_src, _, _ = parse_path(src)
    provider_dst, _, _ = parse_path(dst)

    # s3 -> s3
    if provider_src == "aws" and provider_dst == "aws":
        return fallback_cmd_s3_cp(src, dst, recursive)
    # gcp -> gcp
    elif provider_src == "gcp" and provider_dst == "gcp":
        return fallback_cmd_gcp_cp(src, dst)
    # azure -> azure
    elif provider_src == "azure" and provider_dst == "azure":
        return fallback_cmd_azure_cp(src, dst, recursive)
    # unsupported fallback
    else:
        return None


def replicate_small_sync_cmd(src, dst) -> Optional[str]:
    provider_src, _, _ = parse_path(src)
    provider_dst, _, _ = parse_path(dst)

    # s3 -> s3
    if provider_src == "aws" and provider_dst == "aws":
        return fallback_cmd_s3_sync(src, dst)
    # gcp -> gcp
    elif provider_src == "gcp" and provider_dst == "gcp":
        return fallback_cmd_gcp_sync(src, dst)
    # azure -> azure
    elif provider_src == "azure" and provider_dst == "azure":
        return fallback_cmd_azure_sync(src, dst)
    # unsupported fallback
    else:
        return None


def get_usage_gbits(path):
    if os.path.isdir(path):
        return get_dir_size(path)
    else:
        return os.path.getsize(path)


def get_dir_size(path):
    total = 0
    with os.scandir(path) as it:
        for entry in it:
            if entry.is_file():
                total += entry.stat().st_size
            elif entry.is_dir():
                total += get_dir_size(entry.path)
    return total
