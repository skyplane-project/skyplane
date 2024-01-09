import os

from typing import Optional
from pathlib import Path

from skyplane.utils.path import parse_path


def fallback_cmd_local_cp(src_path: str, dest_path: str, recursive: bool) -> str:
    return f"cp {src_path} {dest_path}" if not recursive else f"cp -r {src_path} {dest_path}"


def fallback_cmd_local_sync(src_path: str, dest_path: str) -> str:
    return f"rsync -av {src_path} {dest_path}"


def fallback_cmd_s3_cp(src_path: str, dest_path: str, recursive: bool) -> str:
    return f"aws s3 cp {src_path} {dest_path}" if not recursive else f"aws s3 cp --recursive {src_path} {dest_path}"


def fallback_cmd_s3_sync(src_path, dest_path):
    return f"aws s3 sync --no-follow-symlinks {src_path} {dest_path}"


def fallback_cmd_gcp_cp(src_path: str, dest_path: str, recursive: bool) -> str:
    return f"gsutil -m cp {src_path} {dest_path}" if not recursive else f"gsutil -m cp -r {src_path} {dest_path}"


def fallback_cmd_gcp_sync(src_path, dest_path):
    return f"gsutil -m rsync -r {src_path} {dest_path}"


def fallback_cmd_azure_cp(src_path: str, dest_path: str, recursive: bool) -> str:
    return f"azcopy cp {src_path} {dest_path}" if not recursive else f"azcopy cp --recursive=true {src_path} {dest_path}"


def fallback_cmd_azure_sync(src_path, dest_path):
    return f"azcopy sync {src_path} {dest_path}"

def fallback_cmd_bq_extract(bucket, table, dest_path):
    ### BQ -> Local
    return f"bq --format=csv query 'select * from [{bucket}.{table}]' > {os.path.join(dest_path, table)}"

def fallback_cmd_bq_upload(bucket, table, src_path): 
    ### Local -> BQ
    return f"bq load --autodetect {bucket}.{table} {src_path}"

def parse_bq_url(src): 
    src = src[5:]
    if (src[len(src)-1] == "/"):
        src = src[0:len(src)-1]
    src = src.replace("/", ".")
    return src

def parse_filename_bq(tablename):
    ## dataset_1.csv -> dataset_1
    return os.path.splitext(os.path.basename(os.path.normpath(tablename)))[0]

def fallback_cmd_bq_download_cp(provider: str, dataset: str, table: str, dest_path: str):
    combined_command = ""
    cur_path = Path.cwd()
    joined_path = os.path.join(cur_path, table)
    combined_command += (fallback_cmd_bq_extract(dataset, table, cur_path) + "&&")
    if (provider == "aws"):
        combined_command += (fallback_cmd_s3_cp(joined_path, dest_path, False))
    elif (provider == "gcp"):
        combined_command += (fallback_cmd_s3_cp(joined_path, dest_path, False) + "&&")
    elif (provider == "azure"): 
        combined_command += (fallback_cmd_azure_cp(joined_path, dest_path, False) + "&&")
    #combined_command += f"rm {joined_path}"
    return combined_command
def fallback_cmd_bq_upload_cp(provider: str, src_path: str, bucket: str, table: str):
    combined_command = ""
    cur_path = Path.cwd()
    joined_path = cur_path
    if (provider == "aws"):
        combined_command += (fallback_cmd_s3_cp(src_path, joined_path, False) + "&&")
    elif (provider == "gcp"):
        combined_command += (fallback_cmd_gcp_cp(src_path, joined_path, False) + "&&")
    elif (provider == "azure"):
        combined_command += (fallback_cmd_azure_cp(src_path, joined_path, False) + "&&")
    joined_path = os.path.join(cur_path, table)
    combined_command += fallback_cmd_bq_upload(bucket, table, joined_path) + "&&"
    combined_command += f"rm {joined_path}"
    return combined_command

def replicate_onprem_cp_cmd(src, dst, recursive=True) -> Optional[str]:
    provider_src, bucket_src, key_src = parse_path(src)
    provider_dst, bucket_dst, key_dst = parse_path(dst)

    # local -> local
    if provider_src == "local" and provider_dst == "local":
        return fallback_cmd_local_cp(src, dst, recursive)
    # local -> s3 or s3 -> local
    elif (provider_src == "local" and provider_dst == "aws") or (provider_src == "aws" and provider_dst == "local"):
        return fallback_cmd_s3_cp(src, dst, recursive)
    # local -> gcp or gcp -> local
    elif (provider_src == "local" and provider_dst == "gcp") or (provider_src == "gcp" and provider_dst == "local"):
        return fallback_cmd_gcp_cp(src, dst, recursive)
    # local -> azure or azure -> local
    elif (provider_src == "local" and provider_dst == "azure") or (provider_src == "azure" and provider_dst == "local"):
        return fallback_cmd_azure_cp(src, dst, recursive)
    elif (provider_src == "local" and provider_dst == "bq") or (provider_src == "bq" and provider_dst == "local"):
        if provider_src == "bq":
            return fallback_cmd_bq_extract(bucket_src, key_src, dst)
        else:
            return fallback_cmd_bq_upload(bucket_dst, parse_filename_bq(key_src), src)
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
    provider_src, bucket_src, key_src = parse_path(src)
    provider_dst, bucket_dst, key_dst = parse_path(dst)
    # s3 -> s3
    if provider_src == "aws" and provider_dst == "aws":
        return fallback_cmd_s3_cp(src, dst, recursive)
    # gcp -> gcp
    elif provider_src == "gcp" and provider_dst == "gcp":
        return fallback_cmd_gcp_cp(src, dst, recursive)
    # azure -> azure
    elif provider_src == "azure" and provider_dst == "azure":
        return fallback_cmd_azure_cp(src, dst, recursive)
    # BQ -> Other Cloud Provider
    elif provider_src == "bq" and provider_dst != "bq":
        return fallback_cmd_bq_download_cp(provider_dst, bucket_src, key_src, dst)
    # Other Cloud Provider -> BQ
    elif provider_src != "bq" and provider_dst == "bq":
        value = fallback_cmd_bq_upload_cp(provider_src, src, bucket_dst, key_src)
        return value
    # unsupported fallback
    else:
        print("unsupported fallback")
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


def get_dir_size(path):
    total = 0
    with os.scandir(path) as it:
        for entry in it:
            if entry.is_file():
                total += entry.stat().st_size
            elif entry.is_dir():
                total += get_dir_size(entry.path)
    return total
