import os
from pathlib import Path
import concurrent.futures
from typing import Dict, List, Tuple
import re
from shutil import copyfile

from tqdm import tqdm
from skylark.obj_store.object_store_interface import ObjectStoreObject

from skylark.obj_store.s3_interface import S3Interface


def is_plausible_local_path(path: str):
    path = Path(path)
    if path.exists():
        return True
    if path.is_dir():
        return True
    if path.parent.exists():
        return True
    return False


def parse_path(path: str):
    if path.startswith("s3://"):
        bucket_name, key_name = path[5:].split("/", 1)
        return "s3", bucket_name, key_name
    elif path.startswith("gs://"):
        bucket_name, key_name = path[5:].split("/", 1)
        return "gs", bucket_name, key_name
    elif (path.startswith("https://") or path.startswith("http://")) and "blob.core.windows.net" in path:
        regex = re.compile(r"https?://([^/]+).blob.core.windows.net/([^/]+)/(.*)")
        match = regex.match(path)
        if match is None:
            raise ValueError(f"Invalid Azure path: {path}")
        account, container, blob_path = match.groups()
        return "azure", account, container, blob_path
    elif is_plausible_local_path(path):
        return "local", None, path
    return path


# skylark ls implementation
def ls_local(path: Path):
    if not path.exists():
        raise FileNotFoundError(path)
    if path.is_dir():
        for child in path.iterdir():
            yield child.name
    else:
        yield path.name


def ls_s3(bucket_name: str, key_name: str, use_tls: bool = True):
    s3 = S3Interface(None, bucket_name, use_tls=use_tls)
    for obj in s3.list_objects(prefix=key_name):
        yield obj.full_path()


# skylark cp implementation


def copy_local_local(src: Path, dst: Path):
    if not src.exists():
        raise FileNotFoundError(src)
    if not dst.parent.exists():
        raise FileNotFoundError(dst.parent)

    if src.is_dir():
        dst.mkdir(exist_ok=True)
        for child in src.iterdir():
            copy_local_local(child, dst / child.name)
    else:
        dst.parent.mkdir(exist_ok=True, parents=True)
        copyfile(src, dst)


def copy_local_s3(src: Path, dst_bucket: str, dst_key: str, use_tls: bool = True):
    s3 = S3Interface(None, dst_bucket, use_tls=use_tls)
    ops: List[concurrent.futures.Future] = []
    path_mapping: Dict[concurrent.futures.Future, Path] = {}

    def _copy(path: Path, dst_key: str, total_size=0.0):
        if path.is_dir():
            for child in path.iterdir():
                total_size += _copy(child, os.path.join(dst_key, child.name))
            return total_size
        else:
            future = s3.upload_object(path, dst_key)
            ops.append(future)
            path_mapping[future] = path
            return path.stat().st_size

    total_bytes = _copy(src, dst_key)

    # wait for all uploads to complete, displaying a progress bar
    with tqdm(total=total_bytes, unit="B", unit_scale=True, unit_divisor=1024, desc="Uploading") as pbar:
        for op in concurrent.futures.as_completed(ops):
            op.result()
            pbar.update(path_mapping[op].stat().st_size)


def copy_s3_local(src_bucket: str, src_key: str, dst: Path):
    s3 = S3Interface(None, src_bucket)
    ops: List[concurrent.futures.Future] = []
    obj_mapping: Dict[concurrent.futures.Future, ObjectStoreObject] = {}

    # copy single object
    def _copy(src_obj: ObjectStoreObject, dst: Path):
        dst.parent.mkdir(exist_ok=True, parents=True)
        future = s3.download_object(src_obj.key, dst)
        ops.append(future)
        obj_mapping[future] = src_obj
        return src_obj.size

    total_bytes = 0.0
    for obj in s3.list_objects(prefix=src_key):
        sub_key = obj.key[len(src_key) :]
        sub_key = sub_key.lstrip("/")
        dest_path = dst / sub_key
        total_bytes += _copy(obj, dest_path)

    # wait for all downloads to complete, displaying a progress bar
    with tqdm(total=total_bytes, unit="B", unit_scale=True, unit_divisor=1024, desc="Downloading") as pbar:
        for op in concurrent.futures.as_completed(ops):
            op.result()
            pbar.update(obj_mapping[op].size)
