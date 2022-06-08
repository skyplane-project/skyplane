import concurrent.futures
import os
from pathlib import Path
from shutil import copyfile
from typing import List, Dict

from rich.progress import Progress, TextColumn, SpinnerColumn, DownloadColumn, TransferSpeedColumn, TimeElapsedColumn

from skyplane import exceptions
from skyplane.obj_store.azure_interface import AzureInterface
from skyplane.obj_store.gcs_interface import GCSInterface
from skyplane.obj_store.object_store_interface import ObjectStoreInterface, ObjectStoreObject
from skyplane.obj_store.s3_interface import S3Interface
from skyplane.utils import logger


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


def copy_local_objstore(object_interface: ObjectStoreInterface, src: Path, dst_key: str):
    with concurrent.futures.ThreadPoolExecutor(max_workers=32) as executor:
        ops: List[concurrent.futures.Future] = []
        path_mapping: Dict[concurrent.futures.Future, Path] = {}

        def _copy(path: Path, dst_key: str, total_size=0.0):
            if path.is_dir():
                for child in path.iterdir():
                    total_size += _copy(child, os.path.join(dst_key, child.name))
                return total_size
            else:
                future = executor.submit(object_interface.upload_object, path, dst_key)
                ops.append(future)
                path_mapping[future] = path
                return path.stat().st_size

        total_bytes = _copy(src, dst_key)
        bytes_copied = 0
        with Progress(
            SpinnerColumn(),
            TextColumn("[bold yellow]Uploading (from local to object store)"),
            DownloadColumn(binary_units=True),
            TransferSpeedColumn(),
            TimeElapsedColumn(),
        ) as progress:
            copy_task = progress.add_task("", total=total_bytes)
            for op in concurrent.futures.as_completed(ops):
                op.result()
                progress.update(copy_task, advance=path_mapping[op].stat().st_size)


def copy_objstore_local(object_interface: ObjectStoreInterface, src_key: str, dst: Path):
    with concurrent.futures.ThreadPoolExecutor(max_workers=32) as executor:
        ops: List[concurrent.futures.Future] = []
        obj_mapping: Dict[concurrent.futures.Future, ObjectStoreObject] = {}

        # copy single object
        def _copy(src_obj: ObjectStoreObject, dst: Path):
            dst.parent.mkdir(exist_ok=True, parents=True)
            future = executor.submit(object_interface.download_object, src_obj.key, dst)
            ops.append(future)
            obj_mapping[future] = src_obj
            return src_obj.size

        obj_count = 0
        total_bytes = 0.0
        for obj in object_interface.list_objects(prefix=src_key):
            sub_key = obj.key[len(src_key) :]
            sub_key = sub_key.lstrip("/")
            dest_path = dst / sub_key
            total_bytes += _copy(obj, dest_path)
            obj_count += 1

        if not obj_count:
            logger.error("Specified object does not exist.")
            raise exceptions.MissingObjectException()

        # wait for all downloads to complete, displaying a progress bar
        bytes_copied = 0
        with Progress(
            SpinnerColumn(),
            TextColumn("[bold yellow]Downloading (from object store to local)"),
            DownloadColumn(binary_units=True),
            TransferSpeedColumn(),
            TimeElapsedColumn(),
        ) as progress:
            copy_task = progress.add_task("", total=total_bytes)
            for op in concurrent.futures.as_completed(ops):
                op.result()
                progress.update(copy_task, advance=obj_mapping[op].stat().st_size)


def copy_local_gcs(src: Path, dst_bucket: str, dst_key: str):
    gcs = GCSInterface(dst_bucket)
    return copy_local_objstore(gcs, src, dst_key)


def copy_gcs_local(src_bucket: str, src_key: str, dst: Path):
    gcs = GCSInterface(src_bucket)
    return copy_objstore_local(gcs, src_key, dst)


def copy_local_azure(src: Path, dst_account_name: str, dst_container_name: str, dst_key: str):
    azure = AzureInterface(dst_account_name, dst_container_name)
    return copy_local_objstore(azure, src, dst_key)


def copy_azure_local(src_account_name: str, src_container_name: str, src_key: str, dst: Path):
    azure = AzureInterface(src_account_name, src_container_name)
    return copy_objstore_local(azure, src_key, dst)


def copy_local_s3(src: Path, dst_bucket: str, dst_key: str):
    s3 = S3Interface(dst_bucket)
    return copy_local_objstore(s3, src, dst_key)


def copy_s3_local(src_bucket: str, src_key: str, dst: Path):
    s3 = S3Interface(src_bucket)
    return copy_objstore_local(s3, src_key, dst)
