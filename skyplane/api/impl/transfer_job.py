from dataclasses import dataclass
import sys
from typing import Callable, Generator, Optional, Tuple

from rich import print as rprint

from skyplane import exceptions
from skyplane.api.api import TransferList
from skyplane.obj_store.azure_blob_interface import AzureBlobObject
from skyplane.obj_store.gcs_interface import GCSObject
from skyplane.obj_store.object_store_interface import ObjectStoreInterface, ObjectStoreObject
from skyplane.obj_store.s3_interface import S3Object
from skyplane.utils import logger


@dataclass
class TransferJob:
    src_path: str
    dst_path: str
    recursive: bool = False

    def estimate_cost(self):
        # TODO
        raise NotImplementedError

    @classmethod
    def map_object_key_prefix(cls, source_prefix: str, source_key: str, dest_prefix: str, recursive: bool = False):
        """
        map_object_key_prefix computes the mapping of a source key in a bucket prefix to the destination.
        Users invoke a transfer via the CLI; aws s3 cp s3://bucket/source_prefix s3://bucket/dest_prefix.
        The CLI will query the object store for all objects in the source prefix and map them to the
        destination prefix using this function.
        """
        join = lambda prefix, fname: prefix + fname if prefix.endswith("/") else prefix + "/" + fname
        src_fname = source_key.split("/")[-1] if "/" in source_key and not source_key.endswith("/") else source_key
        if not recursive:
            if source_key == source_prefix:
                if dest_prefix == "" or dest_prefix == "/":
                    return src_fname
                elif dest_prefix[-1] == "/":
                    return dest_prefix + src_fname
                else:
                    return dest_prefix
            else:
                # todo: don't print output here
                rprint(f"\n:x: [bold red]In order to transfer objects using a prefix, you must use the --recursive or -r flag.[/bold red]")
                rprint(f"[yellow]If you meant to transfer a single object, pass the full source object key.[/yellow]")
                rprint(f"[bright_black]Try running: [bold]skyplane {' '.join(sys.argv[1:])} --recursive[/bold][/bright_black]")
                raise exceptions.MissingObjectException("Encountered a recursive transfer without the --recursive flag.")
        else:
            if source_prefix == "" or source_prefix == "/":
                if dest_prefix == "" or dest_prefix == "/":
                    return source_key
                else:
                    return join(dest_prefix, source_key)
            else:
                # catch special case: map_object_key_prefix("foo", "foobar/baz.txt", "", recursive=True)
                if not source_key.startswith(source_prefix + "/" if not source_prefix.endswith("/") else source_prefix):
                    rprint(f"\n:x: [bold red]The source key {source_key} does not start with the source prefix {source_prefix}[/bold red]")
                    raise exceptions.MissingObjectException(f"Source key {source_key} does not start with source prefix {source_prefix}")
                if dest_prefix == "" or dest_prefix == "/":
                    return source_key[len(source_prefix) :]
                else:
                    src_path_after_prefix = source_key[len(source_prefix) :]
                    src_path_after_prefix = src_path_after_prefix[1:] if src_path_after_prefix.startswith("/") else src_path_after_prefix
                    return join(dest_prefix, src_path_after_prefix)

    @classmethod
    def _make_abstract_object(cls, dest_bucket, dest_key, dest_region) -> ObjectStoreObject:
        if dest_region.startswith("aws"):
            return S3Object(dest_region.split(":")[0], dest_bucket, dest_key)
        elif dest_region.startswith("gcp"):
            return GCSObject(dest_region.split(":")[0], dest_bucket, dest_key)
        elif dest_region.startswith("azure"):
            return AzureBlobObject(dest_region.split(":")[0], dest_bucket, dest_key)
        else:
            raise ValueError(f"Invalid dest_region {dest_region}, unknown provider")

    @classmethod
    def _query_bucket_generator(
        cls,
        src_iface: ObjectStoreInterface,
        dst_iface: ObjectStoreInterface,
        source_prefix: str,
        dest_prefix: str,
        recursive: bool = False,
        pre_filter_fn: Optional[Callable[[ObjectStoreObject], bool]] = None,
        post_filter_fn: Optional[Callable[[ObjectStoreObject, ObjectStoreObject], bool]] = None,
    ) -> Generator[Tuple[ObjectStoreObject, ObjectStoreObject], None, None]:
        """Query source region and return list of objects to transfer."""
        if not src_iface.bucket_exists():
            raise exceptions.MissingBucketException(f"Source bucket {src_iface.path()} does not exist or is not readable.")
        if not dst_iface.bucket_exists():
            raise exceptions.MissingBucketException(f"Destination bucket {dst_iface.path()} does not exist or is not readable.")

        # query all source region objects
        logger.fs.debug(f"Querying objects in {src_iface.path()}")
        n_objs = 0
        for obj in src_iface.list_objects(source_prefix):
            if pre_filter_fn(obj):
                try:
                    dest_key = cls.map_object_key_prefix(source_prefix, obj.key, dest_prefix, recursive=recursive)
                except exceptions.MissingObjectException as e:
                    logger.fs.exception(e)
                    raise e

                dest_region = dst_iface.region_tag()
                dest_bucket = dst_iface.bucket()
                dest_obj = cls._make_abstract_object(dest_bucket, dest_key, dest_region)

                if post_filter_fn(obj, dest_obj):
                    yield obj, dest_obj
                    n_objs += 1

        if n_objs == 0:
            logger.error("Specified object does not exist.\n")
            raise exceptions.MissingObjectException(f"No objects were found in the specified prefix")


@dataclass
class CopyJob(TransferJob):
    def filter_transfer_list(self, transfer_list: TransferList) -> TransferList:
        return transfer_list


@dataclass
class SyncJob(CopyJob):
    def filter_transfer_list(self, transfer_list: TransferList) -> TransferList:
        transfer_pairs = []
        for src_obj, dst_obj in transfer_list:
            if not dst_obj.exists or (src_obj.last_modified > dst_obj.last_modified or src_obj.size != dst_obj.size):
                transfer_pairs.append((src_obj, dst_obj))
        return transfer_pairs
