from dataclasses import dataclass, field
import json
import sys
from typing import Generator, List, Optional, Tuple, Type

from rich import print as rprint
import urllib3

from skyplane import exceptions
from skyplane.api.impl.chunker import Chunker, batch_generator, tail_generator
from skyplane.api.impl.path import parse_path
from skyplane.chunk import ChunkRequest
from skyplane.compute.server import Server
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
    requester_pays: bool = False

    def __post_init__(self):
        provider_src, bucket_src, self.src_prefix = parse_path(self.src_path)
        provider_dst, bucket_dst, self.dst_prefix = parse_path(self.dst_path)
        self.src_iface = ObjectStoreInterface.create(f"{provider_src}:infer", bucket_src)
        self.dst_iface = ObjectStoreInterface.create(f"{provider_dst}:infer", bucket_dst)
        if self.requester_pays:
            self.src_iface.set_requester_bool(True)
            self.dst_iface.set_requester_bool(True)

    def dispatch(self, src_gateways: List[Server], **kwargs) -> Generator[ChunkRequest, None, None]:
        raise NotImplementedError("Dispatch not implemented")

    def verify(self):
        """Verifies the transfer completed, otherwise raises TransferFailedException."""
        raise NotImplementedError("Verify not implemented")

    def estimate_cost(self):
        # TODO
        raise NotImplementedError

    def _transfer_pair_generator(self) -> Generator[Tuple[ObjectStoreObject, ObjectStoreObject], None, None]:
        """Query source region and return list of objects to transfer."""
        if not self.src_iface.bucket_exists():
            raise exceptions.MissingBucketException(f"Source bucket {self.src_iface.path()} does not exist or is not readable.")
        if not self.dst_iface.bucket_exists():
            raise exceptions.MissingBucketException(f"Destination bucket {self.dst_iface.path()} does not exist or is not readable.")

        # query all source region objects
        logger.fs.debug(f"Querying objects in {self.src_iface.path()}")
        n_objs = 0
        for obj in self.src_iface.list_objects(self.src_prefix):
            if self._pre_filter_fn(obj):
                try:
                    dest_key = self._map_object_key_prefix(self.src_prefix, obj.key, self.dst_prefix, recursive=self.recursive)
                except exceptions.MissingObjectException as e:
                    logger.fs.exception(e)
                    raise e

                # make destination object
                dest_provider, dest_region = self.dst_iface.region_tag().split(":")
                if dest_provider == "aws":
                    dest_obj = S3Object(dest_provider, self.dst_iface.bucket(), dest_key)
                elif dest_provider == "azure":
                    dest_obj = AzureBlobObject(dest_provider, self.dst_iface.bucket(), dest_key)
                elif dest_provider == "gcp":
                    dest_obj = GCSObject(dest_provider, self.dst_iface.bucket(), dest_key)
                else:
                    raise ValueError(f"Invalid dest_region {dest_region}, unknown provider")

                if self._post_filter_fn(obj, dest_obj):
                    yield obj, dest_obj
                    n_objs += 1

        if n_objs == 0:
            logger.error("Specified object does not exist.\n")
            raise exceptions.MissingObjectException(f"No objects were found in the specified prefix")

    @classmethod
    def _map_object_key_prefix(cls, source_prefix: str, source_key: str, dest_prefix: str, recursive: bool = False):
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
    def _pre_filter_fn(cls, obj: ObjectStoreObject) -> bool:
        """Optionally filter source objects before they are transferred."""
        return True

    @classmethod
    def _post_filter_fn(cls, src_obj: ObjectStoreObject, dest_obj: ObjectStoreObject) -> bool:
        """Optionally filter objects by comparing the source and destination objects."""
        return True


@dataclass
class CopyJob(TransferJob):
    transfer_list: list = field(default_factory=list)  # transfer list for later verification

    def __post_init__(self):
        self.http_pool = urllib3.PoolManager(retries=urllib3.Retry(total=3))
        return super().__post_init__()

    def dispatch(
        self,
        src_gateways: List[Server],
        multipart_enabled: bool = True,
        multipart_threshold_mb: int = 128,
        multipart_chunk_size_mb: int = 64,
        multipart_max_chunks: int = 10000,
        dispatch_batch_size: int = 64,
    ) -> Generator[ChunkRequest, None, None]:
        """Dispatch transfer job to specified gateways."""
        gen_transfer_list = tail_generator(self._transfer_pair_generator(), self.transfer_list)
        chunker = Chunker(
            self.src_iface,
            self.dst_iface,
            multipart_enabled=multipart_enabled,
            multipart_threshold_mb=multipart_threshold_mb,
            multipart_chunk_size_mb=multipart_chunk_size_mb,
            multipart_max_chunks=multipart_max_chunks,
        )
        chunks = chunker.chunk(gen_transfer_list)
        chunk_requests = chunker.to_chunk_requests(chunks)
        batches = batch_generator(chunk_requests, dispatch_batch_size)
        bytes_dispatched = [0] * len(src_gateways)
        for batch in batches:
            min_idx = bytes_dispatched.index(min(bytes_dispatched))
            server = src_gateways[min_idx]
            n_bytes = sum([cr.chunk.chunk_length_bytes for cr in batch])
            bytes_dispatched[min_idx] += n_bytes
            reply = self.http_pool.request(
                "POST",
                f"{server.gateway_api_url}/api/v1/chunk_requests",
                body=json.dumps([c.as_dict() for c in batch]).encode("utf-8"),
                headers={"Content-Type": "application/json"},
            )
            if reply.status != 200:
                raise Exception(f"Failed to dispatch chunk requests {server.instance_name()}: {reply.data.decode('utf-8')}")
            yield from batch

        def verify(self):
            dst_keys = {dst_o.key: src_o for src_o, dst_o in self.transfer_list}
            for obj in self.dst_iface.list_objects(self.dst_prefix):
                # check metadata (src.size == dst.size) && (src.modified <= dst.modified)
                src_obj = dst_keys.get(obj.key)
                if src_obj and src_obj.size == obj.size and src_obj.last_modified <= obj.last_modified:
                    del dst_keys[obj.key]
            if dst_keys:
                raise exceptions.TransferFailedException(
                    f"{len(dst_keys)} objects failed verification", [obj.key for obj in dst_keys.values()]
                )


@dataclass
class SyncJob(CopyJob):
    @classmethod
    def _post_filter_fn(cls, src_obj: ObjectStoreObject, dest_obj: ObjectStoreObject) -> bool:
        return not dest_obj.exists or (src_obj.last_modified > dest_obj.last_modified or src_obj.size != dest_obj.size)
