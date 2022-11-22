import json
import math
import queue
import sys
import threading
import time
import uuid
from abc import ABC
from collections import defaultdict
from dataclasses import dataclass, field
from queue import Queue
from typing import TYPE_CHECKING, Callable, Generator, List, Optional, Tuple, TypeVar

import urllib3
from rich import print as rprint

from skyplane import exceptions
from skyplane.api.config import TransferConfig
from skyplane.chunk import Chunk, ChunkRequest
from skyplane.obj_store.azure_blob_interface import AzureBlobObject
from skyplane.obj_store.gcs_interface import GCSObject
from skyplane.obj_store.object_store_interface import ObjectStoreInterface, ObjectStoreObject
from skyplane.obj_store.s3_interface import S3Object
from skyplane.utils import logger
from skyplane.utils.definitions import MB
from skyplane.utils.fn import do_parallel
from skyplane.utils.path import parse_path
from skyplane.utils.timer import Timer

if TYPE_CHECKING:
    from skyplane.api.provision.dataplane import Dataplane

T = TypeVar("T")


class Chunker:
    def __init__(
        self,
        src_iface: ObjectStoreInterface,
        dest_iface: ObjectStoreInterface,
        transfer_config: TransferConfig,
        concurrent_multipart_chunk_threads: int = 64,
    ):
        self.src_iface = src_iface
        self.dest_iface = dest_iface
        self.transfer_config = transfer_config
        self.multipart_upload_requests = []
        self.concurrent_multipart_chunk_threads = concurrent_multipart_chunk_threads

    def _run_multipart_chunk_thread(
        self,
        exit_event: threading.Event,
        in_queue: "Queue[Tuple[ObjectStoreObject, ObjectStoreObject]]",
        out_queue: "Queue[Chunk]",
    ):
        """Chunks large files into many small chunks."""
        region = self.dest_iface.region_tag()
        bucket = self.dest_iface.bucket()
        while not exit_event.is_set():
            try:
                input_data = in_queue.get(block=False, timeout=0.1)
            except queue.Empty:
                continue

            # get source and destination object and then compute number of chunks
            src_object, dest_object = input_data
            mime_type = self.src_iface.get_obj_mime_type(src_object.key)
            upload_id = self.dest_iface.initiate_multipart_upload(dest_object.key, mime_type=mime_type)
            chunk_size_bytes = int(self.transfer_config.multipart_chunk_size_mb * MB)
            num_chunks = math.ceil(src_object.size / chunk_size_bytes)
            if num_chunks > self.transfer_config.multipart_max_chunks:
                chunk_size_bytes = int(src_object.size / self.transfer_config.multipart_max_chunks)
                chunk_size_bytes = math.ceil(chunk_size_bytes / MB) * MB  # round to next largest mb
                num_chunks = math.ceil(src_object.size / chunk_size_bytes)

            # create chunks
            offset = 0
            part_num = 1
            parts = []
            for _ in range(num_chunks):
                file_size_bytes = min(chunk_size_bytes, src_object.size - offset)
                assert file_size_bytes > 0, f"file size <= 0 {file_size_bytes}"
                chunk = Chunk(
                    src_key=src_object.key,
                    dest_key=dest_object.key,
                    chunk_id=uuid.uuid4().hex,
                    file_offset_bytes=offset,
                    chunk_length_bytes=file_size_bytes,
                    part_number=part_num,
                    upload_id=upload_id,
                )
                offset += file_size_bytes
                parts.append(part_num)
                part_num += 1
                out_queue.put(chunk)
            self.multipart_upload_requests.append(dict(upload_id=upload_id, key=dest_object.key, parts=parts, region=region, bucket=bucket))

    def to_chunk_requests(self, gen_in: Generator[Chunk, None, None]) -> Generator[ChunkRequest, None, None]:
        """Converts a generator of chunks to a generator of chunk requests."""
        src_region = self.src_iface.region_tag()
        dest_region = self.dest_iface.region_tag()
        src_bucket = self.src_iface.bucket()
        dest_bucket = self.dest_iface.bucket()
        for chunk in gen_in:
            yield ChunkRequest(
                chunk=chunk,
                src_region=src_region,
                dst_region=dest_region,
                src_object_store_bucket=src_bucket,
                dst_object_store_bucket=dest_bucket,
                src_type="object_store",
                dst_type="object_store",
            )

    @staticmethod
    def map_object_key_prefix(source_prefix: str, source_key: str, dest_prefix: str, recursive: bool = False):
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

    def transfer_pair_generator(
        self,
        src_prefix: str,
        dst_prefix: str,
        recursive: bool,
        prefilter_fn: Optional[Callable[[ObjectStoreObject], bool]] = None,
        postfilter_fn: Optional[Callable[[ObjectStoreObject, ObjectStoreObject], bool]] = None,
    ) -> Generator[Tuple[ObjectStoreObject, ObjectStoreObject], None, None]:
        """Query source region and return list of objects to transfer."""
        if not self.src_iface.bucket_exists():
            raise exceptions.MissingBucketException(f"Source bucket {self.src_iface.path()} does not exist or is not readable.")
        if not self.dest_iface.bucket_exists():
            raise exceptions.MissingBucketException(f"Destination bucket {self.dest_iface.path()} does not exist or is not readable.")

        # query all source region objects
        logger.fs.debug(f"Querying objects in {self.src_iface.path()}")
        n_objs = 0
        for obj in self.src_iface.list_objects(src_prefix):
            if prefilter_fn is None or prefilter_fn(obj):
                try:
                    dest_key = self.map_object_key_prefix(src_prefix, obj.key, dst_prefix, recursive=recursive)
                except exceptions.MissingObjectException as e:
                    logger.fs.exception(e)
                    raise e from None

                # make destination object
                dest_provider, dest_region = self.dest_iface.region_tag().split(":")
                if dest_provider == "aws":
                    dest_obj = S3Object(dest_provider, self.dest_iface.bucket(), dest_key)
                elif dest_provider == "azure":
                    dest_obj = AzureBlobObject(dest_provider, self.dest_iface.bucket(), dest_key)
                elif dest_provider == "gcp":
                    dest_obj = GCSObject(dest_provider, self.dest_iface.bucket(), dest_key)
                else:
                    raise ValueError(f"Invalid dest_region {dest_region}, unknown provider")

                if postfilter_fn is None or postfilter_fn(obj, dest_obj):
                    yield obj, dest_obj
                    n_objs += 1

        if n_objs == 0:
            logger.error("Specified object does not exist.\n")
            raise exceptions.MissingObjectException(f"No objects were found in the specified prefix")

    def chunk(
        self, transfer_pair_generator: Generator[Tuple[ObjectStoreObject, ObjectStoreObject], None, None]
    ) -> Generator[Chunk, None, None]:
        """Break transfer list into chunks."""
        multipart_send_queue: Queue[Tuple[ObjectStoreObject, ObjectStoreObject]] = Queue()
        multipart_chunk_queue: Queue[Chunk] = Queue()
        multipart_exit_event = threading.Event()
        multipart_chunk_threads = []

        # start chunking threads
        if self.transfer_config.multipart_enabled:
            for _ in range(self.concurrent_multipart_chunk_threads):
                t = threading.Thread(
                    target=self._run_multipart_chunk_thread,
                    args=(multipart_exit_event, multipart_send_queue, multipart_chunk_queue),
                    daemon=False,
                )
                t.start()
                multipart_chunk_threads.append(t)

        # begin chunking loop
        for src_obj, dst_obj in transfer_pair_generator:
            if self.transfer_config.multipart_enabled and src_obj.size > self.transfer_config.multipart_threshold_mb * MB:
                multipart_send_queue.put((src_obj, dst_obj))
            else:
                yield Chunk(
                    src_key=src_obj.key,
                    dest_key=dst_obj.key,
                    chunk_id=uuid.uuid4().hex,
                    chunk_length_bytes=src_obj.size,
                )

            if self.transfer_config.multipart_enabled:
                # drain multipart chunk queue and yield with updated chunk IDs
                while not multipart_chunk_queue.empty():
                    yield multipart_chunk_queue.get()

        if self.transfer_config.multipart_enabled:
            # send sentinel to all threads
            multipart_exit_event.set()
            for thread in multipart_chunk_threads:
                thread.join()

            # drain multipart chunk queue and yield with updated chunk IDs
            while not multipart_chunk_queue.empty():
                yield multipart_chunk_queue.get()

    @staticmethod
    def batch_generator(gen_in: Generator[T, None, None], batch_size: int) -> Generator[List[T], None, None]:
        """Batches generator, while handling StopIteration"""
        batch = []
        for item in gen_in:
            batch.append(item)
            if len(batch) == batch_size:
                yield batch
                batch = []
        if len(batch) > 0:
            yield batch

    @staticmethod
    def tail_generator(gen_in: Generator[T, None, None], out_list: List[T]) -> Generator[T, None, None]:
        """Tails generator while handling StopIteration"""
        for item in gen_in:
            out_list.append(item)
            yield item


@dataclass
class TransferJob(ABC):
    src_path: str
    dst_path: str
    recursive: bool = False
    requester_pays: bool = False
    uuid: str = field(init=False, default_factory=lambda: str(uuid.uuid4()))

    def __post_init__(self):
        provider_src, bucket_src, self.src_prefix = parse_path(self.src_path)
        provider_dst, bucket_dst, self.dst_prefix = parse_path(self.dst_path)
        self.src_iface = ObjectStoreInterface.create(f"{provider_src}:infer", bucket_src)
        self.dst_iface = ObjectStoreInterface.create(f"{provider_dst}:infer", bucket_dst)
        if self.requester_pays:
            self.src_iface.set_requester_bool(True)
            self.dst_iface.set_requester_bool(True)

    def dispatch(self, dataplane: "Dataplane", **kwargs) -> Generator[ChunkRequest, None, None]:
        raise NotImplementedError("Dispatch not implemented")

    def finalize(self):
        raise NotImplementedError("Finalize not implemented")

    def verify(self):
        """Verifies the transfer completed, otherwise raises TransferFailedException."""
        raise NotImplementedError("Verify not implemented")

    def estimate_cost(self):
        # TODO
        raise NotImplementedError

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
    multipart_transfer_list: list = field(default_factory=list)

    def __post_init__(self):
        self.http_pool = urllib3.PoolManager(retries=urllib3.Retry(total=3))
        return super().__post_init__()

    def estimate_cost(self):
        raise NotImplementedError()

    def gen_transfer_pairs(self, chunker: Optional[Chunker] = None) -> Generator[Tuple[ObjectStoreObject, ObjectStoreObject], None, None]:
        """Generate transfer pairs for the transfer job."""
        if chunker is None:  # used for external access to transfer pair list
            chunker = Chunker(self.src_iface, self.dst_iface, TransferConfig())
        yield from chunker.transfer_pair_generator(
            self.src_prefix, self.dst_prefix, self.recursive, self._pre_filter_fn, self._post_filter_fn
        )

    def dispatch(
        self,
        dataplane: "Dataplane",
        transfer_config: TransferConfig,
        dispatch_batch_size: int = 64,
    ) -> Generator[ChunkRequest, None, None]:
        """Dispatch transfer job to specified gateways."""
        chunker = Chunker(self.src_iface, self.dst_iface, transfer_config)
        transfer_pair_generator = self.gen_transfer_pairs(chunker)
        gen_transfer_list = chunker.tail_generator(transfer_pair_generator, self.transfer_list)
        chunks = chunker.chunk(gen_transfer_list)
        chunk_requests = chunker.to_chunk_requests(chunks)

        # dispatch chunk requests
        with Timer() as t:
            src_gateways = dataplane.source_gateways()
            bytes_dispatched = [0] * len(src_gateways)
            n_multiparts = 0
            for batch in chunker.batch_generator(chunk_requests, dispatch_batch_size):
                logger.fs.debug(f"Queried {len(batch)} chunks in {t.elapsed:.2f} seconds")
                min_idx = bytes_dispatched.index(min(bytes_dispatched))
                server = src_gateways[min_idx]
                n_bytes = sum([cr.chunk.chunk_length_bytes for cr in batch])
                bytes_dispatched[min_idx] += n_bytes
                start = time.time()
                reply = self.http_pool.request(
                    "POST",
                    f"{server.gateway_api_url}/api/v1/chunk_requests",
                    body=json.dumps([c.as_dict() for c in batch]).encode("utf-8"),
                    headers={"Content-Type": "application/json"},
                )
                end = time.time()
                if reply.status != 200:
                    raise Exception(f"Failed to dispatch chunk requests {server.instance_name()}: {reply.data.decode('utf-8')}")
                logger.fs.debug(
                    f"Dispatched {len(batch)} chunk requests to {server.instance_name()} ({n_bytes} bytes) in {end - start:.2f} seconds"
                )
                yield from batch

                # copy new multipart transfers to the multipart transfer list
                updated_len = len(chunker.multipart_upload_requests)
                self.multipart_transfer_list.extend(chunker.multipart_upload_requests[n_multiparts:updated_len])
                n_multiparts = updated_len

    def finalize(self):
        groups = defaultdict(list)
        for req in self.multipart_transfer_list:
            if "region" not in req or "bucket" not in req:
                raise Exception(f"Invalid multipart upload request: {req}")
            groups[(req["region"], req["bucket"])].append(req)
        for key, group in groups.items():
            region, bucket = key
            batch_len = max(1, len(group) // 128)
            batches = [group[i : i + batch_len] for i in range(0, len(group), batch_len)]
            obj_store_interface = ObjectStoreInterface.create(region, bucket)

            def complete_fn(batch):
                for req in batch:
                    obj_store_interface.complete_multipart_upload(req["key"], req["upload_id"])

            do_parallel(complete_fn, batches, n=-1)

    def verify(self):
        dst_keys = {dst_o.key: src_o for src_o, dst_o in self.transfer_list}
        for obj in self.dst_iface.list_objects(self.dst_prefix):
            # check metadata (src.size == dst.size) && (src.modified <= dst.modified)
            src_obj = dst_keys.get(obj.key)
            if src_obj and src_obj.size == obj.size and src_obj.last_modified <= obj.last_modified:
                del dst_keys[obj.key]
        if dst_keys:
            raise exceptions.TransferFailedException(f"{len(dst_keys)} objects failed verification", [obj.key for obj in dst_keys.values()])


@dataclass
class SyncJob(CopyJob):
    def __post_init__(self):
        return super().__post_init__()

    def estimate_cost(self):
        raise NotImplementedError()

    @classmethod
    def _post_filter_fn(cls, src_obj: ObjectStoreObject, dest_obj: ObjectStoreObject) -> bool:
        return not dest_obj.exists or (src_obj.last_modified > dest_obj.last_modified or src_obj.size != dest_obj.size)
