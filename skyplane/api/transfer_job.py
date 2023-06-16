import json
import signal
import time
import time
import typer
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

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Callable, Generator, List, Optional, Tuple, TypeVar, Dict

from abc import ABC, abstractmethod

import urllib3
from rich import print as rprint
from functools import partial

from skyplane import exceptions
from skyplane.api.config import TransferConfig
from skyplane.chunk import Chunk, ChunkRequest
from skyplane.obj_store.azure_blob_interface import AzureBlobObject
from skyplane.obj_store.gcs_interface import GCSObject
from skyplane.obj_store.r2_interface import R2Object
from skyplane.obj_store.storage_interface import StorageInterface
from skyplane.obj_store.object_store_interface import ObjectStoreObject, ObjectStoreInterface
from skyplane.obj_store.s3_interface import S3Object
from skyplane.utils import logger
from skyplane.utils.definitions import MB
from skyplane.utils.fn import do_parallel
from skyplane.utils.path import parse_path
from skyplane.utils.retry import retry_backoff

if TYPE_CHECKING:
    from skyplane.api.dataplane import Dataplane

T = TypeVar("T")


@dataclass
class TransferPair:
    "Represents transfer pair between source and destination"

    def __init__(self, src_obj: ObjectStoreObject, dst_objs: Dict[str, ObjectStoreObject], dst_key: str):
        self.src_obj = src_obj
        self.dst_objs = dst_objs
        self.dst_key = dst_key  # shared destination key across all chunks (differnt prefixes)


@dataclass
class GatewayMessage:
    def __init__(self, chunk: Optional[Chunk] = None, upload_id_mapping: Optional[Dict[str, Tuple[str, str]]] = None):
        self.chunk = chunk
        # TODO: currently, the mapping ID is per-region, however this should be per-bucket, as there may be multiple
        # target buckets in the same region
        self.upload_id_mapping = upload_id_mapping  # region_tag: (upload_id, dst_key)


class Chunker:
    """class that chunks the original files and makes the chunk requests"""

    def __init__(
        self,
        src_iface: StorageInterface,
        dst_ifaces: List[StorageInterface],
        transfer_config: Optional[TransferConfig] = None,
        concurrent_multipart_chunk_threads: Optional[int] = 64,
        num_partitions: Optional[int] = 1,
    ):
        """
        :param src_iface: source object store interface
        :type src_iface: StorageInterface
        :param dst_iface: destination object store interface
        :type dst_ifaces: List[StorageInterface]
        :param transfer_config: the configuration during the transfer
        :type transfer_config: TransferConfig
        :param concurrent_multipart_chunk_threads: the maximum number of concurrent threads that dispatch multipart chunk requests (default: 64)
        :type concurrent_multipart_chunk_threads: int
        """
        self.src_iface = src_iface
        self.dst_ifaces = dst_ifaces
        self.transfer_config = transfer_config
        self.multipart_upload_requests = []
        self.concurrent_multipart_chunk_threads = concurrent_multipart_chunk_threads
        self.num_partitions = num_partitions
        if transfer_config is None:
            self.transfer_config = TransferConfig()

        # threads for multipart uploads
        self.multipart_send_queue: Queue[TransferPair] = Queue()
        self.multipart_chunk_queue: Queue[GatewayMessage] = Queue()
        self.multipart_exit_event = threading.Event()
        self.multipart_chunk_threads = []

        # handle exit signal
        def signal_handler(signal, frame):
            self.multipart_exit_event.set()
            for t in self.multipart_chunk_threads:
                t.join()

        signal.signal(signal.SIGINT, signal_handler)

    def stop(self):
        """Stops all threads"""
        self.multipart_exit_event.set()
        for t in self.multipart_chunk_threads:
            t.join()

    def _run_multipart_chunk_thread(
        self,
        exit_event: threading.Event,
        in_queue: "Queue[TransferPair]",
        out_queue_chunks: "Queue[GatewayMessage]",
    ):
        """Chunks large files into many small chunks."""
        while not exit_event.is_set():
            try:
                transfer_pair = in_queue.get(block=False, timeout=0.1)
            except queue.Empty:
                continue

            src_object = transfer_pair.src_obj
            dest_objects = transfer_pair.dst_objs
            dest_key = transfer_pair.dst_key
            if isinstance(self.src_iface, ObjectStoreInterface):
                mime_type = self.src_iface.get_obj_mime_type(src_object.key)
                # create multipart upload request per destination
                upload_id_mapping = {}
                for dest_iface in self.dst_ifaces:
                    dest_object = dest_objects[dest_iface.region_tag()]
                    upload_id = dest_iface.initiate_multipart_upload(dest_object.key, mime_type=mime_type)
                    # print(f"Created upload id for key {dest_object.key} with upload id {upload_id} for bucket {dest_iface.bucket_name}")
                    # store mapping between key and upload id for each region
                    upload_id_mapping[dest_iface.region_tag()] = (src_object.key, upload_id)
                out_queue_chunks.put(GatewayMessage(upload_id_mapping=upload_id_mapping))  # send to output queue

                # get source and destination object and then compute number of chunks
                chunk_size_bytes = int(self.transfer_config.multipart_chunk_size_mb * MB)
                num_chunks = math.ceil(src_object.size / chunk_size_bytes)
                if num_chunks > self.transfer_config.multipart_max_chunks:
                    chunk_size_bytes = int(src_object.size / self.transfer_config.multipart_max_chunks)
                    chunk_size_bytes = math.ceil(chunk_size_bytes / MB) * MB  # round to next largest mb
                    num_chunks = math.ceil(src_object.size / chunk_size_bytes)

                assert num_chunks * chunk_size_bytes >= src_object.size
                # create chunks
                offset = 0
                part_num = 1
                parts = []
                for _ in range(num_chunks):
                    file_size_bytes = min(chunk_size_bytes, src_object.size - offset)
                    assert file_size_bytes > 0, f"file size <= 0 {file_size_bytes}"
                    chunk = Chunk(
                        src_key=src_object.key,
                        dest_key=dest_key,  # dest_object.key, # TODO: upload basename (no prefix)
                        chunk_id=uuid.uuid4().hex,
                        file_offset_bytes=offset,
                        partition_id=str(part_num % self.num_partitions),
                        chunk_length_bytes=file_size_bytes,
                        part_number=part_num,
                        # upload_id=upload_id,
                        multi_part=True,
                    )
                    assert upload_id is not None, f"Upload id cannot be None for multipart upload for {src_object.key}"
                    assert part_num is not None, f"Partition cannot be none {part_num}"
                    offset += file_size_bytes
                    parts.append(part_num)
                    part_num += 1
                    out_queue_chunks.put(GatewayMessage(chunk=chunk))

                # store multipart ids
                for dest_iface in self.dst_ifaces:
                    bucket = dest_iface.bucket()
                    region = dest_iface.region_tag()
                    dest_object = dest_objects[region]
                    _, upload_id = upload_id_mapping[region]
                    self.multipart_upload_requests.append(
                        dict(upload_id=upload_id, key=dest_object.key, parts=parts, region=region, bucket=bucket)
                    )
            else:
                mime_type = None
                raise NotImplementedError("Multipart not implement for non-object store interfaces")

    # def to_chunk_requests(self, gen_in: Generator[Chunk, None, None]) -> Generator[ChunkRequest, None, None]:
    #    """Converts a generator of chunks to a generator of chunk requests.

    #    :param gen_in: generator that generates chunk requests
    #    :type gen_in: Generator
    #    """
    #    src_region = self.src_iface.region_tag()
    #    src_bucket = self.src_iface.bucket()
    #    for chunk in gen_in:
    #        yield ChunkRequest(
    #            chunk=chunk,
    #            src_region=src_region,
    #            #dst_region=dest_region,
    #            src_object_store_bucket=src_bucket,
    #            #dst_object_store_bucket=dest_bucket,
    #            src_type="object_store",
    #            #dst_type="object_store",
    #        )

    @staticmethod
    def map_object_key_prefix(source_prefix: str, source_key: str, dest_prefix: str, recursive: bool = False):
        """
        map_object_key_prefix computes the mapping of a source key in a bucket prefix to the destination.
        Users invoke a transfer via the CLI; aws s3 cp s3://bucket/source_prefix s3://bucket/dest_prefix.
        The CLI will query the object store for all objects in the source prefix and map them to the
        destination prefix using this function.

        :param source_prefix: source bucket folder prefix
        :type source_prefix: string
        :param source_key: source file key to map in the folder prefix
        :type source_key: string
        :param destination_prefix: destination bucket folder prefix
        :type destination_prefix: string
        :param recursive: whether to copy all the objects matching the pattern (default: False)
        :type recursive: bool
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
                raise exceptions.MissingObjectException("Encountered a recursive transfer without the --recursive flag.") from None
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
        dst_prefixes: List[str],
        dataplane: "Dataplane",
        recursive: bool,
        prefilter_fn: Optional[Callable[[ObjectStoreObject], bool]] = None,  # TODO: change to StorageObject
    ) -> Generator[TransferPair, None, None]:
        """Query source region and return list of objects to transfer.

        :param src_prefix: source bucket folder prefix
        :type src_prefix: string
        :param dst_prefix: destination bucket folder prefix
        :type dst_prefix: string
        :param recursive: if true, will copy objects at folder prefix recursively
        :type recursive: bool
        :param prefilter_fn: filters out objects whose prefixes do not match the filter function (default: None)
        :type prefilter_fn: Callable[[ObjectStoreObject], bool]
        """
        if not isinstance(self.src_iface, ObjectStoreInterface) or not all(
            [isinstance(dst_iface, ObjectStoreInterface) for dst_iface in self.dst_ifaces]
        ):
            raise NotImplementedError("TransferPair only supports object store interfaces")

        if not self.src_iface.bucket_exists():
            raise exceptions.MissingBucketException(f"Source bucket {self.src_iface.path()} does not exist or is not readable.")
        for dst_iface in self.dst_ifaces:
            if not dst_iface.bucket_exists():
                raise exceptions.MissingBucketException(f"Destination bucket {dst_iface.path()} does not exist or is not readable.")

        # query all source region objects
        logger.fs.debug(f"Querying objects in {self.src_iface.path()}")
        n_objs = 0
        for obj in self.src_iface.list_objects(src_prefix):
            do_parallel(lambda i: i.run_command("echo 1"), dataplane.bound_nodes.values(), n=8)
            if prefilter_fn is None or prefilter_fn(obj):
                # collect list of destination objects
                dest_objs = {}
                dest_keys = []
                for i in range(len(self.dst_ifaces)):
                    dst_iface = self.dst_ifaces[i]
                    dst_prefix = dst_prefixes[i]
                    dest_provider, dest_region = dst_iface.region_tag().split(":")
                    try:
                        dest_key = self.map_object_key_prefix(src_prefix, obj.key, dst_prefix, recursive=recursive)
                        assert (
                            dest_key[: len(dst_prefix)] == dst_prefix
                        ), f"Destination key {dest_key} does not start with destination prefix {dst_prefix}"
                        dest_keys.append(dest_key[len(dst_prefix) :])
                    except exceptions.MissingObjectException as e:
                        logger.fs.exception(e)
                        raise e from None

                    dest_obj = dst_iface.create_object_repr(dest_key)
                    dest_objs[dst_iface.region_tag()] = dest_obj

                # assert that all destinations share the same post-fix key
                assert len(list(set(dest_keys))) == 1, f"Destination keys {dest_keys} do not match"
                n_objs += 1
                yield TransferPair(src_obj=obj, dst_objs=dest_objs, dst_key=dest_keys[0])

        if n_objs == 0:
            logger.error("Specified object does not exist.\n")
            raise exceptions.MissingObjectException(f"No objects were found in the specified prefix")

    def chunk(self, transfer_pair_generator: Generator[TransferPair, None, None]) -> Generator[GatewayMessage, None, None]:
        """Break transfer list into chunks.

        :param transfer_pair_generator: generator of pairs of objects to transfer
        :type transfer_pair_generator: Generator
        """
        # start chunking threads
        if self.transfer_config.multipart_enabled:
            for _ in range(self.concurrent_multipart_chunk_threads):
                t = threading.Thread(
                    target=self._run_multipart_chunk_thread,
                    args=(self.multipart_exit_event, self.multipart_send_queue, self.multipart_chunk_queue),
                    daemon=False,
                )
                t.start()
                self.multipart_chunk_threads.append(t)

        # begin chunking loop
        for transfer_pair in transfer_pair_generator:
            src_obj = transfer_pair.src_obj
            if self.transfer_config.multipart_enabled and src_obj.size > self.transfer_config.multipart_threshold_mb * MB:
                self.multipart_send_queue.put(transfer_pair)
            else:
                if transfer_pair.src_obj.size == 0:
                    logger.fs.debug(f"Skipping empty object {src_obj.key}")
                    continue
                yield GatewayMessage(
                    chunk=Chunk(
                        src_key=src_obj.key,
                        dest_key=transfer_pair.dst_key,  # TODO: get rid of dest_key, and have write object have info on prefix  (or have a map here)
                        chunk_id=uuid.uuid4().hex,
                        chunk_length_bytes=transfer_pair.src_obj.size,
                        partition_id=str(0),  # TODO: fix this to distribute across multiple partitions
                    )
                )

            if self.transfer_config.multipart_enabled:
                # drain multipart chunk queue and yield with updated chunk IDs
                while not self.multipart_chunk_queue.empty():
                    yield self.multipart_chunk_queue.get()

        if self.transfer_config.multipart_enabled:
            # wait for processing multipart requests to finish
            logger.fs.debug("Waiting for multipart threads to finish")
            # while not multipart_send_queue.empty():
            # TODO: may be an issue waiting for this in case of force-quit
            while not self.multipart_send_queue.empty():
                logger.fs.debug(f"Remaining in multipart queue: sent {self.multipart_send_queue.qsize()}")
                time.sleep(0.1)
            # send sentinel to all threads
            self.multipart_exit_event.set()
            for thread in self.multipart_chunk_threads:
                thread.join()

            # drain multipart chunk queue and yield with updated chunk IDs
            while not self.multipart_chunk_queue.empty():
                yield self.multipart_chunk_queue.get()

    @staticmethod
    def batch_generator(gen_in: Generator[T, None, None], batch_size: int) -> Generator[List[T], None, None]:
        """Batches generator, while handling StopIteration

        :param gen_in: generator that generates chunk requests
        :type gen_in: Generator
        """
        batch = []
        for item in gen_in:
            batch.append(item)
            if len(batch) == batch_size:
                yield batch
                batch = []
        if len(batch) > 0:
            yield batch

    # @staticmethod
    def prefetch_generator(self, gen_in: Generator[T, None, None], buffer_size: int) -> Generator[T, None, None]:
        """
        Prefetches from generator while handing StopIteration to ensure items yield immediately.
        Start a thread to prefetch items from the generator and put them in a queue. Upon StopIteration,
        the thread will add a sentinel value to the queue.

        :param gen_in: generator that generates chunk requests
        :type gen_in: Generator
        :param buffer_size: maximum size of the buffer to temporarily store the generators
        :type buffer_size: int
        """
        sentinel = object()
        queue = Queue(maxsize=buffer_size)

        def prefetch():
            for item in gen_in:
                if self.multipart_exit_event.is_set():  # exit with exit event
                    break
                queue.put(item)
            queue.put(sentinel)

        thread = threading.Thread(target=prefetch, daemon=True)
        thread.start()

        while True:
            item = queue.get()
            if item is sentinel:
                break
            yield item

    @staticmethod
    def tail_generator(gen_in: Generator[T, None, None], out_list: List[T]) -> Generator[T, None, None]:
        """Tails generator while handling StopIteration

        :param gen_in: generator that generates chunk requests
        :type gen_in: Generator
        :param out_list: list of tail generators
        :type out_list: List
        """
        for item in gen_in:
            out_list.append(item)
            yield item


@dataclass
class TransferJob(ABC):
    """
    transfer job with transfer configurations

    :param src_path: source full path
    :type src_path: str
    :param dst_paths: destination full path
    :type dst_paths: str
    :param recursive: if true, will transfer objects at folder prefix recursively (default: False)
    :type recursive: bool
    :param requester_pays: if set, will support requester pays buckets. (default: False)
    :type requester_pays: bool
    :param uuid: the uuid of one single transfer job
    :type uuid: str
    """

    # @abstractmethod
    def __init__(
        self,
        src_path: str,
        dst_paths: List[str] or str,
        recursive: bool = False,
        requester_pays: bool = False,
        transfer_config: Optional[TransferConfig] = None,
        uuid: str = field(init=False, default_factory=lambda: str(uuid.uuid4())),
    ):
        self.src_path = src_path
        self.dst_paths = dst_paths if isinstance(dst_paths, list) else [dst_paths]
        self.recursive = recursive
        self.requester_pays = requester_pays
        self.uuid = uuid
        self.transfer_config = transfer_config if transfer_config else TransferConfig()

    @property
    def transfer_type(self) -> str:
        if isinstance(self.dst_paths, str):
            return "unicast"
        else:
            return "multicast"

    @property
    def src_prefix(self) -> Optional[str]:
        """Return the source prefix"""
        if not hasattr(self, "_src_prefix"):
            self._src_prefix = parse_path(self.src_path)[2]
        return self._src_prefix

    @property
    def src_iface(self) -> StorageInterface:
        """Return the source object store interface"""
        if not hasattr(self, "_src_iface"):
            provider_src, bucket_src, _ = parse_path(self.src_path)
            self._src_iface = ObjectStoreInterface.create(f"{provider_src}:infer", bucket_src)
            if self.requester_pays:
                self._src_iface.set_requester_bool(True)
        return self._src_iface

    @property
    def dst_prefixes(self) -> List[str]:
        """Return the destination prefix"""
        if not hasattr(self, "_dst_prefix"):
            if self.transfer_type == "unicast":
                self._dst_prefix = [str(parse_path(self.dst_paths[0])[2])]
            else:
                self._dst_prefix = [str(parse_path(path)[2]) for path in self.dst_paths]
        return self._dst_prefix

    @property
    def dst_ifaces(self) -> List[StorageInterface]:
        """Return the destination object store interface"""
        if not hasattr(self, "_dst_ifaces"):
            if self.transfer_type == "unicast":
                provider_dst, bucket_dst, _ = parse_path(self.dst_paths[0])
                self._dst_ifaces = [StorageInterface.create(f"{provider_dst}:infer", bucket_dst)]
            else:
                self._dst_ifaces = []
                for path in self.dst_paths:
                    provider_dst, bucket_dst, _ = parse_path(path)
                    self._dst_ifaces.append(StorageInterface.create(f"{provider_dst}:infer", bucket_dst))
        return self._dst_ifaces

    def dispatch(self, dataplane: "Dataplane", **kwargs) -> Generator[Chunk, None, None]:
        """Dispatch transfer job to specified gateways."""
        raise NotImplementedError("Dispatch not implemented")

    def finalize(self):
        """Complete the multipart upload requests"""
        raise NotImplementedError("Finalize not implemented")

    def verify(self):
        """Verifies the transfer completed, otherwise raises TransferFailedException."""
        raise NotImplementedError("Verify not implemented")

    def size_gb(self):
        """Return the size of the transfer in GB"""
        raise NotImplementedError("Size not implemented")

    @classmethod
    def _pre_filter_fn(cls, obj: ObjectStoreObject) -> bool:
        """Optionally filter source objects before they are transferred.

        :meta private:
        :param obj: source object to be transferred
        :type obj: ObjectStoreObject
        """
        return True


@dataclass
class CopyJob(TransferJob):
    """copy job that copies the source objects to the destination

    :param transfer_list: transfer list for later verification
    :type transfer_list: list
    :param multipart_transfer_list: multipart transfer list for later verification
    :type multipart_transfer_list: list
    """

    def __init__(
        self,
        src_path: str,
        dst_paths: List[str] or str,
        recursive: bool = False,
        requester_pays: bool = False,
        transfer_config: Optional[TransferConfig] = None,
        uuid: str = field(init=False, default_factory=lambda: str(uuid.uuid4())),
    ):
        super().__init__(src_path, dst_paths, recursive, requester_pays, transfer_config, uuid)
        self.transfer_list = []
        self.multipart_transfer_list = []
        self.chunker = Chunker(self.src_iface, self.dst_ifaces, self.transfer_config)  # TODO: should read in existing transfer config

    def stop(self):
        self.chunker.stop()

    @property
    def http_pool(self):
        """http connection pool"""
        if not hasattr(self, "_http_pool"):
            timeout = urllib3.util.Timeout(connect=10.0, read=None)  # no read timeout
            self._http_pool = urllib3.PoolManager(retries=urllib3.Retry(total=3), timeout=timeout)
        return self._http_pool

    def gen_transfer_pairs(
        self,
        dataplane: Optional["Dataplane"] = None
    ) -> Generator[TransferPair, None, None]:
        """Generate transfer pairs for the transfer job.

        :param chunker: chunker that makes the chunk requests
        :type chunker: Chunker
        """
        yield from self.chunker.transfer_pair_generator(self.src_prefix, self.dst_prefixes, dataplane, self.recursive, self._pre_filter_fn)

    def dispatch(
        self,
        dataplane: "Dataplane",
        dispatch_batch_size: int = 100,  # 6.4 GB worth of chunks
    ) -> Generator[Chunk, None, None]:
        """Dispatch transfer job to specified gateways.

        :param dataplane: dataplane that starts the transfer job
        :type dataplane: Dataplane
        :param transfer_config: the configuration during the transfer
        :type transfer_config: TransferConfig
        :param dispatch_batch_size: maximum size of the buffer to temporarily store the generators (default: 1000)
        :type dispatch_batch_size: int
        """
        # chunker = Chunker(self.src_iface, self.dst_ifaces, transfer_config)
        transfer_pair_generator = self.gen_transfer_pairs(dataplane)  # returns TransferPair objects
        gen_transfer_list = self.chunker.tail_generator(transfer_pair_generator, self.transfer_list)
        chunks = self.chunker.chunk(gen_transfer_list)
        batches = self.chunker.batch_generator(
            self.chunker.prefetch_generator(chunks, buffer_size=dispatch_batch_size * 32), batch_size=dispatch_batch_size
        )

        # dispatch chunk requests
        src_gateways = dataplane.source_gateways()
        queue_size = [0] * len(src_gateways)
        n_multiparts = 0
        start = time.time()

        for batch in batches:
            # send upload_id mappings to sink gateways
            upload_id_batch = [cr for cr in batch if cr.upload_id_mapping is not None]
            region_dst_gateways = dataplane.sink_gateways()
            for region_tag, dst_gateways in region_dst_gateways.items():
                for dst_gateway in dst_gateways:
                    # collect upload id mappings
                    mappings = {}
                    for message in upload_id_batch:
                        for region_tag, (key, id) in message.upload_id_mapping.items():
                            if region_tag not in mappings:
                                mappings[region_tag] = {}
                            mappings[region_tag][key] = id

                    # send mapping to gateway
                    reply = self.http_pool.request(
                        "POST",
                        f"{dst_gateway.gateway_api_url}/api/v1/upload_id_maps",
                        body=json.dumps(mappings).encode("utf-8"),
                        headers={"Content-Type": "application/json"},
                    )
                    # TODO: assume that only destination nodes would write to the obj store
                    if reply.status != 200:
                        raise Exception(
                            f"Failed to update upload ids to the dst gateway {dst_gateway.instance_name()}: {reply.data.decode('utf-8')}"
                        )

            # send chunk requests to source gateways
            chunk_batch = [cr.chunk for cr in batch if cr.chunk is not None]
            min_idx = queue_size.index(min(queue_size))
            n_added = 0
            while n_added < len(chunk_batch):
                # TODO: should update every source instance queue size
                server = src_gateways[min_idx]
                assert Chunk.from_dict(chunk_batch[0].as_dict()) == chunk_batch[0], f"Invalid chunk request: {chunk_batch[0].as_dict}"

                # TODO: make async
                st = time.time()
                reply = self.http_pool.request(
                    "POST",
                    f"{server.gateway_api_url}/api/v1/chunk_requests",
                    body=json.dumps([chunk.as_dict() for chunk in chunk_batch[n_added:]]).encode("utf-8"),
                    headers={"Content-Type": "application/json"},
                )
                if reply.status != 200:
                    raise Exception(f"Failed to dispatch chunk requests {server.instance_name()}: {reply.data.decode('utf-8')}")
                et = time.time()
                reply_json = json.loads(reply.data.decode("utf-8"))
                n_added += reply_json["n_added"]
                logger.fs.debug(f"Added {n_added} chunks to server {server} in {et-st}: {reply_json}")
                queue_size[min_idx] = reply_json["qsize"]  # update queue size
                # dont try again with some gateway
                min_idx = (min_idx + 1) % len(src_gateways)

            yield from chunk_batch

            # copy new multipart transfers to the multipart transfer list
            updated_len = len(self.chunker.multipart_upload_requests)
            self.multipart_transfer_list.extend(self.chunker.multipart_upload_requests[n_multiparts:updated_len])
            n_multiparts = updated_len

    def finalize(self):
        """Complete the multipart upload requests"""
        typer.secho(f"Finalizing multipart uploads...", fg="bright_black")
        groups = defaultdict(list)
        for req in self.multipart_transfer_list:
            if "region" not in req or "bucket" not in req:
                raise Exception(f"Invalid multipart upload request: {req}")
            groups[(req["region"], req["bucket"])].append(req)
        for key, group in groups.items():
            region, bucket = key
            batch_len = max(1, len(group) // 128)
            batches = [group[i : i + batch_len] for i in range(0, len(group), batch_len)]
            obj_store_interface = StorageInterface.create(region, bucket)

            def complete_fn(batch):
                for req in batch:
                    logger.fs.debug(f"Finalize upload id {req['upload_id']} for key {req['key']}")
                    retry_backoff(partial(obj_store_interface.complete_multipart_upload, req["key"], req["upload_id"]), initial_backoff=0.5)

            do_parallel(complete_fn, batches, n=8)

    def verify(self):
        """Verify the integrity of the transfered destination objects"""

        def verify_region(i):
            dst_iface = self.dst_ifaces[i]
            dst_prefix = self.dst_prefixes[i]

            # gather destination key mapping for this region
            dst_keys = {pair.dst_objs[dst_iface.region_tag()].key: pair.src_obj for pair in self.transfer_list}

            # list and check destination prefix
            for obj in dst_iface.list_objects(dst_prefix):
                # check metadata (src.size == dst.size) && (src.modified <= dst.modified)
                src_obj = dst_keys.get(obj.key)
                if src_obj and src_obj.size == obj.size and src_obj.last_modified <= obj.last_modified:
                    del dst_keys[obj.key]

            if dst_keys:
                # failed_keys = [obj.key for obj in dst_keys.values()]
                failed_keys = list(dst_keys.keys())
                if failed_keys == [""]:
                    return  # ignore empty key
                raise exceptions.TransferFailedException(
                    f"Destination {dst_iface.region_tag()} bucket {dst_iface.bucket()}: {len(dst_keys)} objects failed verification {failed_keys}"
                )

        n = 1  # number threads
        assert n == 1, "Only use one thread for verifying objects: n>1 causes concurrency error"
        do_parallel(
            verify_region,
            range(len(self.dst_ifaces)),
            spinner=True,
            spinner_persist=False,
            desc="Verifying objects in destination buckets",
            n=n,
        )

    def size_gb(self):
        """Return the size of the transfer in GB"""
        total_size = 0
        for pair in self.gen_transfer_pairs():
            total_size += pair.src_obj.size
        return total_size / 1e9


@dataclass
class TestCopyJob(CopyJob):
    # TODO: remove this class (unnecessary since we have TestObjectStore object)

    """Test copy which does not interact with object stores but uses random data generation on gateways"""

    def __init__(
        self,
        src_path: str,
        dst_paths: List[str] or str,
        recursive: bool = False,
        requester_pays: bool = False,
        transfer_config: Optional[TransferConfig] = None,
        uuid: str = field(init=False, default_factory=lambda: str(uuid.uuid4())),
        num_chunks: int = 10,
        chunk_size_bytes: int = 1024,
    ):
        super().__init__(src_path, dst_paths, recursive, requester_pays, transfer_config, uuid)
        self.num_chunks = num_chunks
        self.chunk_size_bytes = chunk_size_bytes


@dataclass
class SyncJob(CopyJob):
    """sync job that copies the source objects that does not exist in the destination bucket to the destination"""

    def __init__(
        self,
        src_path: str,
        dst_paths: List[str] or str,
        requester_pays: bool = False,
        transfer_config: Optional[TransferConfig] = None,
        uuid: str = field(init=False, default_factory=lambda: str(uuid.uuid4())),
    ):
        super().__init__(src_path, dst_paths, True, requester_pays, transfer_config, uuid)
        self.transfer_list = []
        self.multipart_transfer_list = []

        assert isinstance(self.src_iface, ObjectStoreInterface), "Source must be an object store interface"
        assert not any(
            [not isinstance(iface, ObjectStoreInterface) for iface in self.dst_ifaces]
        ), "Destination must be a object store interface"

    def gen_transfer_pairs(
        self,
        dataplane: Optional["Dataplane"] = None,
    ) -> Generator[TransferPair, None, None]:
        """Generate transfer pairs for the transfer job.

        :param chunker: chunker that makes the chunk requests
        :type chunker: Chunker
        """
        transfer_pair_gen = self.chunker.transfer_pair_generator(self.src_prefix, self.dst_prefixes, dataplane, self.recursive, self._pre_filter_fn)

        # only single destination supported
        assert len(self.dst_ifaces) == 1, "Only single destination supported for sync job"

        # enrich destination objects with metadata
        for src_obj, dest_obj in self._enrich_dest_objs(transfer_pair_gen, self.dst_prefixes):
            if self._post_filter_fn(src_obj, dest_obj):
                yield TransferPair(
                    src_obj=src_obj,
                    dst_objs={self.dst_ifaces[0].region_tag(): dest_obj},
                    dst_key=dest_obj.key.replace(self.dst_prefixes[0], ""),
                )

    def _enrich_dest_objs(
        self, transfer_pairs: Generator[TransferPair, None, None], dest_prefixes: List[str]
    ) -> Generator[Tuple[ObjectStoreObject, ObjectStoreObject], None, None]:
        """
        For skyplane sync, we enrich dest obj metadata with our existing dest obj metadata from the dest bucket following a query.

        :meta private:
        :param transfer_pairs: generator of transfer pairs
        :type transfer_pairs: Generator
        """
        for i in range(len(self.dst_ifaces)):
            dst_iface = self.dst_ifaces[i]
            dest_prefix = dest_prefixes[i]
            logger.fs.debug(f"Querying objects in {dst_iface.bucket()}")
            if not hasattr(self, "_found_dest_objs"):
                self._found_dest_objs = {obj.key: obj for obj in dst_iface.list_objects(dest_prefix)}
            for pair in transfer_pairs:
                src_obj = pair.src_obj
                dest_obj = list(pair.dst_objs.values())[0]
                assert len(list(pair.dst_objs.keys())) == 1, f"Multiple destinations are not support for sync: {pair.dst_objs}"
                if dest_obj.key in self._found_dest_objs:
                    dest_obj.size = self._found_dest_objs[dest_obj.key].size
                    dest_obj.last_modified = self._found_dest_objs[dest_obj.key].last_modified
                yield src_obj, dest_obj

    @classmethod
    def _post_filter_fn(cls, src_obj: ObjectStoreObject, dest_obj: ObjectStoreObject) -> bool:
        """Optionally filter destination objects after they are transferred.

        :param src_obj: source object to be transferred
        :type src_obj: ObjectStoreObject
        :param dest_obj: destination object transferred
        :type dest_obj: ObjectStoreObject
        """
        return not dest_obj.exists or (src_obj.last_modified > dest_obj.last_modified or src_obj.size != dest_obj.size)
