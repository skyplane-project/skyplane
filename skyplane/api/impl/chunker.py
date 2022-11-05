import queue
import threading
import time
from queue import Queue

import math
from typing import Generator, List, Optional, Tuple, TypeVar
import uuid

from skyplane.api.transfer_config import TransferConfig
from skyplane.chunk import Chunk, ChunkRequest
from skyplane.obj_store.object_store_interface import ObjectStoreInterface, ObjectStoreObject
from skyplane.utils import logger
from skyplane.utils.definitions import MB

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

    def multipart_chunk_thread(
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
                    target=self.multipart_chunk_thread,
                    args=(multipart_exit_event, multipart_send_queue, multipart_chunk_queue, self.dest_iface),
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


def tail_generator(gen_in: Generator[T, None, None], out_list: List[T]) -> Generator[T, None, None]:
    """Tails generator while handling StopIteration"""
    for item in gen_in:
        out_list.append(item)
        yield item


def profile_generator(gen_in: Generator[T, None, None], desc: Optional[str] = None, log_every: int = 5) -> Generator[T, None, None]:
    """Profile generator while handling StopIteration"""
    count = 0
    total_time = 0.0
    start = time.time()
    for item in gen_in:
        end = time.time()
        total_time += end - start
        count += 1
        if count % log_every == 0:
            logger.fs.debug(f"[profile_generator] {desc=}: {total_time / count * 1000:.4f} ms per item")
        yield item
