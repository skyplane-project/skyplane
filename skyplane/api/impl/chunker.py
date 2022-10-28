import math
import queue
import threading
from queue import Queue
from typing import Generator, List, Tuple

from skyplane import MB
from skyplane.chunk import Chunk, ChunkRequest
from skyplane.obj_store.object_store_interface import ObjectStoreInterface, ObjectStoreObject
from skyplane.utils.timer import Timer
from skyplane.utils import logger


class Chunker:
    def __init__(
        self,
        src_iface: ObjectStoreInterface,
        dest_iface: ObjectStoreInterface,
        multipart_enabled: bool = True,
        multipart_threshold_mb: int = 128,
        multipart_chunk_size_mb: int = 64,
        multipart_max_chunks: int = 10000,
        concurrent_multipart_chunk_threads: int = 64,
    ):
        self.src_iface = src_iface
        self.dest_iface = dest_iface
        self.multipart_enabled = multipart_enabled
        self.multipart_threshold_mb = multipart_threshold_mb
        self.multipart_chunk_size_mb = multipart_chunk_size_mb
        self.multipart_max_chunks = multipart_max_chunks
        self.multipart_upload_requests = []
        self.concurrent_multipart_chunk_threads = concurrent_multipart_chunk_threads

    def multipart_chunk_thread(
        self,
        exit_event: threading.Event,
        in_queue: "Queue[Tuple[ObjectStoreObject, ObjectStoreObject]]",
        out_queue: "Queue[Chunk]",
        dest_iface: ObjectStoreInterface,
    ):
        """Chunks large files into many small chunks."""
        while not exit_event.is_set():
            try:
                input_data = in_queue.get(block=False, timeout=0.1)
            except queue.Empty:
                continue

            # get source and destination object and then compute number of chunks
            src_object, dest_object = input_data
            with Timer(f"initiate_multipart_upload {src_object}"):
                upload_id = dest_iface.initiate_multipart_upload(dest_object.key)
            chunk_size_bytes = int(self.multipart_chunk_size_mb * MB)
            num_chunks = math.ceil(src_object.size / chunk_size_bytes)
            if num_chunks > self.multipart_max_chunks:
                chunk_size_bytes = int(src_object.size / self.multipart_max_chunks)
                chunk_size_bytes = math.ceil(chunk_size_bytes / MB) * MB  # round to next largest mb
                num_chunks = math.ceil(src_object.size / chunk_size_bytes)

            # create chunks
            offset = 0
            part_num = 1
            parts = []
            for _ in range(num_chunks):
                file_size_bytes = min(chunk_size_bytes, src_object.size - offset)  # size is min(chunk_size, remaining data)
                assert file_size_bytes > 0, f"file size <= 0 {file_size_bytes}"
                chunk = Chunk(
                    src_key=src_object.key,
                    dest_key=dest_object.key,
                    chunk_id=-1,
                    file_offset_bytes=offset,
                    chunk_length_bytes=file_size_bytes,
                    part_number=part_num,
                    upload_id=upload_id,
                )
                offset += chunk_size_bytes
                parts.append(part_num)
                part_num += 1
                out_queue.put(chunk)
            self.multipart_upload_requests.append(dict(upload_id=upload_id, key=dest_object.key, parts=parts))

    def chunk(
        self, transfer_pair_generator: Generator[Tuple[ObjectStoreObject, ObjectStoreObject], None, None]
    ) -> Generator[Chunk, None, None]:
        """Break transfer list into chunks."""
        multipart_send_queue: Queue[Tuple[ObjectStoreObject, ObjectStoreObject]] = Queue()
        multipart_chunk_queue: Queue[Chunk] = Queue()
        multipart_exit_event = threading.Event()
        multipart_chunk_threads = []

        # start chunking threads
        for _ in range(self.concurrent_multipart_chunk_threads):
            t = threading.Thread(
                target=self.multipart_chunk_thread,
                args=(multipart_exit_event, multipart_send_queue, multipart_chunk_queue, self.dest_iface),
                daemon=False,
            )
            t.start()
            multipart_chunk_threads.append(t)

        # begin chunking loop
        current_idx = 0
        for src_obj, dst_obj in transfer_pair_generator:
            if self.multipart_enabled and src_obj.size > self.multipart_threshold_mb * MB:
                multipart_send_queue.put((src_obj, dst_obj))
            else:
                yield Chunk(
                    src_key=src_obj.key,
                    dest_key=dst_obj.key,
                    chunk_id=current_idx,
                    chunk_length_bytes=src_obj.size,
                )
                current_idx += 1

            # drain multipart chunk queue and yield with updated chunk IDs
            while not multipart_chunk_queue.empty():
                chunk = multipart_chunk_queue.get()
                chunk.chunk_id = current_idx
                yield chunk
                current_idx += 1

        # send sentinel to all threads
        multipart_exit_event.set()
        for thread in multipart_chunk_threads:
            thread.join()

        # drain multipart chunk queue and yield with updated chunk IDs
        while not multipart_chunk_queue.empty():
            chunk = multipart_chunk_queue.get()
            chunk.chunk_id = current_idx
            yield chunk
            current_idx += 1

    def to_chunk_requests(self, gen_in):
        """Converts a generator of chunks to a generator of chunk requests."""
        for chunk in gen_in:
            yield ChunkRequest(
                chunk=chunk,
                src_region=self.src_iface.region_tag(),
                dst_region=self.dest_iface.region_tag(),
                src_object_store_bucket=self.src_iface.bucket(),
                dst_object_store_bucket=self.dest_iface.bucket(),
                src_type="object_store",
                dst_type="object_store",
            )


def batch_generator(gen_in, batch_size: int):
    """Batches generator, while handling StopIteration"""
    batch = []
    for item in gen_in:
        logger.fs.debug(f"batch_generator: {item=}")
        batch.append(item)
        if len(batch) == batch_size:
            logger.fs.debug(f"batch_generator: yielding batch {batch=}")
            yield batch
            batch = []
    if len(batch) > 0:
        yield batch


def tail_generator(gen_in, out_list: List):
    """Tails generator"""
    for item in gen_in:
        out_list.append(item)
        yield item
