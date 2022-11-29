import threading
from queue import Queue

from typing import Generator, Tuple, TypeVar
import uuid

from skyplane.api.transfer_config import TransferConfig
from skyplane.chunk import Chunk, ChunkRequest
from skyplane.obj_store.object_store_interface import ObjectStoreInterface, ObjectStoreObject
from skyplane.utils.definitions import MB
from skyplane.api.impl.chunker import Chunker

T = TypeVar("T")


class BCChunker(Chunker):
    def __init__(
        self,
        src_iface: ObjectStoreInterface,
        dest_iface: ObjectStoreInterface,
        transfer_config: TransferConfig,
        num_partitions: int = 10,
        concurrent_multipart_chunk_threads: int = 64,
    ):
        super().__init__(src_iface, dest_iface, transfer_config, concurrent_multipart_chunk_threads)
        self.num_partitions = num_partitions

    def to_bc_chunk_requests(self, gen_in: Generator[Chunk, None, None]) -> Generator[ChunkRequest, None, None]:
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
                    args=(multipart_exit_event, multipart_send_queue, multipart_chunk_queue),
                    daemon=False,
                )
                t.start()
                multipart_chunk_threads.append(t)

        # begin chunking loop
        idx = 0
        for src_obj, dst_obj in transfer_pair_generator:
            if self.transfer_config.multipart_enabled and src_obj.size > self.transfer_config.multipart_threshold_mb * MB:
                multipart_send_queue.put((src_obj, dst_obj))
            else:
                yield Chunk(
                    src_key=src_obj.key,
                    dest_key=dst_obj.key,
                    partition_id=str(idx % self.num_partitions),
                    chunk_id=uuid.uuid4().hex,
                    chunk_length_bytes=src_obj.size,
                )

            if self.transfer_config.multipart_enabled:
                # drain multipart chunk queue and yield with updated chunk IDs
                while not multipart_chunk_queue.empty():
                    yield multipart_chunk_queue.get()
            idx += 1

        if self.transfer_config.multipart_enabled:
            # send sentinel to all threads
            multipart_exit_event.set()
            for thread in multipart_chunk_threads:
                thread.join()

            # drain multipart chunk queue and yield with updated chunk IDs
            while not multipart_chunk_queue.empty():
                yield multipart_chunk_queue.get()
