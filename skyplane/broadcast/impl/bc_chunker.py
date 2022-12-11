import threading
import queue
import uuid
import math

from typing import Optional, Generator, List, Tuple, TypeVar
from skyplane.chunk import ChunkRequest, ChunkState, Chunk
from skyplane.api.config import TransferConfig
from skyplane.chunk import Chunk
from skyplane.obj_store.object_store_interface import ObjectStoreInterface, ObjectStoreObject
from skyplane.utils.definitions import MB
from skyplane.api.transfer_job import Chunker
from skyplane.utils import logger
from queue import Queue

T = TypeVar("T")


class BCChunker(Chunker):
    def __init__(
        self,
        src_iface: Optional[ObjectStoreInterface] = None,
        dest_ifaces: Optional[List[ObjectStoreInterface]] = None,
        transfer_config: Optional[TransferConfig] = None,
        num_partitions: Optional[int] = 2,
        concurrent_multipart_chunk_threads: Optional[int] = 64,
    ):
        # read/ write to object store 
        if src_iface is not None:
            self.dest_iface = dest_ifaces[0]
            super().__init__(src_iface, self.dest_iface, transfer_config, concurrent_multipart_chunk_threads)
            self.dest_ifaces = dest_ifaces
        else:  # random generate data
            assert transfer_config is not None
            assert transfer_config.multipart_enabled is False
            self.transfer_config = transfer_config
            self.src_region = transfer_config.src_region
            self.dst_regions = transfer_config.dst_regions

        self.num_partitions = num_partitions
        self.multipart_upload_requests = []
        self.all_mappings_for_upload_ids = []

    def _run_multipart_chunk_thread(
        self, exit_event: threading.Event, in_queue: "Queue[Tuple[ObjectStoreObject, ObjectStoreObject]]", out_queue: "Queue[Chunk]"
    ):
        """Chunks large files into many small chunks."""
        while not exit_event.is_set():
            try:
                input_data = in_queue.get(block=False, timeout=0.1)  # get data (one piece across destinations)
            except queue.Empty:
                continue

            # get source and destination object (dummy) and then compute number of chunks
            src_object, dest_object = input_data
            mime_type = self.src_iface.get_obj_mime_type(src_object.key)

            region_bucketkey_to_upload_id = {}
            for dest_iface in self.dest_ifaces:
                region_bucketkey_to_upload_id[
                    dest_iface.region_tag() + ":" + dest_iface.bucket() + ":" + dest_object.key
                ] = dest_iface.initiate_multipart_upload(dest_object.key, mime_type=mime_type)

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
                    partition_id=str(part_num % self.num_partitions),
                    chunk_length_bytes=file_size_bytes,
                    part_number=part_num,
                    multi_part=True,
                )
                offset += file_size_bytes
                parts.append(part_num)
                part_num += 1
                out_queue.put(chunk)

            # maintain multipart upload requests for multiple regions
            # Dict[region] = upload id for this region
            self.multipart_upload_requests.append(
                dict(
                    key=dest_object.key,
                    parts=parts,
                    region_bucketkey_to_upload_id=region_bucketkey_to_upload_id,
                    dest_ifaces=self.dest_ifaces,
                )
            )
            self.all_mappings_for_upload_ids.append(region_bucketkey_to_upload_id)
            # print("Multipart upload request: ", self.multipart_upload_requests)

    def transfer_pair_random_generator(self) -> Generator[Tuple[ObjectStoreObject, ObjectStoreObject], None, None]:
        """Generate random transfer pairs"""
        assert self.transfer_config.gen_random_data

        n_chunks = self.transfer_config.num_random_chunks
        random_chunk_size_mb = self.transfer_config.random_chunk_size_mb

        n_objs = 0
        for i in range(n_chunks):
            src_obj = ObjectStoreObject(self.src_region.split(":")[0], "", "chunk_" + str(i), size=random_chunk_size_mb * MB)
            dst_obj = ObjectStoreObject(self.dst_regions[0].split(":")[0], "", "chunk_" + str(i), size=random_chunk_size_mb * MB)
            n_objs += 1
            logger.fs.debug(f"Yield: {src_obj}, {dst_obj}")
            yield src_obj, dst_obj

        if n_objs == 0:
            logger.error("No object was created from bc random generator.\n")
            raise exceptions.MissingObjectException(f"No objects were created from bc random generator")

    def to_chunk_requests(self, gen_in: Generator[Chunk, None, None]) -> Generator[ChunkRequest, None, None]:
        """Converts a generator of chunks to a generator of chunk requests."""
        # read from object store
        if not self.transfer_config.gen_random_data:
            assert self.src_iface is not None
            src_region = self.src_iface.region_tag()
            dest_region = self.dst_iface.region_tag()
            src_bucket = self.src_iface.bucket()
            dest_bucket = self.dst_iface.bucket()
            src_type = "object_store"
            dst_type = src_type

        # read from random data generator
        else:
            src_region = self.src_region
            dest_region = self.dst_regions[0]
            src_bucket = None
            dest_bucket = None 
            src_type = "random"
            dst_type = "save_local"

        for chunk in gen_in:
            yield ChunkRequest(
                chunk=chunk,
                src_region=src_region,
                src_random_size_mb=self.transfer_config.random_chunk_size_mb, 
                dst_region=dest_region,
                src_object_store_bucket=src_bucket,
                dst_object_store_bucket=dest_bucket,
                src_type=src_type,
                dst_type=dst_type,
            )

    def chunk(
        self, transfer_pair_generator: Generator[Tuple[ObjectStoreObject, ObjectStoreObject], None, None]
    ) -> Generator[Chunk, None, None]:
        """Break transfer list into chunks."""
        # maintain a send queue across destinations, dest obj is only a dummy var in broadcast setting (assume dst_obj.key is the same across dsts)
        multipart_send_queue: Queue[Tuple[ObjectStoreObject, ObjectStoreObject]] = Queue()

        # maintain a queue of chunks across destinations
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
        idx = 0
        for src_obj, dst_obj in transfer_pair_generator:
            if self.transfer_config.multipart_enabled and src_obj.size > self.transfer_config.multipart_threshold_mb * MB:
                # dummy dst_obj
                multipart_send_queue.put((src_obj, dst_obj))
            else:
                # Ignore the pair of folders
                if src_obj.size == 0:
                    assert dst_obj.size is None
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
