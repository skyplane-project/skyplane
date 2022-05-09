from functools import partial
import queue
from multiprocessing import Event, Manager, Process, Value, Queue
import time
import traceback
from typing import Dict, Optional

from skylark.gateway.chunk_store import ChunkStore
from skylark.utils import logger
from skylark.chunk import ChunkRequest

from skylark.obj_store.object_store_interface import ObjectStoreInterface

from dataclasses import dataclass

from skylark.utils.utils import retry_backoff


@dataclass
class ObjStoreRequest:
    chunk_req: ChunkRequest
    req_type: str


class GatewayObjStoreConn:
    def __init__(self, chunk_store: ChunkStore, error_event, error_queue: Queue, max_conn=1):
        self.chunk_store = chunk_store
        self.error_event = error_event
        self.error_queue = error_queue
        self.n_processes = max_conn
        self.processes = []

        # shared state
        self.manager = Manager()
        self.next_worker_id = Value("i", 0)
        self.worker_queue: queue.Queue[ObjStoreRequest] = self.manager.Queue()
        self.exit_flags = [Event() for _ in range(self.n_processes)]

        # process-local state
        self.worker_id: Optional[int] = None
        self.obj_store_interfaces: Dict[str, ObjectStoreInterface] = {}

    # interact with object store
    def get_obj_store_interface(self, region: str, bucket: str) -> ObjectStoreInterface:
        key = f"{region}:{bucket}"
        if key not in self.obj_store_interfaces:
            logger.warning(f"[gateway_daemon] ObjectStoreInferface not cached for {key}")
            try:
                self.obj_store_interfaces[key] = ObjectStoreInterface.create(region, bucket)
            except Exception as e:
                raise ValueError(f"Failed to create obj store interface {str(e)}")
        return self.obj_store_interfaces[key]

    def start_workers(self):
        for i in range(self.n_processes):
            p = Process(target=self.worker_loop, args=(i,))
            p.start()
            self.processes.append(p)

    def stop_workers(self):
        for i in range(self.n_processes):
            self.exit_flags[i].set()
        for p in self.processes:
            p.join()
        self.processes = []

    def worker_loop(self, worker_id: int):
        # todo should this use processes instead of threads?
        self.worker_id = worker_id
        while not self.exit_flags[worker_id].is_set() and not self.error_event.is_set():
            try:
                try:
                    request = self.worker_queue.get_nowait()
                    chunk_req = request.chunk_req
                    req_type = request.req_type
                except queue.Empty:
                    continue
                fpath = str(self.chunk_store.get_chunk_file_path(chunk_req.chunk.chunk_id).absolute())
                logger.debug(f"[obj_store:{self.worker_id}] Received chunk ID {chunk_req.chunk.chunk_id}")

                if req_type == "upload":
                    assert chunk_req.dst_type == "object_store"
                    bucket = chunk_req.dst_object_store_bucket
                    self.chunk_store.state_start_upload(chunk_req.chunk.chunk_id, f"obj_store:{self.worker_id}")
                    logger.debug(
                        f"[obj_store:{self.worker_id}] Start upload {chunk_req.chunk.chunk_id} to {bucket}, key {chunk_req.chunk.dest_key}"
                    )

                    obj_store_interface = self.get_obj_store_interface(chunk_req.dst_region, bucket)
                    retry_backoff(
                        partial(
                            obj_store_interface.upload_object,
                            fpath,
                            chunk_req.chunk.dest_key,
                            chunk_req.chunk.part_number,
                            chunk_req.chunk.upload_id,
                        ),
                        max_retries=4,
                    )
                    chunk_file_path = self.chunk_store.get_chunk_file_path(chunk_req.chunk.chunk_id)
                    self.chunk_store.state_finish_upload(chunk_req.chunk.chunk_id, f"obj_store:{self.worker_id}")
                    chunk_file_path.unlink()
                    logger.debug(f"[obj_store:{self.worker_id}] Uploaded {chunk_req.chunk.dest_key} to {bucket}")
                elif req_type == "download":
                    assert chunk_req.src_type == "object_store"

                    # wait for free space
                    while self.chunk_store.remaining_bytes() < chunk_req.chunk.chunk_length_bytes * self.n_processes:
                        time.sleep(0.1)

                    bucket = chunk_req.src_object_store_bucket
                    self.chunk_store.state_start_download(chunk_req.chunk.chunk_id, f"obj_store:{self.worker_id}")
                    logger.debug(f"[obj_store:{self.worker_id}] Start download {chunk_req.chunk.chunk_id} from {bucket}")

                    obj_store_interface = self.get_obj_store_interface(chunk_req.src_region, bucket)
                    retry_backoff(
                        partial(
                            obj_store_interface.download_object,
                            chunk_req.chunk.src_key,
                            fpath,
                            chunk_req.chunk.file_offset_bytes,
                            chunk_req.chunk.chunk_length_bytes,
                        ),
                        max_retries=4,
                    )
                    self.chunk_store.state_finish_download(chunk_req.chunk.chunk_id, f"obj_store:{self.worker_id}")
                    recieved_chunk_size = self.chunk_store.get_chunk_file_path(chunk_req.chunk.chunk_id).stat().st_size
                    assert (
                        recieved_chunk_size == chunk_req.chunk.chunk_length_bytes
                    ), f"Downloaded chunk {chunk_req.chunk.chunk_id} has incorrect size (expected {chunk_req.chunk.chunk_length_bytes} but got {recieved_chunk_size})"
                    logger.debug(f"[obj_store:{self.worker_id}] Downloaded {chunk_req.chunk.src_key} from {bucket}")
                else:
                    raise ValueError(f"Invalid location for chunk req, {req_type}: {chunk_req.src_type}->{chunk_req.dst_type}")
            except Exception as e:
                logger.exception(f"[obj_store:{self.worker_id}] Exception in worker loop: {str(e)}")
                self.error_queue.put(traceback.format_exc())
                self.exit_flags[worker_id].set()
                self.error_event.set()
        # close destination sockets
        logger.info(f"[obj_store:{worker_id}] exiting")

        # TODO: wait for uploads to finish (check chunk exists)

    def queue_request(self, chunk_request: ChunkRequest, request_type: str):
        logger.debug(f"[gateway_daemon] Queueing chunk request {chunk_request.chunk.chunk_id}")
        self.worker_queue.put(ObjStoreRequest(chunk_request, request_type))
