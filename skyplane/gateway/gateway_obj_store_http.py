import queue
import time
import traceback
from dataclasses import dataclass
from functools import partial
from multiprocessing import Event, Manager, Process, Value, Queue
from typing import Dict, Optional

from skyplane.config_paths import cloud_config
from skyplane.chunk import ChunkRequest
from skyplane.gateway.chunk_store import ChunkStore
from skyplane.obj_store.object_store_interface import ObjectStoreInterface
from skyplane.utils import logger
from skyplane.utils.retry import retry_backoff


@dataclass
class ObjStoreRequest:
    chunk_req: ChunkRequest
    req_type: str


class GatewayHttpConn:
    def __init__(self, chunk_store: ChunkStore, error_event, error_queue: Queue, max_conn=1):
        self.chunk_store = chunk_store
        self.error_event = error_event
        self.error_queue = error_queue
        self.n_processes = max_conn
        self.processes = []
        self.src_requester_pays = cloud_config.get_flag("requester_pays")

        # shared state
        self.manager = Manager()
        self.next_worker_id = Value("i", 0)
        self.worker_download_queue: queue.Queue[ObjStoreRequest] = self.manager.Queue()
        self.worker_upload_queue: queue.Queue[ObjStoreRequest] = self.manager.Queue()
        self.exit_flags = [Event() for _ in range(self.n_processes)]

        # process-local state
        self.worker_id: Optional[int] = None
        self.obj_store_interfaces: Dict[str, ObjectStoreInterface] = {}

    # interact with object store
    def get_obj_store_interface(self, region: str, bucket: str) -> ObjectStoreInterface:
        key = f"{region}:{bucket}"
        if key not in self.obj_store_interfaces:
            logger.warning(f"[gateway_daemon] ObjectStoreInterface not cached for {key}")
            try:
                self.obj_store_interfaces[key] = ObjectStoreInterface.create(region, bucket)
            except Exception as e:
                raise ValueError(f"Failed to create obj store interface {str(e)}")
        return self.obj_store_interfaces[key]

    def start_workers(self):
        # logger.log(f"Start worker: {self.n_processes}")
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
        self.worker_id = worker_id
        while not self.exit_flags[worker_id].is_set() and not self.error_event.is_set():
            try:
                # Http download request is an upload request
                try:
                    request = self.worker_upload_queue.get_nowait()
                    chunk_req = request.chunk_req
                    req_type = request.req_type
                except queue.Empty:
                    continue

                assert req_type == "http"

                # wait for free space
                while self.chunk_store.remaining_bytes() < chunk_req.chunk.chunk_length_bytes * self.n_processes:
                    time.sleep(0.1)

                bucket = chunk_req.dst_object_store_bucket
                self.chunk_store.state_start_upload(chunk_req.chunk.chunk_id, f"obj_store:{self.worker_id}")
                logger.debug(
                    f"[obj_store:{self.worker_id}] Start upload {chunk_req.chunk.chunk_id} to {bucket}, key {chunk_req.chunk.dest_key}"
                )

                obj_store_interface = self.get_obj_store_interface(chunk_req.dst_region, bucket)
                # sleep_time = random.uniform(0.8, 1.2)
                # time.sleep(sleep_time) # Avoid high I/O process
                retry_backoff(
                    partial(
                        obj_store_interface.download_http,
                        chunk_req,
                    ),
                    max_retries=4,
                )
                self.chunk_store.state_finish_upload(chunk_req.chunk.chunk_id, f"obj_store:{self.worker_id}")
                logger.debug(f"[obj_store:{self.worker_id}] Uploaded {chunk_req.chunk.chunk_id} to {bucket}")

            except Exception as e:
                logger.exception(f"[obj_store:{self.worker_id}] Exception in worker loop: {str(e)}")
                self.error_queue.put(traceback.format_exc())
                self.exit_flags[worker_id].set()
                self.error_event.set()
        # close destination sockets
        logger.info(f"[obj_store:{worker_id}] exiting")

    def queue_upload_request(self, chunk_request: ChunkRequest):
        self.worker_upload_queue.put(ObjStoreRequest(chunk_request, "http"))

    def queue_download_request(self, chunk_request: ChunkRequest):
        self.worker_download_queue.put(ObjStoreRequest(chunk_request, "download"))
