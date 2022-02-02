import queue
import threading
import socket
from multiprocessing import Event, Manager, Process, Value
from typing import Dict, List, Optional

import requests
import setproctitle
from skylark.utils import logger
from skylark import MB
from skylark.chunk import ChunkRequest
from skylark.gateway.chunk_store import ChunkStore
from skylark.utils.utils import Timer, wait_for

import concurrent.futures
from skylark.obj_store.object_store_interface import ObjectStoreInterface

from dataclasses import dataclass


@dataclass
class ObjStoreRequest:
    chunk_req: ChunkRequest
    req_type: str


class GatewayObjStoreConn:
    def __init__(self, chunk_store, max_conn=32):

        self.chunk_store = chunk_store
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
            self.obj_store_interfaces[key] = ObjectStoreInterface.create(region, bucket)
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

    def download(region, bucket, fpath, key):
        obj_store_interface = self.get_obj_store_interface(region, bucket)

    def worker_loop(self, worker_id: int):
        setproctitle.setproctitle(f"skylark-gateway-obj-store:{worker_id}")
        self.worker_id = worker_id

        while not self.exit_flags[worker_id].is_set():
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
                region = chunk_req.dst_region
                bucket = chunk_req.dst_object_store_bucket

                self.chunk_store.state_start_upload(chunk_req.chunk.chunk_id)

                logger.debug(f"[obj_store:{self.worker_id}] Start upload {chunk_req.chunk.chunk_id} to {bucket}")

                def upload(region, bucket, fpath, key, chunk_id):
                    obj_store_interface = self.get_obj_store_interface(region, bucket)
                    obj_store_interface.upload_object(fpath, key).result()

                    # update chunk state
                    self.chunk_store.state_finish_upload(chunk_id)
                    logger.debug(f"[obj_store:{self.worker_id}] Uploaded {chunk_id} to {bucket}")

                # wait for upload in seperate thread
                threading.Thread(target=upload, args=(region, bucket, fpath, chunk_req.chunk.key, chunk_req.chunk.chunk_id)).start()

            elif req_type == "download":
                assert chunk_req.src_type == "object_store"
                region = chunk_req.src_region
                bucket = chunk_req.src_object_store_bucket

                logger.debug(f"[obj_store:{self.worker_id}] Starting download {chunk_req.chunk.chunk_id} from {bucket}")

                def download(region, bucket, fpath, key, chunk_id):
                    obj_store_interface = self.get_obj_store_interface(region, bucket)
                    obj_store_interface.download_object(key, fpath).result()

                    # update chunk state
                    self.chunk_store.state_finish_download(chunk_id)
                    logger.debug(f"[obj_store:{self.worker_id}] Downloaded {chunk_id} from {bucket}")

                # wait for request to return in sepearte thread, so we can update chunk state
                threading.Thread(target=download, args=(region, bucket, fpath, chunk_req.chunk.key, chunk_req.chunk.chunk_id)).start()

            else:
                raise ValueError(f"Invalid location for chunk req, {req_type}: {chunk_req.src_type}->{chunk_req.dst_type}")

        # close destination sockets
        logger.info(f"[obj_store:{worker_id}] exiting")

        # TODO: wait for uploads to finish (check chunk exists)

    def queue_request(self, chunk_request: ChunkRequest, request_type: str):
        self.worker_queue.put(ObjStoreRequest(chunk_request, request_type))
