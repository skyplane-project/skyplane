import hashlib
import os
import queue
import select
import signal
import socket
from contextlib import closing
from multiprocessing import Event, Manager, Process, Value
from pathlib import Path
import time
from typing import List, Tuple

import requests
from loguru import logger

from skylark.gateway.chunk_store import ChunkRequest, ChunkStore
from skylark.gateway.wire_protocol_header import WireProtocolHeader
from skylark.utils import PathLike, Timer


class GatewaySender:
    def __init__(self, chunk_store: ChunkStore, n_processes=1, batch_size=1):
        self.chunk_store = chunk_store
        self.n_processes = n_processes
        self.batch_size = batch_size
        self.processes = []

        # shared state
        self.manager = Manager()
        self.next_worker_id = Value("i", 0)
        self.worker_queues: queue.Queue[int] = [self.manager.Queue() for _ in range(self.n_processes)]
        self.exit_flags = [Event() for _ in range(self.n_processes)]

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

    def worker_loop(self, id: int):
        while not self.exit_flags[id].is_set():
            # get up to pipeline_batch_size chunks from the queue
            chunk_ids_to_send = []
            while len(chunk_ids_to_send) < self.batch_size:
                try:
                    chunk_ids_to_send.append(self.worker_queues[id].get_nowait())
                except queue.Empty:
                    break

            # check next hop is the same for all chunks in the batch
            if chunk_ids_to_send:
                logger.debug(f"worker {id} sending {len(chunk_ids_to_send)} chunks")
                chunks = []
                for idx in chunk_ids_to_send:
                    self.chunk_store.pop_chunk_request_path(idx)
                    chunks.append(self.chunk_store.get_chunk_request(idx))
                next_hop = chunks[0].path[0]
                assert all(next_hop.hop_cloud_region == chunk.path[0].hop_cloud_region for chunk in chunks)
                assert all(next_hop.hop_ip_address == chunk.path[0].hop_ip_address for chunk in chunks)

                # send chunks
                chunk_ids = [req.chunk.chunk_id for req in chunks]
                self.send_chunks(chunk_ids, next_hop.hop_ip_address)
            time.sleep(0.1)  # short interval to batch requests

    def queue_request(self, chunk_request: ChunkRequest):
        # todo go beyond round robin routing? how to handle stragglers or variable-sized objects?
        with self.next_worker_id.get_lock():
            worker_id = self.next_worker_id.value
            logger.debug(f"queuing chunk request {chunk_request.chunk.chunk_id} to worker {worker_id}")
            self.worker_queues[worker_id].put(chunk_request.chunk.chunk_id)
            self.next_worker_id.value = (worker_id + 1) % self.n_processes

    def send_chunks(self, chunk_ids: List[int], dst_host="127.0.0.1"):
        """Send list of chunks to gateway server, pipelining small chunks together into a single socket stream."""
        # notify server of upcoming ChunkRequests
        # pop chunk_req.path[0] to remove self
        chunk_reqs = [self.chunk_store.get_chunk_request(chunk_id) for chunk_id in chunk_ids]
        response = requests.post(f"http://{dst_host}:8080/api/v1/chunk_requests", json=[c.as_dict() for c in chunk_reqs])
        assert response.status_code == 200 and response.json()["status"] == "ok"

        # contact server to set up socket connection
        response = requests.post(f"http://{dst_host}:8080/api/v1/servers")
        assert response.status_code == 200
        dst_port = int(response.json()["server_port"])

        logger.info(f"sending {len(chunk_ids)} chunks to {dst_host}:{dst_port}")
        with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
            sock.connect((dst_host, dst_port))
            for idx, chunk_id in enumerate(chunk_ids):
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_CORK, 1)  # disable Nagle's algorithm
                logger.warning(f"[sender] Sending chunk {chunk_id} to {dst_host}:{dst_port}")
                chunk = self.chunk_store.get_chunk_request(chunk_id).chunk

                # send chunk header
                chunk_file_path = self.chunk_store.get_chunk_file_path(chunk_id)
                header = chunk.to_wire_header(end_of_stream=idx == len(chunk_ids) - 1)
                sock.sendall(header.to_bytes())
                logger.debug(f"[sender] Sent chunk header {header}")

                # send chunk data
                assert chunk_file_path.exists(), f"chunk file {chunk_file_path} does not exist"
                with open(chunk_file_path, "rb") as fd:
                    logger.debug(f"[sender] Sending file")
                    bytes_sent = sock.sendfile(fd)
                    logger.debug(f"[sender] Sent chunk data {bytes_sent} bytes")

                self.chunk_store.finish_upload(chunk_id)
                chunk_file_path.unlink()
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_CORK, 0)  # send remaining packets

        # close server
        response = requests.delete(f"http://{dst_host}:8080/api/v1/servers/{dst_port}")
        assert response.status_code == 200 and response.json() == {"status": "ok"}

        # move chunk_reqs from downloaded to uploaded
        for chunk_req in chunk_reqs:
            self.chunk_store.mark_chunk_request_uploaded(chunk_req)
