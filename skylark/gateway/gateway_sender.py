import queue
import socket
import time
from contextlib import closing
from multiprocessing import Event, Manager, Process, Value
from typing import Dict, List

import requests
import setproctitle
from loguru import logger
from skylark import MB

from skylark.gateway.chunk import ChunkRequest, WireProtocolHeader
from skylark.gateway.chunk_store import ChunkStore


class GatewaySender:
    def __init__(self, chunk_store: ChunkStore, n_processes=1, max_batch_size_mb=16):
        self.chunk_store = chunk_store
        self.n_processes = n_processes
        self.max_batch_size_bytes = max_batch_size_mb * MB
        self.processes = []

        # shared state
        self.manager = Manager()
        self.next_worker_id = Value("i", 0)
        self.worker_queue: queue.Queue[int] = self.manager.Queue()
        self.exit_flags = [Event() for _ in range(self.n_processes)]

        # process-local state
        self.worker_id: int = None
        self.destination_ports: Dict[str, int] = None  # ip_address -> port

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
        setproctitle.setproctitle(f"skylark-gateway-sender:{worker_id}")
        self.worker_id = worker_id
        self.destination_ports = {}
        while not self.exit_flags[worker_id].is_set():
            # get all items from queue
            chunk_ids_to_send = []
            total_bytes = 0.0
            while True:
                try:
                    chunk_ids_to_send.append(self.worker_queue.get_nowait())
                    total_bytes = sum(
                        self.chunk_store.get_chunk_request(chunk_id).chunk.chunk_length_bytes for chunk_id in chunk_ids_to_send
                    )
                    if total_bytes > self.max_batch_size_bytes:
                        break
                except queue.Empty:
                    break

            if len(chunk_ids_to_send) > 0:
                # check next hop is the same for all chunks in the batch
                if chunk_ids_to_send:
                    logger.debug(f"[sender:{worker_id}] sending {len(chunk_ids_to_send)} chunks, {chunk_ids_to_send}")
                    chunks = []
                    for idx in chunk_ids_to_send:
                        self.chunk_store.pop_chunk_request_path(idx)
                        req = self.chunk_store.get_chunk_request(idx)
                        chunks.append(req)
                    next_hop = chunks[0].path[0]
                    assert all(next_hop.hop_cloud_region == chunk.path[0].hop_cloud_region for chunk in chunks)
                    assert all(next_hop.hop_ip_address == chunk.path[0].hop_ip_address for chunk in chunks)

                    # send chunks
                    logger.debug(
                        f"[sender:{worker_id}] Sending {len(chunks)} chunks ({total_bytes / MB:.2f}MB) to {next_hop.hop_ip_address}, {chunk_ids_to_send}"
                    )
                    chunk_ids = [req.chunk.chunk_id for req in chunks]
                    self.send_chunks(chunk_ids, next_hop.hop_ip_address)

        # close destination sockets
        for dst_host, dst_port in self.destination_ports.items():
            response = requests.delete(f"http://{dst_host}:8080/api/v1/servers/{dst_port}")
            assert response.status_code == 200 and response.json() == {"status": "ok"}, response.json()
            logger.info(f"[sender:{worker_id}] closed destination socket {dst_host}:{dst_port}")

    def queue_request(self, chunk_request: ChunkRequest):
        self.worker_queue.put(chunk_request.chunk.chunk_id)

    def send_chunks(self, chunk_ids: List[int], dst_host: str):
        """Send list of chunks to gateway server, pipelining small chunks together into a single socket stream."""
        # notify server of upcoming ChunkRequests
        chunk_reqs = [self.chunk_store.get_chunk_request(chunk_id) for chunk_id in chunk_ids]
        response = requests.post(f"http://{dst_host}:8080/api/v1/chunk_requests", json=[c.as_dict() for c in chunk_reqs])
        assert response.status_code == 200 and response.json()["status"] == "ok"

        # contact server to set up socket connection
        if self.destination_ports.get(dst_host) is None:
            response = requests.post(f"http://{dst_host}:8080/api/v1/servers")
            assert response.status_code == 200
            self.destination_ports[dst_host] = int(response.json()["server_port"])
            logger.info(f"[sender:{self.worker_id}] started new server connection to {dst_host}:{self.destination_ports[dst_host]}")
        dst_port = self.destination_ports[dst_host]

        with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
            sock.connect((dst_host, dst_port))
            for idx, chunk_id in enumerate(chunk_ids):
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_CORK, 1)  # disable Nagle's algorithm
                self.chunk_store.state_start_upload(chunk_id)
                chunk = self.chunk_store.get_chunk_request(chunk_id).chunk

                # send chunk header
                chunk.to_wire_header(n_chunks_left_on_socket=len(chunk_ids) - idx - 1).to_socket(sock)

                # send chunk data
                chunk_file_path = self.chunk_store.get_chunk_file_path(chunk_id)
                assert chunk_file_path.exists(), f"chunk file {chunk_file_path} does not exist"
                with open(chunk_file_path, "rb") as fd:
                    bytes_sent = sock.sendfile(fd)
                self.chunk_store.state_finish_upload(chunk_id)
                chunk_file_path.unlink()
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_CORK, 0)  # send remaining packets
            logger.info(f"[sender:{self.worker_id} -> {dst_port}] Sent {len(chunk_ids)} chunks, {chunk_ids}")
