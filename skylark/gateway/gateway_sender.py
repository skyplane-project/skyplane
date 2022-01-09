from functools import partial
import queue
import socket
from contextlib import closing
from multiprocessing import Event, Manager, Process, Value
from typing import Dict, List, Optional

import requests
import setproctitle
from loguru import logger
from skylark import MB

from skylark.gateway.chunk import ChunkRequest
from skylark.gateway.chunk_store import ChunkStore
from skylark.utils.utils import Timer, wait_for


class GatewaySender:
    def __init__(self, chunk_store: ChunkStore, n_processes=1, max_batch_size_bytes=64 * MB):
        self.chunk_store = chunk_store
        self.n_processes = n_processes
        self.max_batch_size_bytes = max_batch_size_bytes
        self.processes = []

        # shared state
        self.manager = Manager()
        self.next_worker_id = Value("i", 0)
        self.worker_queue: queue.Queue[int] = self.manager.Queue()
        self.exit_flags = [Event() for _ in range(self.n_processes)]

        # process-local state
        self.worker_id: Optional[int] = None
        self.destination_ports: Dict[str, int] = {}  # ip_address -> int
        self.destination_sockets: Dict[str, socket.socket] = {}  # ip_address -> socket
        self.sent_chunk_ids: Dict[str, List[int]] = {}  # ip_address -> list of chunk_ids

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

        while not self.exit_flags[worker_id].is_set():
            # get all items from queue
            chunk_ids_to_send = []
            total_bytes = 0.0
            while True:
                # try to take up to max_batch_size_bytes from queue
                # avoids issue where one sender has more load than the others
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
                    chunk_ids = [req.chunk.chunk_id for req in chunks]
                    self.send_chunks(chunk_ids, next_hop.hop_ip_address)
                    if next_hop.hop_ip_address not in self.sent_chunk_ids:
                        self.sent_chunk_ids[next_hop.hop_ip_address] = []
                    self.sent_chunk_ids[next_hop.hop_ip_address].extend(chunk_ids)

        # close destination sockets
        for dst_socket in self.destination_sockets.values():
            dst_socket.close()
        
        # wait for all chunks to reach state "downloaded"
        def wait_for_chunks(chunk_ids):
            cr_status = {}
            for ip, ip_chunk_ids in self.sent_chunk_ids.items():
                response = requests.get(f"http://{ip}:8080/api/v1/chunk_requests")
                assert response.status_code == 200, f"{response.status_code} {response.text}"
                host_state = response.json()["chunk_requests"]
                for chunk_id in chunk_ids:
                    cr_status[chunk_id] = host_state[chunk_id]["state"]
            return all(cr_status[chunk_id] == "downloaded" for chunk_id in chunk_ids)
        
        wait_for(partial(wait_for_chunks, chunk_ids_to_send))

        # close servers
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
            self.destination_sockets[dst_host] = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.destination_sockets[dst_host].connect((dst_host, self.destination_ports[dst_host]))
            logger.info(f"[sender:{self.worker_id}] started new server connection to {dst_host}:{self.destination_ports[dst_host]}")
        sock = self.destination_sockets[dst_host]
        dst_port = self.destination_ports[dst_host]

        with Timer() as t:
            for idx, chunk_id in enumerate(chunk_ids):
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_CORK, 1)
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
        logger.info(f"[sender:{self.worker_id} -> {dst_port}] Sent {len(chunk_ids)} chunks in {t.elapsed:.2}s, {chunk_ids}")
