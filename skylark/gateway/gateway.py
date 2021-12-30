import hashlib
import os
import select
import signal
import socket
from contextlib import closing
from multiprocessing import Process, Value
from pathlib import Path
from typing import List, Tuple

import requests
from loguru import logger

from skylark.gateway.chunk_store import ChunkStore
from skylark.gateway.wire_protocol_header import WireProtocolHeader
from skylark.utils import Timer


class Gateway:
    def __init__(self, server_blk_size=4096 * 16):
        self.chunk_store = ChunkStore()
        self.server_blk_size = server_blk_size
        self.server_processes = []
        self.server_ports = []

    @staticmethod
    def checksum_sha256(path: Path) -> str:
        # todo reading the whole file into memory is not ideal, maybe load chunks or use the linux md5 command
        # todo standardize paths in skylark to be either str or Path or PathLike
        with open(path, "rb") as f:
            hashstr = hashlib.sha256(f.read()).hexdigest()
            assert len(hashstr) == 64
            return hashstr

    def get_free_port(self):
        with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
            s.bind(("", 0))
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            return s.getsockname()[1]

    def start_server(self):
        def server_worker(port):
            with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
                sock.bind(("0.0.0.0", port))
                port = sock.getsockname()[1]
                exit_flag = Value("i", 0)

                def signal_handler(signal, frame):
                    exit_flag.value = 1

                signal.signal(signal.SIGINT, signal_handler)

                sock.listen()
                sock.setblocking(False)
                while True:
                    if exit_flag.value == 1:
                        logger.warning(f"[server:{port}] Exiting on signal")
                        return
                    # Wait for a connection with a timeout of 1 second w/ select
                    readable, _, _ = select.select([sock], [], [], 1)
                    if readable:
                        conn, addr = sock.accept()
                        chunks_received = self.recv_chunks(conn, addr)
                        conn.close()
                        logger.info(f"[server] Received {len(chunks_received)} chunks")

        port = self.get_free_port()
        p = Process(target=server_worker, args=(port,))
        self.server_processes.append(p)
        self.server_ports.append(port)
        p.start()
        logger.info(f"[server] Started server (port = {port})")
        return port

    def stop_server(self, port: int):
        matched_process = None
        for server_port, server_process in zip(self.server_ports, self.server_processes):
            if port == port:
                matched_process = server_process
                break
        if matched_process is None:
            raise ValueError(f"No server found on port {port}")
        else:
            os.kill(matched_process.pid, signal.SIGINT)
            matched_process.join(30)
            matched_process.terminate()
            self.server_processes.remove(matched_process)
            self.server_ports.remove(port)
        logger.warning(f"[server] Stopped server (port = {port})")
        return port

    def stop_servers(self):
        for port in self.server_ports:
            self.stop_server(port)
        assert len(self.server_ports) == 0
        assert len(self.server_processes) == 0

    def send_chunks(self, chunk_ids: List[int], dst_host="127.0.0.1"):
        """Send list of chunks to gateway server, pipelining small chunks together into a single socket stream."""
        # notify server of upcoming ChunkRequests
        # pop chunk_req.path[0] to remove self
        chunk_reqs = []
        for chunk_id in chunk_ids:
            chunk_req = self.chunk_store.get_chunk_request(chunk_id)
            chunk_req.path.pop(0)
            chunk_reqs.append(chunk_req)
        response = requests.post(f"http://{dst_host}:8080/api/v1/chunk_requests", json=[c.as_dict() for c in chunk_reqs])
        assert response.status_code == 200 and response.json() == {"status": "ok"}

        # contact server to set up socket connection
        response = requests.post(f"http://{dst_host}:8080/api/v1/servers")
        assert response.status_code == 200
        dst_port = int(response.json()["server_port"])

        with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
            sock.connect((dst_host, dst_port))
            for idx, chunk_id in enumerate(chunk_ids):
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_CORK, 1)  # disable Nagle's algorithm
                logger.info(f"[client] Sending chunk {chunk_id} to {dst_host}:{dst_port}")
                chunk = self.chunk_store.get_chunk(chunk_id)
                self.chunk_store.start_upload(chunk_id)
                chunk_file_path = self.chunk_store.get_chunk_file_path(chunk_id)
                header = chunk.to_wire_header(end_of_stream=idx == len(chunk_ids) - 1)
                sock.sendall(header.to_bytes())
                with open(chunk_file_path, "rb") as fd:
                    sock.sendfile(fd)
                self.chunk_store.finish_upload(chunk_id)
                chunk_file_path.unlink()
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_CORK, 0)  # send remaining packets

        # close server
        response = requests.delete(f"http://{dst_host}:8080/api/v1/servers/{dst_port}")
        assert response.status_code == 200 and response.json() == {"status": "ok"}

        # move chunk_reqs from downloaded to uploaded
        for chunk_req in chunk_reqs:
            self.chunk_store.mark_chunk_request_uploaded(chunk_req)

    def recv_chunks(self, conn: socket.socket, addr: Tuple[str, int]):
        logger.info(f"[server] Connection from {addr}")
        chunks_received = []
        while True:
            # receive header and write data to file
            chunk_header = WireProtocolHeader.from_socket(conn)
            self.chunk_store.start_download(chunk_header.chunk_id)
            with Timer() as t:
                chunk_data_size = chunk_header.chunk_len
                chunk_file_path = self.chunk_store.get_chunk_file_path(chunk_header.chunk_id)
                with chunk_file_path.open("wb") as f:
                    while chunk_data_size > 0:
                        data = conn.recv(min(chunk_data_size, self.server_blk_size))
                        f.write(data)
                        chunk_data_size -= len(data)
            # check hash, update status and close socket if transfer is complete
            if self.checksum_sha256(chunk_file_path) != chunk_header.chunk_hash_sha256:
                raise ValueError(f"Received chunk {chunk_header.chunk_id} with invalid hash")
            self.chunk_store.finish_download(chunk_header.chunk_id, t.elapsed)
            self.chunk_store.mark_chunk_request_downloaded(chunk_header.chunk_id)
            if chunk_header.end_of_stream:
                conn.close()
                return chunks_received
