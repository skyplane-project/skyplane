import hashlib
import os
import queue
import select
import signal
import socket
from contextlib import closing
from multiprocessing import Event, Manager, Process, Value
from pathlib import Path
from typing import List, Tuple

import requests
from loguru import logger

from skylark.gateway.chunk_store import ChunkRequest, ChunkStore
from skylark.gateway.wire_protocol_header import WireProtocolHeader
from skylark.utils import PathLike, Timer


class GatewayReciever:
    def __init__(self, chunk_store: ChunkStore, server_blk_size=4096 * 16):
        self.chunk_store = chunk_store
        self.server_blk_size = server_blk_size
        self.server_processes = []
        self.server_ports = []

    @staticmethod
    def checksum_sha256(path: PathLike) -> str:
        # todo reading the whole file into memory is not ideal, maybe load chunks or use the linux md5 command
        # todo standardize paths in skylark to be either str or Path or PathLike
        with Path(open).open("rb") as f:
            hashstr = hashlib.sha256(f.read()).hexdigest()
            assert len(hashstr) == 64
            return hashstr

    def get_free_port(self):
        with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
            s.bind(("", 0))
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            return s.getsockname()[1]

    def start_server(self):
        # todo a good place to add backpressure?
        started_event = Event()

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
                started_event.set()
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
        started_event.wait()
        logger.info(f"[server] Started server (port = {port})")
        return port

    def stop_server(self, port: int):
        matched_process = None
        for server_port, server_process in zip(self.server_ports, self.server_processes):
            if server_port == port:
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

    def recv_chunks(self, conn: socket.socket, addr: Tuple[str, int]):
        logger.info(f"[server] Connection from {addr}")
        chunks_received = []
        while True:
            # receive header and write data to file
            chunk_header = WireProtocolHeader.from_socket(conn)
            self.chunk_store.start_download(chunk_header.chunk_id)
            with Timer() as t:
                chunk_data_size = chunk_header.chunk_len
                chunk_received_size = 0
                chunk_file_path = self.chunk_store.get_chunk_file_path(chunk_header.chunk_id)
                with chunk_file_path.open("wb") as f:
                    while chunk_data_size > 0:
                        data = conn.recv(min(chunk_data_size, self.server_blk_size))
                        f.write(data)
                        chunk_data_size -= len(data)
                        chunk_received_size += len(data)
            # check hash, update status and close socket if transfer is complete
            # todo write checksums upon read from object store
            # if self.checksum_sha256(chunk_file_path) != chunk_header.chunk_hash_sha256:
            #     raise ValueError(f"Received chunk {chunk_header.chunk_id} with invalid hash")
            self.chunk_store.finish_download(chunk_header.chunk_id, t.elapsed)
            chunks_received.append(chunk_header.chunk_id)
            logger.info(f"[server] Received chunk {chunk_header.chunk_id} ({chunk_received_size} bytes) in {t.elapsed:.2f} seconds")
            if chunk_header.end_of_stream:
                logger.info(f"[server] Received end of stream, returning")
                conn.close()
                return chunks_received
