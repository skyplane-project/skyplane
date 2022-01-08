import os
import os
import select
import signal
import socket
from contextlib import closing
from multiprocessing import Event, Manager, Process, Value
from typing import Tuple

import setproctitle
from loguru import logger

from skylark.gateway.chunk import WireProtocolHeader
from skylark.gateway.chunk_store import ChunkStore
from skylark.utils.utils import Timer


class GatewayReceiver:
    def __init__(self, chunk_store: ChunkStore, server_blk_size=4096 * 16):
        self.chunk_store = chunk_store
        self.server_blk_size = server_blk_size

        # shared state
        self.manager = Manager()
        self.server_processes = []
        self.server_ports = []

    def start_server(self):
        # todo a good place to add backpressure?
        started_event = Event()
        port = Value("i", 0)

        def server_worker():
            with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
                sock.bind(("0.0.0.0", 0))
                socket_port = sock.getsockname()[1]
                port.value = socket_port
                exit_flag = Value("i", 0)

                def signal_handler(signal, frame):
                    exit_flag.value = 1

                signal.signal(signal.SIGINT, signal_handler)
                setproctitle.setproctitle(f"skylark-gateway-receiver:{socket_port}")

                sock.listen()
                sock.setblocking(False)
                started_event.set()
                while True:
                    if exit_flag.value == 1:
                        logger.warning(f"[server:{socket_port}] Exiting on signal")
                        return
                    # Wait for a connection with a timeout of 1 second w/ select
                    readable, _, _ = select.select([sock], [], [], 1)
                    if readable:
                        conn, addr = sock.accept()
                        chunks_received = self.recv_chunks(conn, addr)
                        conn.close()
                        logger.debug(f"[receiver] {chunks_received} chunks received")

        p = Process(target=server_worker)
        p.start()
        started_event.wait()
        self.server_processes.append(p)
        self.server_ports.append(port.value)
        logger.info(f"[server] Started server (port = {port.value})")
        return port.value

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
        server_port = conn.getsockname()[1]
        chunks_received = []
        while True:
            # receive header and write data to file
            chunk_header = WireProtocolHeader.from_socket(conn)
            self.chunk_store.state_start_download(chunk_header.chunk_id)
            logger.debug(f"[server:{server_port}] Got chunk header {chunk_header}")
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
                    logger.debug(
                        f"[receiver:{server_port}] {chunk_header.chunk_id} chunk received {chunk_received_size}/{chunk_header.chunk_len}"
                    )
            # todo check hash, update status and close socket if transfer is complete
            self.chunk_store.state_finish_download(chunk_header.chunk_id, t.elapsed)
            chunks_received.append(chunk_header.chunk_id)
            logger.info(
                f"[receiver:{server_port}] Received chunk {chunk_header.chunk_id} ({chunk_received_size} bytes) in {t.elapsed:.2f} seconds"
            )
            if chunk_header.end_of_stream:
                conn.close()
                logger.debug(f"[receiver:{server_port}] End of stream reached")
                return chunks_received
