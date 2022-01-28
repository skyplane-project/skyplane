import os
import signal
import socket
import time
from contextlib import closing
from multiprocessing import Event, Process, Value
from typing import Tuple

import setproctitle
from skylark.utils import logger
from skylark import GB, MB, KB
from skylark.chunk import WireProtocolHeader
from skylark.gateway.chunk_store import ChunkStore
from skylark.utils.utils import Timer


class GatewayReceiver:
    def __init__(self, chunk_store: ChunkStore, write_back_block_size=32 * MB, max_pending_chunks=1):
        self.chunk_store = chunk_store
        self.write_back_block_size = write_back_block_size
        self.max_pending_chunks = max_pending_chunks
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
                started_event.set()
                logger.info(f"[receiver:{socket_port}] Waiting for connection")
                conn, addr = sock.accept()
                logger.info(f"[receiver:{socket_port}] Accepted connection from {addr}")
                while True:
                    if exit_flag.value == 1:
                        logger.warning(f"[receiver:{socket_port}] Exiting on signal")
                        conn.close()
                        return
                    self.recv_chunks(conn, addr)

        p = Process(target=server_worker)
        p.start()
        started_event.wait()
        self.server_processes.append(p)
        self.server_ports.append(port.value)
        logger.info(f"[receiver:{port.value}] Started server)")
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
        logger.warning(f"[server:{port}] Stopped server")
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
            logger.debug(f"[receiver:{server_port}] Blocking for next header")
            chunk_header = WireProtocolHeader.from_socket(conn)
            logger.debug(f"[receiver:{server_port}]:{chunk_header.chunk_id} Got chunk header {chunk_header}")
            self.chunk_store.state_start_download(chunk_header.chunk_id)

            # block until we have enough space to write the chunk
            while self.chunk_store.remaining_bytes() < chunk_header.chunk_len * self.max_pending_chunks:
                logger.warning(f"[receiver:{server_port}]:{chunk_header.chunk_id} Waiting for space to write chunk ")
                time.sleep(0.01)  # todo should use inotify

            # get data
            logger.debug(f"[receiver:{server_port}]:{chunk_header.chunk_id} wire header length {chunk_header.chunk_len}")
            with Timer() as t:
                chunk_data_size = chunk_header.chunk_len
                chunk_received_size = 0
                chunk_file_path = self.chunk_store.get_chunk_file_path(chunk_header.chunk_id)
                chunk_data = b""
                while chunk_data_size > 0:
                    data = conn.recv(min(chunk_data_size, self.write_back_block_size))
                    chunk_data += data
                    chunk_data_size -= len(data)
                    chunk_received_size += len(data)
                    logger.debug(f"[receiver:{server_port}]:{chunk_header.chunk_id} received {chunk_received_size} / {chunk_header.chunk_len} bytes")

                with chunk_file_path.open("wb") as f:
                    f.write(chunk_data)
            logger.info(
                f"[receiver:{server_port}]:{chunk_header.chunk_id} in {t.elapsed:.2f} seconds ({chunk_received_size * 8 / t.elapsed / GB:.2f}Gbps)"
            )
            # todo check hash
            self.chunk_store.state_finish_download(chunk_header.chunk_id, t.elapsed)
            chunks_received.append(chunk_header.chunk_id)

            if chunk_header.n_chunks_left_on_socket == 0:
                logger.debug(f"[receiver:{server_port}] End of stream reached")
                return
