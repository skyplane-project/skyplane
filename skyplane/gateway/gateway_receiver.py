import os
import signal
import socket
import ssl
import time
import traceback
import nacl.secret
from contextlib import closing
from multiprocessing import Event, Process, Value, Queue
from typing import Optional, Tuple

import lz4.frame

from skyplane import MB
from skyplane.chunk import WireProtocolHeader
from skyplane.gateway.cert import generate_self_signed_certificate
from skyplane.gateway.chunk_store import ChunkStore
from skyplane.utils import logger
from skyplane.utils.timer import Timer


class GatewayReceiver:
    def __init__(
        self,
        region: str,
        chunk_store: ChunkStore,
        error_event,
        error_queue: Queue,
        recv_block_size=4 * MB,
        max_pending_chunks=1,
        use_tls: bool = True,
        use_compression: bool = True,
        e2ee_key_bytes: Optional[bytes] = None,
    ):
        self.region = region
        self.chunk_store = chunk_store
        self.error_event = error_event
        self.error_queue = error_queue
        self.recv_block_size = recv_block_size
        self.max_pending_chunks = max_pending_chunks
        self.use_compression = use_compression
        if e2ee_key_bytes is None:
            self.e2ee_secretbox = None
        else:
            self.e2ee_secretbox = nacl.secret.SecretBox(e2ee_key_bytes)
        self.server_processes = []
        self.server_ports = []
        self.next_gateway_worker_id = 0
        self.socket_profiler_event_queue = Queue()

        # SSL context
        if use_tls:
            generate_self_signed_certificate("temp.cert", "temp.key")
            self.ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
            self.ssl_context.check_hostname = False
            self.ssl_context.verify_mode = ssl.CERT_NONE
            self.ssl_context.load_cert_chain("temp.cert", "temp.key")
            logger.info(f"Using {str(ssl.OPENSSL_VERSION)}")
        else:
            self.ssl_context = None

        # private state per worker
        self.worker_id: Optional[int] = None

    def start_server(self):
        started_event = Event()
        port = Value("i", 0)

        def server_worker(worker_id: int):
            self.worker_id = worker_id
            with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
                sock.bind(("0.0.0.0", 0))
                socket_port = sock.getsockname()[1]
                port.value = socket_port
                exit_flag = Event()

                def signal_handler(signal, frame):
                    exit_flag.set()

                signal.signal(signal.SIGINT, signal_handler)

                sock.listen()
                if self.ssl_context is not None:
                    ssl_sock = self.ssl_context.wrap_socket(sock, server_side=True)
                else:
                    ssl_sock = sock
                started_event.set()
                logger.info(f"[receiver:{socket_port}] Waiting for connection")
                ssl_conn, addr = ssl_sock.accept()
                logger.info(f"[receiver:{socket_port}] Accepted connection from {addr}")
                while not exit_flag.is_set() and not self.error_event.is_set():
                    try:
                        self.recv_chunks(ssl_conn, addr)
                    except Exception as e:
                        logger.warning(f"[receiver:{socket_port}] Error: {str(e)}")
                        self.error_queue.put(traceback.format_exc())
                        exit_flag.set()
                        self.error_event.set()
                logger.warning(f"[receiver:{socket_port}] Exiting on signal")
                ssl_conn.close()

        gateway_id = self.next_gateway_worker_id
        self.next_gateway_worker_id += 1
        p = Process(target=server_worker, args=(gateway_id,))
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
            chunk_request = self.chunk_store.get_chunk_request(chunk_header.chunk_id)
            should_decrypt = self.e2ee_secretbox is not None and chunk_request.dst_region == self.region
            should_decompress = chunk_header.is_compressed and chunk_request.dst_region == self.region

            # wait for space
            while self.chunk_store.remaining_bytes() < chunk_header.data_len * self.max_pending_chunks:
                time.sleep(0.1)

            # get data
            self.chunk_store.state_queue_download(chunk_header.chunk_id)
            self.chunk_store.state_start_download(chunk_header.chunk_id, f"receiver:{self.worker_id}")
            logger.debug(f"[receiver:{server_port}]:{chunk_header.chunk_id} wire header length {chunk_header.data_len}")
            with Timer() as t:
                with self.chunk_store.get_chunk_file_path(chunk_header.chunk_id).open("wb") as f:
                    socket_data_len = chunk_header.data_len
                    chunk_received_size = 0
                    chunk_received_size_decompressed = 0
                    to_write = bytearray(socket_data_len)
                    to_write_view = memoryview(to_write)
                    while socket_data_len > 0:
                        nbytes = conn.recv_into(to_write_view[chunk_received_size:], min(socket_data_len, self.recv_block_size))
                        socket_data_len -= nbytes
                        chunk_received_size += nbytes
                        self.socket_profiler_event_queue.put(
                            dict(
                                receiver_id=self.worker_id,
                                chunk_id=chunk_header.chunk_id,
                                time_ms=t.elapsed * 1000.0,
                                bytes=chunk_received_size,
                            )
                        )
                    to_write = bytes(to_write)
                    if should_decrypt:
                        to_write = self.e2ee_secretbox.decrypt(to_write)
                    if should_decompress:
                        data_batch_decompressed = lz4.frame.decompress(to_write)
                        chunk_received_size_decompressed += len(data_batch_decompressed)
                        to_write = data_batch_decompressed
                    f.write(to_write)
            assert (
                socket_data_len == 0 and chunk_received_size == chunk_header.data_len
            ), f"Size mismatch: got {chunk_received_size} expected {chunk_header.data_len} and had {socket_data_len} bytes remaining"

            # todo check hash
            self.chunk_store.state_finish_download(chunk_header.chunk_id, f"receiver:{self.worker_id}")
            chunks_received.append(chunk_header.chunk_id)

            if chunk_header.n_chunks_left_on_socket == 0:
                logger.debug(f"[receiver:{server_port}] End of stream reached")
                return
