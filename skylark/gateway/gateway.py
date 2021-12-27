import hashlib
import os
import select
import shutil
import signal
import socket
from contextlib import closing
from multiprocessing import Manager, Process, Value
from pathlib import Path
from typing import List, Tuple

from loguru import logger
from skylark.gateway.chunk_header import ChunkHeader
from skylark.utils import Timer


class Gateway:
    """
    Runs an HTTP server on port 80 to control Gateway tasks.

    A Gateway controls replication on a single server. It accepts replication jobs
    which are queued and load-balanced across senders (GatewayClients). It also starts
    multiple copies of a GatewayServer in separate processes to accept incoming file
    transfers.
    """

    def __init__(
        self,
        chunk_dir="/dev/shm/skylark/chunks",
        server_blk_size=4096 * 16,
    ):
        self.chunk_dir = Path(chunk_dir)
        self.chunk_dir.mkdir(parents=True, exist_ok=True)
        self.server_blk_size = server_blk_size
        self.server_processes = []
        self.server_ports = []

        # multiprocess coordination
        self.manager = Manager()
        self.chunks = self.manager.dict()  # Dict[int, Dict]

    @staticmethod
    def checksum_sha256(path: Path) -> str:
        with open(path, "rb") as f:
            hashstr = hashlib.sha256(f.read()).hexdigest()
            assert len(hashstr) == 64
            return hashstr

    def register_chunk(self, chunk_id: int, source_chunk_path: Path):
        """
        Register a chunk with the gateway.

        This is called by the GatewayClient when it receives a chunk from the
        sender.
        """
        logger.debug(f"[gateway] Registering chunk {chunk_id}")
        chunk_path = self.chunk_dir / f"{chunk_id}.chunk"
        shutil.copyfile(source_chunk_path, chunk_path)
        header = ChunkHeader(
            chunk_id=chunk_id,
            chunk_size_bytes=chunk_path.stat().st_size,
            chunk_offset_bytes=0,
            chunk_hash_sha256=self.checksum_sha256(chunk_path),
        )
        self.chunks[chunk_id] = self.manager.dict({"header": header, "file_path": str(chunk_path.resolve()), "is_complete": True})

    def send_chunks(self, chunk_ids: List[int], dst_host="127.0.0.1", dst_port=8100):
        """Send list of chunks to gateway server, pipelining small chunks together into a single socket stream."""
        with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_CORK, 1)
            sock.connect((dst_host, dst_port))
            for idx, chunk_id in enumerate(chunk_ids):
                logger.info(f"[client] Sending chunk {chunk_id} to {dst_host}:{dst_port}")
                chunk_file_path = self.chunk_dir / f"{chunk_id}.chunk"
                header = ChunkHeader(
                    chunk_id=chunk_id,
                    chunk_size_bytes=chunk_file_path.stat().st_size,
                    chunk_offset_bytes=0,
                    chunk_hash_sha256=self.checksum_sha256(chunk_file_path),
                    end_of_stream=idx == len(chunk_ids) - 1,
                )
                sock.sendall(header.to_bytes())
                with open(chunk_file_path, "rb") as fd:
                    sock.sendfile(fd)

    def recv_chunks(self, conn: socket.socket, addr: Tuple[str, int]):
        logger.info(f"[server] Connection from {addr}")
        chunks_received = []
        bytes_received = 0.0
        transfer_seconds = 0.0
        while True:
            chunk_header = ChunkHeader.from_socket(conn)

            # log metadata
            chunk_file_path = self.chunk_dir / f"{chunk_header.chunk_id}.chunk"
            self.chunks[chunk_header.chunk_id] = self.manager.dict()
            self.chunks[chunk_header.chunk_id]["header"] = chunk_header
            self.chunks[chunk_header.chunk_id]["file_path"] = str(chunk_file_path.resolve())
            self.chunks[chunk_header.chunk_id]["is_complete"] = False

            # recieve file
            with Timer() as t:
                chunk_data_size = chunk_header.chunk_size_bytes
                with chunk_file_path.open("wb") as f:
                    while chunk_data_size > 0:
                        data = conn.recv(min(chunk_data_size, self.server_blk_size))
                        f.write(data)
                        chunk_data_size -= len(data)
                        bytes_received += len(data)
                    # check hash
                    if self.checksum_sha256(chunk_file_path) != chunk_header.chunk_hash_sha256:
                        raise ValueError(f"Received chunk {chunk_header.chunk_id} with invalid hash")

            transfer_seconds += t.elapsed
            self.chunks[chunk_header.chunk_id]["is_complete"] = True
            self.chunks[chunk_header.chunk_id]["stats.download_runtime_s"] = t.elapsed

            if chunk_header.end_of_stream:
                conn.close()
                return chunks_received

    def get_free_port(self):
        with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
            s.bind(("", 0))
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            return s.getsockname()[1]

    def start_server(self):
        def server_worker(port):
            with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_CORK, 1)
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
