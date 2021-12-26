import argparse
import hashlib
import os
import signal
import socket
from contextlib import closing
import sys

from dataclasses import dataclass
from multiprocessing import Process
from pathlib import Path
from typing import List

from loguru import logger

from skylark.utils import Timer


@dataclass
class ChunkHeader:
    # sent over wire in order:
    #   magic
    #   chunk_id
    #   chunk_size_bytes
    #   chunk_offset_bytes
    #   end_of_stream
    #   chunk_hash_sha256
    chunk_id: int  # unsigned long
    chunk_size_bytes: int  # unsigned long
    chunk_offset_bytes: int  # unsigned long
    chunk_hash_sha256: str  # 64-byte checksum
    end_of_stream: bool = False  # false by default, but true if this is the last chunk

    @staticmethod
    def magic_hex():
        return 0x534B595F4C41524B  # "SKY_LARK"

    @staticmethod
    def length_bytes():
        # magic (8) + chunk_id (8) + chunk_size_bytes (8) + chunk_offset_bytes (8) + end_of_stream (1) + chunk_hash_sha256 (64)
        return 8 + 8 + 8 + 8 + 1 + 64

    @staticmethod
    def from_bytes(data: bytes):
        assert len(data) == ChunkHeader.length_bytes()
        magic = int.from_bytes(data[:8], byteorder="big")
        if magic != ChunkHeader.magic_hex():
            raise ValueError("Invalid magic number")
        chunk_id = int.from_bytes(data[8:16], byteorder="big")
        chunk_size_bytes = int.from_bytes(data[16:24], byteorder="big")
        chunk_offset_bytes = int.from_bytes(data[24:32], byteorder="big")
        chunk_end_of_stream = bool(data[32])
        chunk_hash_sha256 = data[33:].decode("utf-8")
        return ChunkHeader(
            chunk_id=chunk_id,
            chunk_size_bytes=chunk_size_bytes,
            chunk_offset_bytes=chunk_offset_bytes,
            chunk_hash_sha256=chunk_hash_sha256,
            end_of_stream=chunk_end_of_stream,
        )

    def to_bytes(self):
        out_bytes = b""
        out_bytes += self.magic_hex().to_bytes(8, byteorder="big")
        out_bytes += self.chunk_id.to_bytes(8, byteorder="big")
        out_bytes += self.chunk_size_bytes.to_bytes(8, byteorder="big")
        out_bytes += self.chunk_offset_bytes.to_bytes(8, byteorder="big")
        out_bytes += bytes([int(self.end_of_stream)])
        assert len(self.chunk_hash_sha256) == 64
        out_bytes += self.chunk_hash_sha256.encode("utf-8")
        assert len(out_bytes) == ChunkHeader.length_bytes()
        return out_bytes

    @staticmethod
    def from_socket(sock):
        header_bytes = sock.recv(ChunkHeader.length_bytes())
        return ChunkHeader.from_bytes(header_bytes)

    def to_socket(self, sock):
        assert sock.sendall(self.to_bytes()) == None


def checksum_sha256(path: Path) -> str:
    with open(path, "rb") as f:
        hashstr = hashlib.sha256(f.read()).hexdigest()
        assert len(hashstr) == 64
        return hashstr


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
    
    def send_chunks(self, chunk_ids: List[int], dst_host="127.0.0.1", dst_port=8100):
        """Send list of chunks to gateway server, pipelining small chunks together into a single socket stream."""
        with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_CORK, 1)
            sock.connect((dst_host, dst_port))
            for idx, chunk_id in enumerate(chunk_ids):
                logger.info(f"[client] Sending chunk {chunk_id} to {dst_host}:{dst_port}")
                chunk_file_path = self.chunk_dir / chunk_id
                header = ChunkHeader(
                    chunk_id=chunk_id,
                    chunk_size_bytes=chunk_file_path.stat().st_size,
                    chunk_offset_bytes=0,
                    chunk_hash_sha256=checksum_sha256(chunk_file_path),
                    end_of_stream=idx == len(chunk_ids) - 1,
                )
                sock.sendall(header.to_bytes())
                with open(chunk_file_path, "rb") as fd:
                    sock.sendfile(fd)
    
    def recv_chunks(self, sock_conn):
        conn, addr = sock_conn
        logger.info(f"[server] Connection from {addr}")
        chunks_received = []
        bytes_received = 0.0
        transfer_seconds = 0.0
        while True:
            chunk_header = ChunkHeader.from_socket(conn)
            chunk_file_path = self.chunk_dir / chunk_header.chunk_id
            logger.info(f"[server] Received chunk {chunk_header.chunk_id}")
            with Timer() as t:
                chunk_data_size = chunk_header.chunk_size_bytes
                with chunk_file_path.open("wb") as f:
                    while chunk_data_size > 0:
                        data = conn.recv(min(chunk_data_size, self.server_blk_size))
                        f.write(data)
                        chunk_data_size -= len(data)
                        bytes_received += len(data)
            transfer_seconds += t.elapsed

            if chunk_header.end_of_stream:
                gbps = bytes_received * 8 / 1e3 / transfer_seconds
                logger.info(
                    f"[server] Received {chunks_received} chunks, {bytes_received / 1e9:.2}GB in {transfer_seconds:.2}s at {gbps:.2}Gbps"
                )
                conn.close()
                return chunks_received

    def get_free_port(self):
        with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
            s.bind(('', 0))
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            return s.getsockname()[1]

    def start_recv_server(self):
        """Start a server to receive chunks from a client."""
        def server_worker(port):
            with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_CORK, 1)
                sock.bind(("0.0.0.0", port))
                port = sock.getsockname()[1]
                exit_flag = False
                logger.info(f"[server] Listening on port {port}")

                def signal_handler(signal, frame):
                    exit_flag = True

                signal.signal(signal.SIGINT, signal_handler)

                sock.listen(1)
                while True:
                    self.recv_chunks(sock.accept())
                    if exit_flag:
                        break
                logger.info(f"[server] Exiting server worker on port {port}")
        
        for i in range(self.server_num_connections):
            port = self.get_free_port() 
            p = Process(target=server_worker, args=(port,))
            self.server_processes.append(p)
            self.server_ports.append(port)
            p.start()
        
        logger.info(f"[server] Started {self.server_num_connections} servers, returning control to Skylark API")

    def stop_server(self):
        logger.warning(f"[server] Stopping {self.server_num_connections} servers")
        for p in self.server_processes:
            logger.debug(f"[server] Signaling server on port {p.pid}")
            os.kill(p.pid, signal.SIGINT)
            p.join(30)
            p.terminate()
            self.server_processes.remove(p)
        self.server_processes.clear()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Skylark Gateway")
    parser.add_argument("--chunk-dir", type=str, default="/dev/shm/skylark/chunks")
    parser.add_argument("--server-start-port", type=int, default=8100)
    parser.add_argument("--server-num-connections", type=int, default=16)
    parser.add_argument("--server-blk-size", type=int, default=4096 * 16)
    args = parser.parse_args()

    gw = Gateway(
        chunk_dir=args.chunk_dir,
        server_start_port=args.server_start_port,
        server_num_connections=args.server_num_connections,
        server_blk_size=args.server_blk_size,
    )
    gw.start_recv_server()
    
    # set sigint handler
    def signal_handler(signal, frame):
        gw.stop_server()
        sys.exit(0)
    signal.signal(signal.SIGINT, signal_handler)