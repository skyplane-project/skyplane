import argparse
from multiprocessing import Process
import os
from pathlib import Path
import socket
import signal

from loguru import logger

from skylark.utils import Timer


def server_worker(port, chunk_dir, blk_size=32 * 1024 * 1024):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_CORK, 1)
    sock.bind(("", port))
    logger.info(f"Listening on port {port}")
    sock.listen(1)

    while True:
        try:
            conn, addr = sock.accept()
            Path(args.chunk_dir).mkdir(parents=True, exist_ok=True)

            with Timer() as t:
                chunk_idx = int.from_bytes(conn.recv(8), byteorder="big")
                chunk_size = int.from_bytes(conn.recv(4), byteorder="big")
                chunk_path = Path(chunk_dir) / str(chunk_idx)
                with chunk_path.open("wb") as f:
                    while chunk_size > 0:
                        data = conn.recv(min(chunk_size, blk_size))
                        f.write(data)
                        chunk_size -= len(data)
                conn.close()

            chunk_file_size = chunk_path.stat().st_size
            logger.info(
                f"[{chunk_idx}] Received {chunk_file_size / 1e6:.1f}MB in {t.elapsed:.2} seconds at {chunk_file_size / t.elapsed / 1e9:.4f} Gbps from {addr}"
            )
        except Exception as e:
            logger.error(e)


def server(args):
    logger.info(f"Starting server on port {args.port}...{args.port + args.num_connections}")
    processes = []
    for i in range(args.num_connections):
        port = args.port + i
        p = Process(target=server_worker, args=(port, args.chunk_dir, args.blk_size))
        p.start()
        processes.append(p)

    def handler(signum, frame):
        logger.info("Closing server.")
        for p in processes:
            p.terminate()
        exit(0)

    signal.signal(signal.SIGINT, handler)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Skylark gateway server")
    parser.add_argument("--chunk_dir", default="/dev/shm/skylark/chunks_out", type=str)
    parser.add_argument("--port", default=8100, type=int)
    parser.add_argument("--num_connections", default=1, type=int)
    parser.add_argument("--blk_size", default=1024 * 1024, type=int)
    args = parser.parse_args()
    Path(args.chunk_dir).mkdir(parents=True, exist_ok=True)
    server(args)
