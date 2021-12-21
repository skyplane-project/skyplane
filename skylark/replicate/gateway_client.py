import argparse
import os
from pathlib import Path
import socket

from loguru import logger

from skylark.utils import Timer


def client(args):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_CORK, 1)
    sock.connect((args.dst_host, args.dst_port))

    chunk_file_path = Path(args.chunk_dir) / args.chunk_id
    chunk_file_path.parent.mkdir(parents=True, exist_ok=True)
    chunk_file_size = os.path.getsize(chunk_file_path)
    with Timer() as t:
        sock.send(int(args.chunk_id).to_bytes(8, byteorder="big"))
        sock.send(chunk_file_size.to_bytes(4, byteorder="big"))
        logger.info(f"Sent file size: {chunk_file_size}")
        with open(chunk_file_path, "rb") as chunk_fd:
            sock.sendfile(chunk_fd)
        sock.close()
    logger.info(
        f"[{args.chunk_id}] Sent {chunk_file_size / 1e6:.1f}MB in {t.elapsed:.2} seconds at {chunk_file_size * 8 / t.elapsed / 1e9:.4f} Gbps"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Skylark Gateway client")
    parser.add_argument("--chunk_dir", default="/dev/shm/skylark/chunks_in")
    parser.add_argument("--dst_host", default=None)
    parser.add_argument("--dst_port", default=None, type=int)
    parser.add_argument("--chunk_id", default=None, type=str)
    args = parser.parse_args()
    Path(args.chunk_dir).mkdir(parents=True, exist_ok=True)
    client(args)
