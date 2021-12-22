import argparse
import json
import os
from pathlib import Path
import socket

from loguru import logger

from skylark.utils import Timer


def check_reachable(host, port):
    """check reachability via port 22"""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(1)
    try:
        sock.connect((host, port))
        sock.close()
    except socket.error:
        raise RuntimeError(f"Host {host} is not reachable via port {port}")
    logger.info(f"Host {host} is reachable via port {port}")


def client(args):
    check_reachable(args.dst_host, 22)

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

    throughput_gbps = chunk_file_size * 8 / t.elapsed / 1e9
    logger.info(f"[{args.chunk_id}] Sent {chunk_file_size / 1e6:.1f}MB in {t.elapsed:.2} seconds at {throughput_gbps:.4f} Gbps")
    print(json.dumps({"chunk_id": args.chunk_id, "size_bytes": chunk_file_size, "runtime_s": t.elapsed, "speed_gbps": throughput_gbps}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Skylark Gateway client")
    parser.add_argument("--chunk_dir", default="/dev/shm/skylark/chunks_in")
    parser.add_argument("--dst_host", default="127.0.0.1")
    parser.add_argument("--dst_port", default=8100, type=int)
    parser.add_argument("--chunk_id", default=None, type=str)
    args = parser.parse_args()
    Path(args.chunk_dir).mkdir(parents=True, exist_ok=True)
    client(args)
