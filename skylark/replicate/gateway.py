"""
chunk_store.py: store file chunks in a local tmpfs

class Chunk:
    def __init__(self, chunk_id, chunk_path)
    def read(self) -> bytes

class ChunkStore:
    def __init__(self, path)
    def store(self, chunk: Chunk)
    def get(self, chunk_id: ChunkId)
    def delete(self, chunk_id: ChunkId)
"""

import os
import shutil
import tempfile
import logging
from pathlib import Path

from loguru import logger

from skylark.utils import PathLike


class Chunk:
    def __init__(self, chunk_id: str, chunk_path: PathLike):
        self.chunk_id = chunk_id
        self.chunk_path = Path(chunk_path)
            
        def read(self) -> bytes:
            with open(self.chunk_path, 'rb') as f:
                return f.read()
    
        def __str__(self):
            return f'Chunk({self.chunk_id}, {self.chunk_path})'


class ChunkStore:
    def __init__(self, base_path: PathLike = "/dev/shm/skylark/chunks"):
        self.path = Path(base_path)
        self.path.mkdir(parents=True, exist_ok=True)
        self.chunks = {}

    def store(self, chunk_id: str, data: bytes):
        chunk_path = self.path / chunk_id
        with open(chunk_path, 'wb') as f:
            f.write(data)
        self.chunks[chunk_id] = Chunk(chunk_id, chunk_path)
        return self.chunks[chunk_id]
    
    def get(self, chunk_id: str) -> Chunk:
        return self.chunks.get(chunk_id)

    def delete(self, chunk_id: str):
        chunk = self.chunks.get(chunk_id)
        if chunk:
            chunk.chunk_path.unlink()
            del self.chunks[chunk_id]
    
    def update_chunks(self):
        for chunk_id in os.listdir(self.path):
            chunk_path = self.path / chunk_id
            self.chunks[chunk_id] = Chunk(chunk_id, chunk_path)

 
"""
gateway, a CLI to copy file from source server to destination server
usage:
python gateway client --dst_host dest_ip --dst_port dest_port --chunk_dir /dev/shm/skylark/chunks --chunk_id chunk_id
python gateway server --port dest_port --chunk_dir /dev/shm/skylark/chunks

server will listen on port for sockets from clients and write incoming files to chunk_dir
"""

import argparse
import signal
import socket
from skylark.utils import Timer

def parse_args():
    parser = argparse.ArgumentParser(description='skylark gateway')
    parser.add_argument('mode', choices=['server', 'client'])
    # common args
    parser.add_argument('--chunk_dir', default='/tmp/skylark/chunks')

    # client args
    parser.add_argument('--dst_host', default=None)
    parser.add_argument('--dst_port', default=None, type=int)
    parser.add_argument('--chunk_id', default=None, type=str)

    # server args
    parser.add_argument('--port', default=None, type=int)
    return parser.parse_args()

def client(args):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((args.dst_host, args.dst_port))
    
    chunk_file_path = Path(args.chunk_dir) / args.chunk_id
    chunk_file_path.parent.mkdir(parents=True, exist_ok=True)
    chunk_file_size = os.path.getsize(chunk_file_path)
    with Timer() as t:
        with open(chunk_file_path, 'rb') as chunk_fd:
            sock.sendfile(chunk_fd)
        sock.close()
    logger.info(f'[{args.chunk_id}] Sent {chunk_file_size / 1e6:.1f}MB in {t.elapsed:.2} seconds at {chunk_file_size / t.elapsed / 1e9:.4f} Gbps')

def server(args):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(('', args.port))
    sock.listen(1)

    # catch control-c and close socket
    def signal_handler(signal, frame):
        sock.close()
        exit(0)
    signal.signal(signal.SIGINT, signal_handler)

    while True:
        conn, addr = sock.accept()
        logger.info(f'[{addr}] Connected')
        Path(args.chunk_dir).mkdir(parents=True, exist_ok=True)

        with Timer() as t:
            chunk_fd_system, chunk_id = tempfile.mkstemp(dir=args.chunk_dir)
            with open(chunk_fd_system, 'wb') as chunk_fd:
                while True:
                    data = conn.recv(1024)
                    if not data:
                        break
                    chunk_fd.write(data)
            conn.close()
        
        chunk_file_path = Path(args.chunk_dir) / chunk_id
        chunk_file_size = os.path.getsize(chunk_file_path)

        logger.info(f'[{chunk_id}] Received {chunk_file_size / 1e6:.1f}MB in {t.elapsed:.2} seconds at {chunk_file_size / t.elapsed / 1e9:.4f} Gbps')

if __name__ == '__main__':
    args = parse_args()
    if args.mode == 'server':
        server(args)
    elif args.mode == 'client':
        client(args)