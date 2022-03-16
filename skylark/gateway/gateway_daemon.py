import argparse
import atexit
import json
import os
import sys
from multiprocessing import Event
from os import PathLike
from pathlib import Path
from typing import Dict


import setproctitle
from skylark.utils import logger
from skylark import MB, print_header
from skylark.chunk import ChunkState
from skylark.gateway.chunk_store import ChunkStore
from skylark.gateway.gateway_daemon_api import GatewayDaemonAPI
from skylark.gateway.gateway_receiver import GatewayReceiver
from skylark.gateway.gateway_sender import GatewaySender

from skylark.gateway.gateway_obj_store import GatewayObjStoreConn


class GatewayDaemon:
    def __init__(self, region: str, outgoing_ports: Dict[str, int], chunk_dir: PathLike, max_incoming_ports=64, use_tls=True):
        # todo max_incoming_ports should be configurable rather than static
        self.region = region
        self.max_incoming_ports = max_incoming_ports
        self.chunk_store = ChunkStore(chunk_dir)
        self.exit_flag = Event()
        
        self.gateway_receiver = GatewayReceiver(chunk_store=self.chunk_store, max_pending_chunks=max_incoming_ports, use_tls=use_tls)
        self.gateway_sender = GatewaySender(chunk_store=self.chunk_store, outgoing_ports=outgoing_ports, use_tls=use_tls)
        self.obj_store_conn = GatewayObjStoreConn(chunk_store=self.chunk_store, max_conn=8)
        self.api_server = GatewayDaemonAPI(self.chunk_store, self.gateway_receiver, daemon_exit_fn=self.stop)

    def stop(self):
        self.exit_flag.set()

    def run(self):
        self.gateway_sender.start_workers()
        self.obj_store_conn.start_workers()
        self.api_server.start()
        logger.info(f"[gateway_daemon] API started at {self.api_server.url}")
        setproctitle.setproctitle(f"skylark-gateway-daemon")
        while not self.exit_flag.is_set():
            # queue object uploads and relays
            for chunk_req in self.chunk_store.get_chunk_requests(ChunkState.downloaded):
                if self.region == chunk_req.dst_region and chunk_req.dst_type == "save_local":  # do nothing, save to ChunkStore
                    self.chunk_store.state_queue_upload(chunk_req.chunk.chunk_id)
                    self.chunk_store.state_start_upload(chunk_req.chunk.chunk_id, "save_local")
                    self.chunk_store.state_finish_upload(chunk_req.chunk.chunk_id, "save_local")
                    self.chunk_store.get_chunk_file_path(chunk_req.chunk.chunk_id).unlink()
                elif self.region == chunk_req.dst_region and chunk_req.dst_type == "object_store":
                    self.chunk_store.state_queue_upload(chunk_req.chunk.chunk_id)
                    self.obj_store_conn.queue_request(chunk_req, "upload")
                elif self.region != chunk_req.dst_region:
                    self.gateway_sender.queue_request(chunk_req)
                    self.chunk_store.state_queue_upload(chunk_req.chunk.chunk_id)
                else:
                    self.chunk_store.state_fail(chunk_req.chunk.chunk_id)
                    raise ValueError(f"No upload handler for ChunkRequest: {chunk_req}")

            # queue object store downloads and relays (if space is available)
            for chunk_req in self.chunk_store.get_chunk_requests(ChunkState.registered):
                if self.region == chunk_req.src_region and chunk_req.src_type == "read_local":  # do nothing, read from local ChunkStore
                    self.chunk_store.state_queue_download(chunk_req.chunk.chunk_id)
                    self.chunk_store.state_start_download(chunk_req.chunk.chunk_id, "read_local")
                    self.chunk_store.state_finish_download(chunk_req.chunk.chunk_id, "read_local")
                elif self.region == chunk_req.src_region and chunk_req.src_type == "random":
                    size_mb = chunk_req.src_random_size_mb
                    if self.chunk_store.remaining_bytes() >= size_mb * MB * self.max_incoming_ports:
                        self.chunk_store.state_start_download(chunk_req.chunk.chunk_id, "random")
                        fpath = str(self.chunk_store.get_chunk_file_path(chunk_req.chunk.chunk_id).absolute())
                        with self.dl_pool_semaphore:
                            os.system(f"fallocate -l {size_mb * MB} {fpath}")
                        chunk_req.chunk.chunk_length_bytes = os.path.getsize(fpath)
                        self.chunk_store.chunk_requests[chunk_req.chunk.chunk_id] = chunk_req
                        self.chunk_store.state_finish_download(chunk_req.chunk.chunk_id, "random")
                elif self.region == chunk_req.src_region and chunk_req.src_type == "object_store":
                    self.chunk_store.state_queue_download(chunk_req.chunk.chunk_id)
                    self.obj_store_conn.queue_request(chunk_req, "download")
                elif self.region != chunk_req.src_region:  # do nothing, waiting for chunk to be be ready_to_upload
                    continue
                else:
                    self.chunk_store.state_fail(chunk_req.chunk.chunk_id)
                    raise ValueError(f"No download handler for ChunkRequest: {chunk_req}")
        self.gateway_receiver.stop_servers()
        self.gateway_sender.stop_workers()
        self.obj_store_conn.stop_workers()
        self.api_server.stop()
        sys.exit(0)


if __name__ == "__main__":
    print_header()
    parser = argparse.ArgumentParser(description="Skylark Gateway Daemon")
    parser.add_argument("--region", type=str, required=True, help="Region tag (provider:region")
    parser.add_argument(
        "--outgoing-ports", type=str, required=True, help="JSON encoded path mapping destination ip to number of outgoing ports"
    )
    parser.add_argument("--chunk-dir", type=Path, default="/tmp/skylark/chunks", help="Directory to store chunks")
    parser.add_argument("--disable-tls", action="store_true")
    args = parser.parse_args()

    daemon = GatewayDaemon(
        region=args.region, outgoing_ports=json.loads(args.outgoing_ports), chunk_dir=args.chunk_dir, use_tls=not args.disable_tls
    )
    daemon.run()
