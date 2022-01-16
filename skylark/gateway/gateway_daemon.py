import argparse
import atexit
import os
import re
import signal
import sys
import threading
from multiprocessing import Event
from os import PathLike
from pathlib import Path
from typing import Optional

import setproctitle
from loguru import logger

from skylark import MB, print_header
from skylark.chunk import ChunkState
from skylark.gateway.chunk_store import ChunkStore
from skylark.gateway.gateway_daemon_api import GatewayDaemonAPI
from skylark.gateway.gateway_receiver import GatewayReceiver
from skylark.gateway.gateway_sender import GatewaySender


class GatewayDaemon:
    def __init__(self, chunk_dir: PathLike, debug=False, log_dir: Optional[PathLike] = None, outgoing_connections=1):
        if log_dir is not None:
            log_dir = Path(log_dir)
            log_dir.mkdir(exist_ok=True)
            logger.remove()
            logger.add(log_dir / "gateway_daemon.log", rotation="10 MB", enqueue=True)
            logger.add(sys.stderr, colorize=True, format="{function:>15}:{line:<3} {level:<8} {message}", level="DEBUG", enqueue=True)
        self.chunk_store = ChunkStore(chunk_dir)
        self.gateway_receiver = GatewayReceiver(chunk_store=self.chunk_store, max_pending_chunks=outgoing_connections)
        self.gateway_sender = GatewaySender(chunk_store=self.chunk_store, n_processes=outgoing_connections)

        # API server
        self.api_server = GatewayDaemonAPI(self.chunk_store, self.gateway_receiver, debug=debug, log_dir=log_dir)
        self.api_server.start()
        atexit.register(self.cleanup)
        logger.info(f"Gateway daemon API started at {self.api_server.url}")

    def cleanup(self):
        logger.warning("Shutting down gateway daemon")
        self.api_server.shutdown()

    def run(self):
        setproctitle.setproctitle(f"skylark-gateway-daemon")
        exit_flag = Event()

        def exit_handler(signum, frame):
            logger.warning("Received signal {}. Exiting...".format(signum))
            exit_flag.set()
            self.gateway_receiver.stop_servers()
            self.gateway_sender.stop_workers()
            sys.exit(0)

        logger.info("Starting gateway sender workers")
        self.gateway_sender.start_workers()
        signal.signal(signal.SIGINT, exit_handler)
        signal.signal(signal.SIGTERM, exit_handler)

        logger.info("Starting daemon loop")
        while not exit_flag.is_set():
            # queue object uploads and relays
            for chunk_req in self.chunk_store.get_chunk_requests(ChunkState.downloaded):
                if len(chunk_req.path) > 0:
                    current_hop = chunk_req.path[0]
                    if current_hop.chunk_location_type == "dst_object_store":
                        logger.warning(f"NOT IMPLEMENTED: Queuing object store upload for chunk {chunk_req.chunk.chunk_id}")
                        self.chunk_store.state_fail(chunk_req.chunk.chunk_id)
                    elif current_hop.chunk_location_type == "relay" or current_hop.chunk_location_type.startswith("random_"):
                        logger.info(f"Queuing chunk {chunk_req.chunk.chunk_id} for relay")
                        self.gateway_sender.queue_request(chunk_req)
                        self.chunk_store.state_queue_upload(chunk_req.chunk.chunk_id)
                    elif current_hop.chunk_location_type == "save_local":  # do nothing, save to ChunkStore
                        self.chunk_store.state_queue_upload(chunk_req.chunk.chunk_id)
                        self.chunk_store.state_start_upload(chunk_req.chunk.chunk_id)
                        self.chunk_store.state_finish_upload(chunk_req.chunk.chunk_id)
                    else:
                        logger.error(f"Unknown chunk location type {current_hop.chunk_location_type}")
                        self.chunk_store.state_fail(chunk_req.chunk.chunk_id)
                        raise ValueError(f"Unknown or incorrect chunk_location_type {current_hop.chunk_location_type}")
                else:
                    logger.error(f"Ready to upload chunk {chunk_req.chunk.chunk_id} has no hops")

            # queue object store downloads and relays (if space is available)
            # todo ensure space is available
            for chunk_req in self.chunk_store.get_chunk_requests(ChunkState.registered):
                if len(chunk_req.path) > 0:
                    current_hop = chunk_req.path[0]
                    if current_hop.chunk_location_type == "src_object_store":
                        logger.warning(f"NOT IMPLEMENTED: Queuing object store download for chunk {chunk_req.chunk.chunk_id}")
                        self.chunk_store.state_fail(chunk_req.chunk.chunk_id)
                    elif current_hop.chunk_location_type.startswith("random_"):
                        self.chunk_store.state_start_download(chunk_req.chunk.chunk_id)
                        size_mb_match = re.search(r"random_(\d+)MB", current_hop.chunk_location_type)
                        assert size_mb_match is not None
                        size_mb = int(size_mb_match.group(1))

                        def fn(chunk_req, size_mb):
                            fpath = str(self.chunk_store.get_chunk_file_path(chunk_req.chunk.chunk_id).absolute())
                            os.system(f"dd if=/dev/zero of={fpath} bs={MB} count={size_mb}")
                            chunk_req.chunk.chunk_length_bytes = os.path.getsize(fpath)
                            self.chunk_store.chunk_requests[chunk_req.chunk.chunk_id] = chunk_req
                            self.chunk_store.state_finish_download(chunk_req.chunk.chunk_id)

                        threading.Thread(target=fn, args=(chunk_req, size_mb)).start()
                    elif current_hop.chunk_location_type == "relay" or current_hop.chunk_location_type == "save_local":
                        # do nothing, waiting for chunk to be be ready_to_upload
                        continue
                    else:
                        logger.error(f"Unknown chunk location type {current_hop.chunk_location_type}")
                        self.chunk_store.state_fail(chunk_req.chunk.chunk_id)
                        raise ValueError(f"Unknown or incorrect chunk_location_type {current_hop.chunk_location_type}")
                else:
                    logger.error(f"Registered chunk {chunk_req.chunk.chunk_id} has no hops")


if __name__ == "__main__":
    print_header()
    parser = argparse.ArgumentParser(description="Skylark Gateway Daemon")
    parser.add_argument("--chunk-dir", type=Path, default="/dev/shm/skylark/chunks", help="Directory to store chunks")
    parser.add_argument("--debug", action="store_true", help="Enable debug mode for Flask")
    parser.add_argument("--log-dir", type=Path, default=Path("/var/log/skylark"), help="Directory to write logs to")
    parser.add_argument("--outgoing-connections", type=int, default=1, help="Number of outgoing connections to make to the next relay")
    args = parser.parse_args()

    daemon = GatewayDaemon(
        chunk_dir=args.chunk_dir, debug=args.debug, log_dir=Path(args.log_dir), outgoing_connections=args.outgoing_connections
    )
    daemon.run()
