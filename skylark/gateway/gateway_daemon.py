import argparse
import atexit
from multiprocessing import Event
import os
import re
import signal
from os import PathLike
from pathlib import Path
import sys
import time
from typing import Optional

from loguru import logger
from skylark.gateway.chunk_store import ChunkRequest, ChunkState, ChunkStore

from skylark.gateway.gateway_reciever import GatewayReciever
from skylark.gateway.gateway_daemon_api import GatewayDaemonAPI
from skylark.gateway.gateway_sender import GatewaySender


class GatewayDaemon:
    def __init__(self, chunk_dir: PathLike, debug=False, log_dir: Optional[PathLike] = None, outgoing_connections=1, outgoing_batch_size=1):
        if log_dir is not None:
            log_dir = Path(log_dir)
            log_dir.mkdir(exist_ok=True)
            logger.add(log_dir / "gateway_daemon.log", rotation="10 MB")
            logger.add(sys.stderr, level="DEBUG" if debug else "INFO")
        self.chunk_store = ChunkStore(chunk_dir)
        self.gateway_reciever = GatewayReciever(chunk_store=self.chunk_store)
        self.gateway_sender = GatewaySender(chunk_store=self.chunk_store, n_processes=outgoing_connections, batch_size=outgoing_batch_size)

        # API server
        self.api_server = GatewayDaemonAPI(self.chunk_store, self.gateway_reciever, debug=debug, log_dir=log_dir)
        self.api_server.start()
        atexit.register(self.cleanup)
        logger.info("Gateway daemon API started")

    def cleanup(self):
        logger.warning("Shutting down gateway daemon")
        self.api_server.shutdown()

    def run(self):
        exit_flag = Event()

        def exit_handler(signum, frame):
            logger.warning("Received signal {}. Exiting...".format(signum))
            exit_flag.set()
            self.gateway_reciever.stop_servers()
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
                        self.chunk_store.fail(chunk_req.chunk.chunk_id)
                    elif (
                        current_hop.chunk_location_type == "src_object_store"
                        or current_hop.chunk_location_type == "relay"
                        or current_hop.chunk_location_type.startswith("random_")
                    ):
                        logger.info(f"Queuing chunk {chunk_req.chunk.chunk_id} for relay")
                        self.gateway_sender.queue_request(chunk_req)
                        self.chunk_store.start_upload(chunk_req.chunk.chunk_id)
                    elif current_hop.chunk_location_type == "save_local":
                        # do nothing, done
                        pass
                    else:
                        logger.error(f"Unknown chunk location type {current_hop.chunk_location_type}")
                        self.chunk_store.fail(chunk_req.chunk.chunk_id)
                        raise ValueError(f"Unknown or incorrect chunk_location_type {current_hop.chunk_location_type}")
                else:
                    logger.error(f"Ready to upload chunk {chunk_req.chunk_id} has no hops")

            # queue object store downloads and relays (if space is available)
            # todo ensure space is available
            for chunk_req in self.chunk_store.get_chunk_requests(ChunkState.registered):
                if len(chunk_req.path) > 0:
                    current_hop = chunk_req.path[0]
                    if current_hop.chunk_location_type == "src_object_store":
                        logger.warning(f"NOT IMPLEMENTED: Queuing object store download for chunk {chunk_req.chunk.chunk_id}")
                        self.chunk_store.fail(chunk_req.chunk.chunk_id)
                    elif current_hop.chunk_location_type.startswith("random_"):
                        self.chunk_store.start_download(chunk_req.chunk.chunk_id)
                        size_mb = int(re.search(r"random_(\d+)MB", current_hop.chunk_location_type).group(1))
                        logger.info(f"Generating {size_mb}MB random chunk {chunk_req.chunk.chunk_id}")
                        with self.chunk_store.get_chunk_file_path(chunk_req.chunk.chunk_id).open("wb") as f:
                            f.write(os.urandom(int(size_mb * 1e6)))

                        # update chunk size
                        chunk_req.chunk.chunk_length_bytes = int(size_mb * 1e6)
                        self.chunk_store.chunk_requests[chunk_req.chunk.chunk_id] = chunk_req
                        self.chunk_store.finish_download(chunk_req.chunk.chunk_id)
                    elif current_hop.chunk_location_type == "relay" or current_hop.chunk_location_type == "save_local":
                        # do nothing, waiting for chunk to be be ready_to_upload
                        continue
                    else:
                        logger.error(f"Unknown chunk location type {current_hop.chunk_location_type}")
                        self.chunk_store.fail(chunk_req.chunk.chunk_id)
                        raise ValueError(f"Unknown or incorrect chunk_location_type {current_hop.chunk_location_type}")
                else:
                    logger.error(f"Registered chunk {chunk_req.chunk_id} has no hops")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Skylark Gateway Daemon")
    parser.add_argument("--chunk-dir", type=Path, default="/dev/shm/skylark/chunks", required=True, help="Directory to store chunks")
    parser.add_argument("--debug", action="store_true", help="Enable debug mode for Flask")
    parser.add_argument("--log-dir", type=Path, default=Path("/var/log/skylark"), help="Directory to write logs to")
    parser.add_argument("--outgoing-connections", type=int, default=1, help="Number of outgoing connections to make to the next relay")
    args = parser.parse_args()
    daemon = GatewayDaemon(
        chunk_dir=args.chunk_dir, debug=args.debug, log_dir=Path(args.log_dir), outgoing_connections=args.outgoing_connections
    )
    daemon.run()
