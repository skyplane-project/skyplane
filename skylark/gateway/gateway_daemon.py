import argparse
import atexit
import json
import os
import re
import signal
import sys
import threading
from multiprocessing import Event
from os import PathLike
from pathlib import Path
from typing import Dict, Optional

import setproctitle
from loguru import logger
from skylark import MB, print_header
from skylark.chunk import ChunkState
from skylark.gateway.chunk_store import ChunkStore
from skylark.gateway.gateway_daemon_api import GatewayDaemonAPI
from skylark.gateway.gateway_receiver import GatewayReceiver
from skylark.gateway.gateway_sender import GatewaySender

from skylark.obj_store.s3_interface import S3Interface
from skylark.obj_store.gcs_interface import GCSInterface
from skylark.obj_store.object_store_interface import ObjectStoreInterface


class GatewayDaemon:
    def __init__(
        self,
        region: str,
        outgoing_ports: Dict[str, int],
        chunk_dir: PathLike,
        max_incoming_ports=64,
        debug=False,
        log_dir: Optional[PathLike] = None,
    ):
        if log_dir is not None:
            log_dir = Path(log_dir)
            log_dir.mkdir(exist_ok=True)
            logger.remove()
            logger.add(log_dir / "gateway_daemon.log", rotation="10 MB", enqueue=True)
            logger.add(sys.stderr, colorize=True, format="{function:>15}:{line:<3} {level:<8} {message}", level="DEBUG", enqueue=True)
        self.region = region
        self.chunk_store = ChunkStore(chunk_dir)
        self.gateway_receiver = GatewayReceiver(chunk_store=self.chunk_store, max_pending_chunks=max_incoming_ports)
        self.gateway_sender = GatewaySender(chunk_store=self.chunk_store, outgoing_ports=outgoing_ports)
        self.obj_store_interfaces: Dict[str, ObjectStoreInterface] = {}

        # API server
        atexit.register(self.cleanup)
        self.api_server = GatewayDaemonAPI(
            self.chunk_store, self.gateway_receiver, debug=debug, log_dir=log_dir, daemon_cleanup_handler=self.cleanup
        )
        self.api_server.start()
        logger.info(f"Gateway daemon API started at {self.api_server.url}")

    def get_obj_store_interface(self, region: str, bucket: str) -> ObjectStoreInterface:
        # return cached interface
        if region in self.obj_store_interfaces:
            return self.obj_store_interfaces[region]

        # create new interface
        if region.startswith("aws"):
            self.obj_store_interfaces[region] = S3Interface(region.split(":")[1], bucket, use_tls=False)
        elif region.startswith("gcp"):
            self.obj_store_interfaces[region] = GCSInterface(region.split(":")[1][:-2], bucket)
        else:
            ValueError(f"Invalid region {region} - could not create interface")

        return self.obj_store_interfaces[region]

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
                if self.region == chunk_req.dst_region and chunk_req.dst_type == "save_local":  # do nothing, save to ChunkStore
                    logger.info(f"Save local {chunk_req.chunk.chunk_id}")
                    self.chunk_store.state_queue_upload(chunk_req.chunk.chunk_id)
                    self.chunk_store.state_start_upload(chunk_req.chunk.chunk_id)
                    self.chunk_store.state_finish_upload(chunk_req.chunk.chunk_id)
                elif self.region == chunk_req.dst_region and chunk_req.dst_type == "object_store":
                    self.chunk_store.state_queue_upload(chunk_req.chunk.chunk_id)
                    self.chunk_store.state_start_upload(chunk_req.chunk.chunk_id)

                    # function to upload data to object store
                    def fn(chunk_req, dst_region, dst_bucket):
                        fpath = str(self.chunk_store.get_chunk_file_path(chunk_req.chunk.chunk_id).absolute())
                        logger.info(f"Creating interface {dst_region}--{dst_bucket}")
                        obj_store_interface = self.get_obj_store_interface(dst_region, dst_bucket)
                        logger.info(f"Waiting for upload {dst_bucket}:{chunk_req.chunk.key}")
                        obj_store_interface.upload_object(fpath, chunk_req.chunk.key).result()
                        logger.info(f"Uploaded {fpath} to {dst_bucket}:{chunk_req.chunk.key})")
                        self.chunk_store.state_finish_upload(chunk_req.chunk.chunk_id)

                    # start in seperate thread
                    threading.Thread(target=fn, args=(chunk_req, chunk_req.dst_region, chunk_req.dst_object_store_bucket)).start()
                elif self.region != chunk_req.dst_region:
                    logger.info(f"Queuing chunk {chunk_req.chunk.chunk_id} for relay")
                    self.gateway_sender.queue_request(chunk_req)
                    self.chunk_store.state_queue_upload(chunk_req.chunk.chunk_id)
                else:
                    self.chunk_store.state_fail(chunk_req.chunk.chunk_id)
                    raise ValueError(f"No upload handler for ChunkRequest: {chunk_req}")

            # queue object store downloads and relays (if space is available)
            # todo ensure space is available
            for chunk_req in self.chunk_store.get_chunk_requests(ChunkState.registered):
                if self.region == chunk_req.src_region and chunk_req.src_type == "read_local":  # do nothing, read from local ChunkStore
                    logger.info(f"Read local {chunk_req.chunk.chunk_id}")
                    self.chunk_store.state_start_download(chunk_req.chunk.chunk_id)
                    self.chunk_store.state_finish_download(chunk_req.chunk.chunk_id)
                elif self.region == chunk_req.src_region and chunk_req.src_type == "object_store":
                    self.chunk_store.state_start_download(chunk_req.chunk.chunk_id)

                    # function to download data from source object store
                    def fn(chunk_req, src_region, src_bucket):
                        fpath = str(self.chunk_store.get_chunk_file_path(chunk_req.chunk.chunk_id).absolute())
                        logger.info(f"Creating interface {src_region}--{src_bucket}")
                        obj_store_interface = self.get_obj_store_interface(src_region, src_bucket)
                        logger.info(f"Waiting for download {src_bucket}:{chunk_req.chunk.key}")
                        obj_store_interface.download_object(chunk_req.chunk.key, fpath).result()
                        logger.info(f"Downloaded key {chunk_req.chunk.key} to {fpath})")
                        self.chunk_store.chunk_requests[chunk_req.chunk.chunk_id] = chunk_req
                        self.chunk_store.state_finish_download(chunk_req.chunk.chunk_id)

                    # start in seperate thread
                    threading.Thread(target=fn, args=(chunk_req, chunk_req.src_region, chunk_req.src_object_store_bucket)).start()
                elif self.region == chunk_req.src_region and chunk_req.src_type == "random":
                    self.chunk_store.state_start_download(chunk_req.chunk.chunk_id)
                    size_mb = chunk_req.src_random_size_mb

                    # function to write random data file
                    def fn(chunk_req, size_mb):
                        fpath = str(self.chunk_store.get_chunk_file_path(chunk_req.chunk.chunk_id).absolute())
                        os.system(f"fallocate -l {size_mb * MB} {fpath}")
                        chunk_req.chunk.chunk_length_bytes = os.path.getsize(fpath)
                        self.chunk_store.chunk_requests[chunk_req.chunk.chunk_id] = chunk_req
                        self.chunk_store.state_finish_download(chunk_req.chunk.chunk_id)

                    # generate random data in seperate thread
                    threading.Thread(target=fn, args=(chunk_req, size_mb)).start()
                elif self.region != chunk_req.src_region:  # do nothing, waiting for chunk to be be ready_to_upload
                    continue
                else:
                    self.chunk_store.state_fail(chunk_req.chunk.chunk_id)
                    raise ValueError(f"No download handler for ChunkRequest: {chunk_req}")


if __name__ == "__main__":
    print_header()
    parser = argparse.ArgumentParser(description="Skylark Gateway Daemon")
    parser.add_argument("--region", type=str, required=True, help="Region tag (provider:region")
    parser.add_argument(
        "--outgoing-ports", type=str, required=True, help="JSON encoded path mapping destination ip to number of outgoing ports"
    )
    parser.add_argument("--chunk-dir", type=Path, default="/dev/shm/skylark/chunks", help="Directory to store chunks")
    parser.add_argument("--debug", action="store_true", help="Enable debug mode for Flask")
    parser.add_argument("--log-dir", type=Path, default=Path("/var/log/skylark"), help="Directory to write logs to")
    args = parser.parse_args()

    daemon = GatewayDaemon(
        region=args.region,
        outgoing_ports=json.loads(args.outgoing_ports),
        chunk_dir=args.chunk_dir,
        debug=args.debug,
        log_dir=Path(args.log_dir),
    )
    daemon.run()
