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

from skylark.obj_store.s3_interface import S3Interface


class GatewayDaemon:
    def __init__(self, chunk_dir: PathLike, debug=False, log_dir: Optional[PathLike] = None, outgoing_connections=1):
        if log_dir is not None:
            log_dir = Path(log_dir)
            log_dir.mkdir(exist_ok=True)
            logger.remove()
            logger.add(log_dir / "gateway_daemon.log", rotation="10 MB", enqueue=True)
            logger.add(sys.stderr, colorize=True, format="{function:>15}:{line:<3} {level:<8} {message}", level="DEBUG", enqueue=True)
        self.chunk_store = ChunkStore(chunk_dir)
        self.gateway_receiver = GatewayReceiver(chunk_store=self.chunk_store)
        self.gateway_sender = GatewaySender(chunk_store=self.chunk_store, n_processes=outgoing_connections)

        # S3 interface objects 
        # TODO: figure out if awscrt parallelizes with a single interface
        self.s3_interface_objs: Dict[str, S3Interface] = {}

        # API server
        atexit.register(self.cleanup)
        self.api_server = GatewayDaemonAPI(
            self.chunk_store, self.gateway_receiver, debug=debug, log_dir=log_dir, daemon_cleanup_handler=self.cleanup
        )
        self.api_server.start()
        logger.info(f"Gateway daemon API started at {self.api_server.url}")

    def cleanup(self):
        logger.warning("Shutting down gateway daemon")
        self.api_server.shutdown()

    def get_obj_store_interface(region, bucket):

        # TODO: GCP/Azure support
        if region in self.s3_interface_objs: # cached interface
            return self.s3_interface_objs[region]

        s3_interface = S3Interface(region.split(":")[1], bucket, use_tls=False)
        self.s3_interface_objs[region] = s3_interface
        return s3_interface



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
                        self.chunk_store.state_queue_upload(chunk_req.chunk.chunk_id)
                        self.chunk_store.state_start_upload(chunk_req.chunk.chunk_id)

                        # function to upload data from S3
                        def fn(chunk_req, dst_region, dst_bucket):
                            fpath = str(self.chunk_store.get_chunk_file_path(chunk_req.chunk.chunk_id).absolute())
                            s3_interface = self.get_obj_store_interface(dst_region, dst_bucket)
                            logger.info(f"Waiting for upload {dst_bucket}:{chunk_req.chunk.key}")
                            s3_interface.upload_object(fpath, chunk_req.chunk.key).result()
                            logger.info(f"Uploaded {fpath} to {dst_bucket}:{chunk_req.chunk.key})")
                            self.chunk_store.state_finish_upload(chunk_req.chunk.chunk_id)

                        # start in seperate thread
                        threading.Thread(
                            target=fn, args=(chunk_req, current_hop.dst_object_store_region, current_hop.dst_object_store_bucket)
                        ).start()

                    elif (
                        current_hop.chunk_location_type == "relay"
                        or current_hop.chunk_location_type.startswith("random_")
                        or current_hop.chunk_location_type == "src_object_store"
                    ):
                        logger.info(f"Queuing chunk {chunk_req.chunk.chunk_id} for relay")
                        self.gateway_sender.queue_request(chunk_req)
                        self.chunk_store.state_queue_upload(chunk_req.chunk.chunk_id)
                    elif current_hop.chunk_location_type == "save_local":  # do nothing, save to ChunkStore
                        logger.info(f"Save local {chunk_req.chunk.chunk_id}")
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

                        self.chunk_store.state_start_download(chunk_req.chunk.chunk_id)

                        src_bucket = current_hop.src_object_store_bucket
                        src_region = current_hop.src_object_store_region

                        # function to download data from S3
                        def fn(chunk_req, src_region, src_bucket):
                            fpath = str(self.chunk_store.get_chunk_file_path(chunk_req.chunk.chunk_id).absolute())
                            s3_interface = self.get_obj_store_interface(src_region, src_bucket)

                            logger.info(f"Waiting for download {src_bucket}:{chunk_req.chunk.key}")
                            s3_interface.download_object(chunk_req.chunk.key, fpath).result()
                            logger.info(f"Downloaded key {chunk_req.chunk.key} to {fpath})")
                            self.chunk_store.chunk_requests[chunk_req.chunk.chunk_id] = chunk_req
                            self.chunk_store.state_finish_download(chunk_req.chunk.chunk_id)

                        # start in seperate thread
                        threading.Thread(target=fn, args=(chunk_req, src_region, src_bucket)).start()

                    elif current_hop.chunk_location_type.startswith("random_"):
                        self.chunk_store.state_start_download(chunk_req.chunk.chunk_id)

                        size_mb_match = re.search(r"random_(\d+)MB", current_hop.chunk_location_type)
                        assert size_mb_match is not None
                        size_mb = int(size_mb_match.group(1))

                        # function to write random data file
                        def fn(chunk_req, size_mb):
                            fpath = str(self.chunk_store.get_chunk_file_path(chunk_req.chunk.chunk_id).absolute())
                            logger.info(f"Writing random data path {fpath}")
                            os.system(f"dd if=/dev/zero of={fpath} bs={MB} count={size_mb}")
                            chunk_req.chunk.chunk_length_bytes = os.path.getsize(fpath)
                            self.chunk_store.chunk_requests[chunk_req.chunk.chunk_id] = chunk_req
                            self.chunk_store.state_finish_download(chunk_req.chunk.chunk_id)

                        # generate random data in seperate thread
                        threading.Thread(target=fn, args=(chunk_req, size_mb)).start()
                    elif (
                        current_hop.chunk_location_type == "relay"
                        or current_hop.chunk_location_type == "save_local"
                        or current_hop.chunk_location_type == "dst_object_store"
                    ):
                        # do nothing, waiting for chunk to be be ready_to_upload
                        continue
                    else:
                        is_store = current_hop.chunk_location_type == "src_object_store"
                        logger.error(f"Unknown chunk location type {current_hop.chunk_location_type}, {is_store}")
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
