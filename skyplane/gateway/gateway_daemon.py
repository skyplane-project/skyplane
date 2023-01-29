import argparse
import atexit
import json
import os
import signal
import sys
import threading
import time
from multiprocessing import Event, Queue
from os import PathLike
from pathlib import Path
from threading import BoundedSemaphore

from typing import Dict

from skyplane.chunk import ChunkState
from skyplane.gateway.chunk_store import ChunkStore
from skyplane.gateway.gateway_daemon_api import GatewayDaemonAPI
from skyplane.gateway.gateway_obj_store import GatewayObjStoreConn
from skyplane.gateway.gateway_receiver import GatewayReceiver
from skyplane.gateway.gateway_sender import GatewaySender
from skyplane.utils import logger
from skyplane.utils.definitions import MB


class GatewayDaemon:
    def __init__(
        self,
        region: str,
        outgoing_ports: Dict[str, int],
        chunk_dir: PathLike,
        max_inflight_chunks=64,
        use_tls=True,
        use_compression=False,
        use_e2ee=True,
    ):
        # todo max_inflight_chunks should be configurable rather than static
        self.region = region
        self.max_inflight_chunks = max_inflight_chunks
        self.chunk_store = ChunkStore(chunk_dir)
        self.error_event = Event()
        self.error_queue = Queue()
        if use_e2ee:
            e2ee_key_path = Path(os.environ["E2EE_KEY_FILE"]).expanduser()
            with open(e2ee_key_path, "rb") as f:
                e2ee_key_bytes = f.read()
        else:
            e2ee_key_bytes = None
        self.gateway_receiver = GatewayReceiver(
            region,
            self.chunk_store,
            self.error_event,
            self.error_queue,
            max_pending_chunks=max_inflight_chunks,
            use_tls=use_tls,
            use_compression=use_compression,
            e2ee_key_bytes=e2ee_key_bytes,
        )
        self.gateway_sender = GatewaySender(
            region,
            self.chunk_store,
            self.error_event,
            self.error_queue,
            outgoing_ports=outgoing_ports,
            use_tls=use_tls,
            use_compression=use_compression,
            e2ee_key_bytes=e2ee_key_bytes,
        )
        provider = region.split(":")[0]
        if provider == "azure":
            n_conn = 24  # due to throttling limits from authentication
        else:
            n_conn = 32
        self.obj_store_conn = GatewayObjStoreConn(self.chunk_store, self.error_event, self.error_queue, max_conn=n_conn)

        # Download thread pool
        self.dl_pool_semaphore = BoundedSemaphore(value=128)
        self.ul_pool_semaphore = BoundedSemaphore(value=128)

        # API server
        self.api_server = GatewayDaemonAPI(self.chunk_store, self.gateway_receiver, self.error_event, self.error_queue)
        self.api_server.start()
        atexit.register(self.api_server.shutdown)
        logger.info(f"[gateway_daemon] API started at {self.api_server.url}")

    def run(self):
        exit_flag = Event()

        def exit_handler(signum, frame):
            logger.warning("[gateway_daemon] Received signal {}. Exiting...".format(signum))
            exit_flag.set()
            self.gateway_receiver.stop_servers()
            self.gateway_sender.stop_workers()
            self.obj_store_conn.stop_workers()
            sys.exit(0)

        logger.info("[gateway_daemon] Starting gateway sender workers")
        self.gateway_sender.start_workers()
        self.obj_store_conn.start_workers()
        signal.signal(signal.SIGINT, exit_handler)
        signal.signal(signal.SIGTERM, exit_handler)

        logger.info("[gateway_daemon] Starting daemon loop")
        try:
            while not exit_flag.is_set() and not self.error_event.is_set():
                # queue object uploads and relays
                for chunk_req in self.chunk_store.get_chunk_requests(ChunkState.downloaded):
                    if self.region == chunk_req.dst_region and chunk_req.dst_type == "save_local":  # do nothing, save to ChunkStore
                        self.chunk_store.state_queue_upload(chunk_req.chunk.chunk_id)
                        self.chunk_store.state_start_upload(chunk_req.chunk.chunk_id, "save_local")
                        self.chunk_store.state_finish_upload(chunk_req.chunk.chunk_id, "save_local")
                        self.chunk_store.get_chunk_file_path(chunk_req.chunk.chunk_id).unlink()
                    elif self.region == chunk_req.dst_region and chunk_req.dst_type == "object_store":
                        self.chunk_store.state_queue_upload(chunk_req.chunk.chunk_id)
                        self.obj_store_conn.queue_upload_request(chunk_req)
                    elif self.region != chunk_req.dst_region:
                        self.gateway_sender.queue_request(chunk_req)
                        self.chunk_store.state_queue_upload(chunk_req.chunk.chunk_id)
                    else:
                        self.chunk_store.state_fail(chunk_req.chunk.chunk_id)
                        self.error_queue.put(ValueError("[gateway_daemon] Unknown destination type"))
                        self.error_event.set()

                # queue object store downloads and relays (if space is available)
                for chunk_req in self.chunk_store.get_chunk_requests(ChunkState.registered):
                    if self.region == chunk_req.src_region and chunk_req.src_type == "read_local":  # do nothing, read from local ChunkStore
                        self.chunk_store.state_queue_download(chunk_req.chunk.chunk_id)
                        self.chunk_store.state_start_download(chunk_req.chunk.chunk_id, "read_local")
                        self.chunk_store.state_finish_download(chunk_req.chunk.chunk_id, "read_local")
                    elif self.region == chunk_req.src_region and chunk_req.src_type == "random":
                        size_mb = chunk_req.src_random_size_mb
                        assert chunk_req.src_random_size_mb is not None, "Random chunk size not set"
                        if self.chunk_store.remaining_bytes() >= size_mb * MB * self.max_inflight_chunks:

                            def fn(chunk_req, size_mb):
                                while self.chunk_store.remaining_bytes() < size_mb * MB * self.max_inflight_chunks:
                                    time.sleep(0.1)
                                self.chunk_store.state_start_download(chunk_req.chunk.chunk_id, "random")
                                fpath = str(self.chunk_store.get_chunk_file_path(chunk_req.chunk.chunk_id).absolute())
                                with self.dl_pool_semaphore:
                                    size_bytes = int(size_mb * MB)
                                    assert size_bytes > 0, f"Invalid size {size_bytes} for fallocate"
                                    os.system(f"fallocate -l {size_bytes} {fpath}")
                                chunk_req.chunk.chunk_length_bytes = os.path.getsize(fpath)
                                self.chunk_store.chunk_requests[chunk_req.chunk.chunk_id] = chunk_req
                                self.chunk_store.state_finish_download(chunk_req.chunk.chunk_id, "random")

                            self.chunk_store.state_queue_download(chunk_req.chunk.chunk_id)
                            threading.Thread(target=fn, args=(chunk_req, size_mb)).start()
                    elif self.region == chunk_req.src_region and chunk_req.src_type == "object_store":
                        self.chunk_store.state_queue_download(chunk_req.chunk.chunk_id)
                        self.obj_store_conn.queue_download_request(chunk_req)
                    elif self.region != chunk_req.src_region:  # do nothing, waiting for chunk to be be ready_to_upload
                        continue
                    else:
                        self.chunk_store.state_fail(chunk_req.chunk.chunk_id)
                        self.error_queue.put(ValueError("[gateway_daemon] Unknown source type"))
                        self.error_event.set()
                time.sleep(0.1)  # yield
        except Exception as e:
            self.error_queue.put(e)
            self.error_event.set()
            logger.error(f"[gateway_daemon] Exception in daemon loop: {e}")
            logger.exception(e)

        # shut down workers except for API to report status
        logger.info("[gateway_daemon] Exiting all workers except for API")
        self.gateway_sender.stop_workers()
        self.gateway_receiver.stop_servers()
        self.obj_store_conn.stop_workers()
        logger.info("[gateway_daemon] Done")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Skyplane Gateway Daemon")
    parser.add_argument("--region", type=str, required=True, help="Region tag (provider:region")
    parser.add_argument(
        "--outgoing-ports", type=str, required=True, help="JSON encoded path mapping destination ip to number of outgoing ports"
    )
    parser.add_argument("--chunk-dir", type=Path, default="/tmp/skyplane/chunks", help="Directory to store chunks")
    parser.add_argument("--disable-tls", action="store_true")
    parser.add_argument("--use-compression", action="store_true")
    parser.add_argument("--disable-e2ee", action="store_true")
    args = parser.parse_args()

    os.makedirs(args.chunk_dir)
    daemon = GatewayDaemon(
        region=args.region,
        outgoing_ports=json.loads(args.outgoing_ports),
        chunk_dir=args.chunk_dir,
        use_tls=not args.disable_tls,
        use_compression=args.use_compression,
        use_e2ee=not args.disable_e2ee,
    )
    daemon.run()
