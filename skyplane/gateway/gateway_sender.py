import json
import queue
import socket
import ssl
import time
import traceback
import nacl.secret
from functools import partial
from multiprocessing import Event, Process, Queue
from typing import Dict, List, Optional
import urllib3

import lz4.frame

from skyplane import MB
from skyplane.chunk import ChunkRequest
from skyplane.gateway.chunk_store import ChunkStore
from skyplane.utils import logger
from skyplane.utils.retry import retry_backoff
from skyplane.utils.timer import Timer


class GatewaySender:
    def __init__(
        self,
        region: str,
        chunk_store: ChunkStore,
        error_event,
        error_queue: Queue,
        outgoing_ports: Dict[str, int],
        use_tls: bool = True,
        use_compression: bool = True,
        e2ee_key_bytes: Optional[bytes] = None,
    ):
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        self.region = region
        self.chunk_store = chunk_store
        self.error_event = error_event
        self.error_queue = error_queue
        self.outgoing_ports = outgoing_ports
        self.use_compression = use_compression
        if e2ee_key_bytes is None:
            self.e2ee_secretbox = None
        else:
            self.e2ee_secretbox = nacl.secret.SecretBox(e2ee_key_bytes)
        self.n_processes = sum(outgoing_ports.values())
        self.processes = []

        # SSL context
        if use_tls:
            self.ssl_context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
            self.ssl_context.check_hostname = False
            self.ssl_context.verify_mode = ssl.CERT_NONE
            logger.info(f"Using {str(ssl.OPENSSL_VERSION)}")
        else:
            self.ssl_context = None

        # shared state
        self.worker_queue: queue.Queue[int] = Queue()
        self.exit_flags = [Event() for _ in range(self.n_processes)]

        # process-local state
        self.worker_id: Optional[int] = None
        self.sender_port: Optional[int] = None
        self.destination_ports: Dict[str, int] = {}  # ip_address -> int
        self.destination_sockets: Dict[str, socket.socket] = {}  # ip_address -> socket
        self.sent_chunk_ids: Dict[str, List[int]] = {}  # ip_address -> list of chunk_ids
        self.http_pool = urllib3.PoolManager(retries=urllib3.Retry(total=3), cert_reqs="CERT_NONE")

    def start_workers(self):
        for ip, num_connections in self.outgoing_ports.items():
            for i in range(num_connections):
                p = Process(target=self.worker_loop, args=(i, ip))
                p.start()
                self.processes.append(p)

    def stop_workers(self):
        for i in range(self.n_processes):
            self.exit_flags[i].set()
        for p in self.processes:
            p.join()
        self.processes = []

    def worker_loop(self, worker_id: int, dest_ip: str):
        self.worker_id = worker_id
        while not self.exit_flags[worker_id].is_set() and not self.error_event.is_set():
            try:
                try:
                    next_chunk_id = self.worker_queue.get_nowait()
                except queue.Empty:
                    continue

                logger.debug(f"[sender:{self.worker_id}] Sending chunk ID {next_chunk_id} to IP {dest_ip}")
                self.chunk_store.get_chunk_request(next_chunk_id)
                self.send_chunks([next_chunk_id], dest_ip)
                if dest_ip not in self.sent_chunk_ids:
                    self.sent_chunk_ids[dest_ip] = []
                self.sent_chunk_ids[dest_ip].append(next_chunk_id)
            except Exception as e:
                logger.error(f"[sender:{self.worker_id}] Exception: {e}")
                self.error_queue.put(traceback.format_exc())
                self.error_event.set()
                self.exit_flags[worker_id].set()

        # close destination sockets
        logger.info(f"[sender:{worker_id}] exiting, closing sockets")
        for dst_socket in self.destination_sockets.values():
            dst_socket.close()

        # wait for all chunks to reach state "downloaded"
        def wait_for_chunks():
            cr_status = {}
            for ip, ip_chunk_ids in self.sent_chunk_ids.items():
                response = self.http_pool.request("GET", f"https://{ip}:8080/api/v1/incomplete_chunk_requests")
                assert response.status == 200, f"{response.status_code} {response.data}"
                host_state = json.loads(response.data.decode("utf-8"))["chunk_requests"]
                for chunk_id in ip_chunk_ids:
                    if chunk_id in host_state:
                        cr_status[chunk_id] = host_state[chunk_id]["state"]
            return all(status not in ["registered", "download_queued", "download_in_progress"] for status in cr_status.values())

        logger.info(f"[sender:{worker_id}] waiting for chunks to reach state 'downloaded'")
        wait_success = False
        for _ in range(60):
            if wait_for_chunks():
                wait_success = True
                break
            time.sleep(1)
        if not wait_success:
            raise Exception("Timed out waiting for chunks to reach state 'downloaded'")
        logger.info(f"[sender:{worker_id}] all chunks reached state 'downloaded'")

        # close servers
        logger.info(f"[sender:{worker_id}] exiting, closing servers")
        for dst_host, dst_port in self.destination_ports.items():
            response = self.http_pool.request("DELETE", f"https://{dst_host}:8080/api/v1/servers/{dst_port}")
            assert response.status == 200 and json.loads(response.data.decode("utf-8")) == {"status": "ok"}
            logger.info(f"[sender:{worker_id}] closed destination socket {dst_host}:{dst_port}")

    def queue_request(self, chunk_request: ChunkRequest):
        self.worker_queue.put(chunk_request.chunk.chunk_id)

    def make_socket(self, dst_host):
        response = self.http_pool.request("POST", f"https://{dst_host}:8080/api/v1/servers")
        assert response.status == 200, f"{response.status} {response.data.decode('utf-8')}"
        self.destination_ports[dst_host] = int(json.loads(response.data.decode("utf-8"))["server_port"])
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((dst_host, self.destination_ports[dst_host]))
        original_timeout = sock.gettimeout()
        sock.settimeout(30.0)  # For the TLS handshake
        logger.info(f"[sender:{self.worker_id}] started new server connection to {dst_host}:{self.destination_ports[dst_host]}")
        if self.ssl_context is not None:
            sock = self.ssl_context.wrap_socket(sock)
            logger.info(f"[sender:{self.worker_id}] finished TLS handshake to {dst_host}:{self.destination_ports[dst_host]}")
        sock.settimeout(original_timeout)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        return sock

    # send chunks to other instances
    def send_chunks(self, chunk_ids: List[int], dst_host: str):
        """Send list of chunks to gateway server, pipelining small chunks together into a single socket stream."""
        # notify server of upcoming ChunkRequests
        with Timer(f"prepare to pre-register chunks {chunk_ids} to {dst_host}"):
            logger.debug(f"[sender:{self.worker_id}]:{chunk_ids} pre-registering chunks")
            chunk_reqs = [self.chunk_store.get_chunk_request(chunk_id) for chunk_id in chunk_ids]
            register_body = json.dumps([c.as_dict() for c in chunk_reqs]).encode("utf-8")
        with Timer(f"pre-register chunks {chunk_ids} to {dst_host}"):
            response = self.http_pool.request(
                "POST",
                f"https://{dst_host}:8080/api/v1/chunk_requests",
                body=register_body,
                headers={"Content-Type": "application/json"},
            )
            assert response.status == 200 and json.loads(response.data.decode("utf-8")).get("status") == "ok"
            logger.debug(f"[sender:{self.worker_id}]:{chunk_ids} registered chunks")

        # contact server to set up socket connection
        if self.destination_ports.get(dst_host) is None:
            logger.debug(f"[sender:{self.worker_id}]:{chunk_ids} creating new socket")
            self.destination_sockets[dst_host] = retry_backoff(
                partial(self.make_socket, dst_host), max_retries=3, exception_class=socket.timeout
            )
            logger.debug(f"[sender:{self.worker_id}]:{chunk_ids} created new socket")
        sock = self.destination_sockets[dst_host]

        for idx, chunk_id in enumerate(chunk_ids):
            self.chunk_store.state_start_upload(chunk_id, f"sender:{self.worker_id}")
            chunk_req = self.chunk_store.get_chunk_request(chunk_id)
            chunk = chunk_req.chunk
            chunk_file_path = self.chunk_store.get_chunk_file_path(chunk_id)

            # read data from disk (and optionally compress if sending from source region)
            with open(chunk_file_path, "rb") as f:
                data = f.read()
            assert len(data) == chunk.chunk_length_bytes, f"chunk {chunk_id} has size {len(data)} but should be {chunk.chunk_length_bytes}"

            wire_length = len(data)
            compressed_length = None
            if self.use_compression and self.region == chunk_req.src_region:
                data = lz4.frame.compress(data)
                wire_length = len(data)
                compressed_length = wire_length
                logger.debug(
                    f"[sender:{self.worker_id}]:{chunk_ids} compressed {chunk_id} from {chunk.chunk_length_bytes} to {wire_length} ({100 * wire_length / chunk.chunk_length_bytes:.2f}%)"
                )
            if self.e2ee_secretbox is not None and self.region == chunk_req.src_region:
                data = self.e2ee_secretbox.encrypt(data)
                wire_length = len(data)

            # send chunk header
            header = chunk.to_wire_header(
                n_chunks_left_on_socket=len(chunk_ids) - idx - 1, wire_length=wire_length, is_compressed=(compressed_length is not None)
            )
            logger.debug(f"[sender:{self.worker_id}]:{chunk_id} sending chunk header")
            header.to_socket(sock)
            logger.debug(f"[sender:{self.worker_id}]:{chunk_id} sent chunk header")

            # send chunk data
            assert chunk_file_path.exists(), f"chunk file {chunk_file_path} does not exist"
            with Timer() as t:
                sock.sendall(data)

            logger.debug(f"[sender:{self.worker_id}]:{chunk_id} sent at {chunk.chunk_length_bytes * 8 / t.elapsed / MB:.2f}Mbps")
            self.chunk_store.state_finish_upload(chunk_id, f"sender:{self.worker_id}", compressed_size_bytes=compressed_length)
            chunk_file_path.unlink()
