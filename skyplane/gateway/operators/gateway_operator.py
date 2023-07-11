import json
from pathlib import Path
import os
from typing import List
import queue
import socket
import ssl
import time
import lz4.frame
import traceback
from functools import partial
from multiprocessing import Event, Process, Queue
from multiprocessing.managers import DictProxy
from typing import Dict, List, Optional

import urllib3
import nacl.secret
from abc import ABC, abstractmethod

from skyplane.config_paths import cloud_config
from skyplane.utils.definitions import MB
from skyplane.utils import logger
from skyplane.utils.retry import retry_backoff
from skyplane.utils.timer import Timer
from skyplane.obj_store.object_store_interface import ObjectStoreInterface

from skyplane.chunk import ChunkRequest, ChunkState
from skyplane.gateway.gateway_queue import GatewayQueue
from skyplane.gateway.chunk_store import ChunkStore


class GatewayOperator(ABC):
    def __init__(
        self,
        handle: str,
        region: str,  # TODO: remove
        input_queue: GatewayQueue,
        output_queue: GatewayQueue,
        error_event,
        error_queue: Queue,
        chunk_store: ChunkStore,
        n_processes: Optional[int] = 1,
    ):
        self.handle = handle
        self.region = region
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.chunk_store = chunk_store
        self.error_event = error_event
        self.error_queue = error_queue
        self.n_processes = n_processes

        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        # args for worker function
        self.args = ()

        # shared state
        # TODO: move this outside of GateWaySender to share between processes?
        self.processes = []
        self.exit_flags = [Event() for _ in range(self.n_processes)]

        # process-local state
        self.worker_id: Optional[int] = None

    def start_workers(self):
        for i in range(self.n_processes):
            p = Process(target=self.worker_loop, args=(i,) + self.args)
            p.start()
            self.processes.append(p)

    def stop_workers(self):
        for i in range(self.n_processes):
            self.exit_flags[i].set()
        for p in self.processes:
            p.join()
        self.processes = []

    def worker_loop(self, worker_id: int, *args):
        self.worker_id = worker_id
        while not self.exit_flags[worker_id].is_set() and not self.error_event.is_set():
            try:
                # get chunk from input queue
                try:
                    # will only get data for that handle
                    chunk_req = self.input_queue.get_nowait(self.handle)
                except queue.Empty:
                    continue

                # TODO: status logging
                self.chunk_store.log_chunk_state(chunk_req, ChunkState.in_progress, operator_handle=self.handle, worker_id=worker_id)
                # process chunk
                succ = self.process(chunk_req, *args)

                # place in output queue
                if succ:
                    self.chunk_store.log_chunk_state(chunk_req, ChunkState.complete, operator_handle=self.handle, worker_id=worker_id)
                    if self.output_queue is not None:
                        self.output_queue.put(chunk_req)
                    else:
                        print(f"[{self.handle}:{self.worker_id}] Output queue is None - terminal operator")
                    time.sleep(0.1)  # yield ?
                else:
                    # failed to process - re-queue
                    time.sleep(0.1)
                    self.input_queue.put(chunk_req)

            except Exception as e:
                logger.error(f"[{self.handle}:{self.worker_id}] Exception: {e}")
                self.error_queue.put(traceback.format_exc())
                self.error_event.set()
                self.exit_flags[worker_id].set()

        # run worker exit function
        self.worker_exit(worker_id)

    def worker_exit(self, worker_id: int):
        pass

    @abstractmethod
    def process(self, chunk_req: ChunkRequest, **args):
        pass


class GatewayWaitReceiver(GatewayOperator):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    # TODO: alternative (potentially better performnace) implementation: connect via queue with GatewayReceiver to listen
    # for download completition events - join with chunk request queue from ChunkStore
    def process(self, chunk_req: ChunkRequest):
        chunk_file_path = self.chunk_store.get_chunk_file_path(chunk_req.chunk.chunk_id)
        if not os.path.exists(chunk_file_path):  # chunk still not downloaded, re-queue
            # logger.debug(f"[{self.handle}:{self.worker_id}] Chunk {chunk_req.chunk.chunk_id} not downloaded yet, re-queueing")
            return False

        # check to see if file is completed downloading
        # Successfully received chunk 38400a29812142a486eaefcdebedf371, 161867776    0, 67108864
        with open(chunk_file_path, "rb") as f:
            data = f.read()
            if len(data) < chunk_req.chunk.chunk_length_bytes:
                # download not complete
                return False
            assert (
                len(data) == chunk_req.chunk.chunk_length_bytes
            ), f"Downloaded chunk length does not match expected length: {len(data)}, {chunk_req.chunk.chunk_length_bytes}"
        print(
            f"[{self.handle}:{self.worker_id}] Successfully received chunk {chunk_req.chunk.chunk_id}, {len(data)}, {chunk_req.chunk.chunk_length_bytes}"
        )
        return True


class GatewaySender(GatewayOperator):
    def __init__(
        self,
        handle: str,
        region: str,
        input_queue: GatewayQueue,
        output_queue: GatewayQueue,
        error_event,
        error_queue: Queue,
        chunk_store: ChunkStore,
        ip_addr: str,
        use_tls: Optional[bool] = True,
        use_compression: Optional[bool] = True,
        e2ee_key_bytes: Optional[bytes] = None,
        n_processes: Optional[int] = 32,
    ):
        super().__init__(handle, region, input_queue, output_queue, error_event, error_queue, chunk_store, n_processes)
        self.ip_addr = ip_addr
        self.use_tls = use_tls
        self.use_compression = use_compression
        self.e2ee_key_bytes = e2ee_key_bytes
        self.args = (ip_addr,)

        # provider = region.split(":")[0]
        # if provider == "aws" or provider == "gcp":
        #    self.n_processes = 32
        # elif provider == "azure":
        #    self.n_processes = 24  # due to throttling limits from authentication

        # encryption
        if e2ee_key_bytes is None:
            self.e2ee_secretbox = None
        else:
            self.e2ee_secretbox = nacl.secret.SecretBox(e2ee_key_bytes)

        # SSL context
        if use_tls:
            self.ssl_context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
            self.ssl_context.check_hostname = False
            self.ssl_context.verify_mode = ssl.CERT_NONE
            logger.info(f"Using {str(ssl.OPENSSL_VERSION)}")
        else:
            self.ssl_context = None

        # process-local state
        self.sender_port: Optional[int] = None
        self.destination_ports: Dict[str, int] = {}  # ip_address -> int
        self.destination_sockets: Dict[str, socket.socket] = {}  # ip_address -> socket
        self.sent_chunk_ids: Dict[str, List[int]] = {}  # ip_address -> list of chunk_ids

        # http pool
        timeout = urllib3.util.Timeout(connect=10.0, read=None)  # no read timeout
        self.http_pool = urllib3.PoolManager(retries=urllib3.Retry(total=3), cert_reqs="CERT_NONE", timeout=timeout)

    def worker_exit(self, worker_id: int):
        # close destination sockets
        logger.info(f"[sender:{worker_id}] exiting, closing sockets")
        for dst_socket in self.destination_sockets.values():
            dst_socket.close()

        # wait for all chunks to reach state "downloaded"
        # TODO: remove/replace for broadcast
        def wait_for_chunks():
            cr_status = {}
            for ip, ip_chunk_ids in self.sent_chunk_ids.items():
                response = self.http_pool.request("GET", f"https://{ip}:8080/api/v1/incomplete_chunk_requests")
                assert response.status == 200, f"{response.status} {response.data}"
                host_state = json.loads(response.data.decode("utf-8"))["chunk_requests"]
                for chunk_id in ip_chunk_ids:
                    if chunk_id in host_state:
                        cr_status[chunk_id] = host_state[chunk_id]["state"]
            return all(status not in ["registered", "download_queued", "download_in_progress"] for status in cr_status.values())

        logger.info(f"[sender:{worker_id}] waiting for chunks to reach state 'downloaded'")
        wait_success = False
        for _ in range(60):
            try:
                if wait_for_chunks():
                    wait_success = True
                    break
            except Exception as e:
                logger.error(f"[Gateway Sender wait_for_chunks()] Exception: {e}")

            time.sleep(5)  # originally 1
        if not wait_success:
            raise Exception("Timed out waiting for chunks to reach state 'downloaded'")
        logger.info(f"[sender:{worker_id}] all chunks reached state 'downloaded'")

        # close servers
        logger.info(f"[sender:{worker_id}] exiting, closing servers")
        for dst_host, dst_port in self.destination_ports.items():
            response = self.http_pool.request("DELETE", f"https://{dst_host}:8080/api/v1/servers/{dst_port}")
            assert response.status == 200 and json.loads(response.data.decode("utf-8")) == {"status": "ok"}
            logger.info(f"[sender:{worker_id}] closed destination socket {dst_host}:{dst_port}")

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
    def process(self, chunk_req: ChunkRequest, dst_host: str):
        """Send list of chunks to gateway server, pipelining small chunks together into a single socket stream."""
        # notify server of upcoming ChunkRequests

        # print(f"[{self.handle}:{self.worker_id}] Sending chunk ID {chunk_req.chunk.chunk_id} to IP {dst_host}")

        # TODO: does this function need to be implemented to work for a list of chunks?

        chunk_ids = [chunk_req.chunk.chunk_id]
        chunk_reqs = [chunk_req]
        try:
            with Timer(f"pre-register chunks {chunk_ids} to {dst_host}"):
                # TODO: remove chunk request wrapper
                # while True:
                #   try:
                #       response = self.http_pool.request(
                #           "POST", f"https://{dst_host}:8080/api/v1/chunk_requests", body=register_body, headers={"Content-Type": "application/json"}
                #       )
                #       break
                #   except Exception as e:
                #       print("sender post error", e)
                #       time.sleep(1)
                n_added = 0
                while n_added < len(chunk_reqs):
                    register_body = json.dumps([c.chunk.as_dict() for c in chunk_reqs[n_added:]]).encode("utf-8")
                    # print(f"[sender-{self.worker_id}]:{chunk_ids} register body {register_body}")
                    response = self.http_pool.request(
                        "POST",
                        f"https://{dst_host}:8080/api/v1/chunk_requests",
                        body=register_body,
                        headers={"Content-Type": "application/json"},
                    )
                    reply_json = json.loads(response.data.decode("utf-8"))
                    print(f"[sender-{self.worker_id}]", n_added, reply_json, dst_host)
                    n_added += reply_json["n_added"]
                    assert response.status == 200, f"Wrong response status {response.status}"
                    # json.loads(response.data.decode("utf-8")).get("status") == "ok"
                    if n_added == len(chunk_reqs):
                        print(f"[sender-{self.worker_id}]:{chunk_ids} registered chunks")
                    else:
                        time.sleep(1)
        except Exception as e:
            print(f"[{self.handle}:{self.worker_id}] Error registering chunks {chunk_ids} to {dst_host}: {e}")
            raise e

        # contact server to set up socket connection
        if self.destination_ports.get(dst_host) is None:
            print(f"[sender-{self.worker_id}]:{chunk_ids} creating new socket")
            self.destination_sockets[dst_host] = retry_backoff(
                partial(self.make_socket, dst_host), max_retries=3, exception_class=socket.timeout
            )
            print(f"[sender-{self.worker_id}]:{chunk_ids} created new socket")
        sock = self.destination_sockets[dst_host]

        # TODO: cleanup so this isn't a loop
        for idx, chunk_req in enumerate(chunk_reqs):
            # self.chunk_store.state_start_upload(chunk_id, f"sender:{self.worker_id}")
            chunk_id = chunk_req.chunk.chunk_id
            chunk = chunk_req.chunk
            chunk_file_path = self.chunk_store.get_chunk_file_path(chunk_id)

            # read data from disk (and optionally compress if sending from source region)
            with open(chunk_file_path, "rb") as f:
                data = f.read()
            assert len(data) == chunk.chunk_length_bytes, f"chunk {chunk_id} has size {len(data)} but should be {chunk.chunk_length_bytes}"

            wire_length = len(data)
            raw_wire_length = wire_length
            compressed_length = None

            if self.use_compression:
                data = lz4.frame.compress(data)
                wire_length = len(data)
                compressed_length = wire_length
            if self.e2ee_secretbox is not None:
                data = self.e2ee_secretbox.encrypt(data)
                wire_length = len(data)

            # send chunk header
            header = chunk.to_wire_header(
                n_chunks_left_on_socket=len(chunk_ids) - idx - 1,
                wire_length=wire_length,
                raw_wire_length=raw_wire_length,
                is_compressed=(compressed_length is not None),
            )
            # print(f"[sender-{self.worker_id}]:{chunk_id} sending chunk header {header}")
            header.to_socket(sock)
            # print(f"[sender-{self.worker_id}]:{chunk_id} sent chunk header")

            # send chunk data
            assert chunk_file_path.exists(), f"chunk file {chunk_file_path} does not exist"
            # file_size = os.path.getsize(chunk_file_path)

            with Timer() as t:
                sock.sendall(data)

            # logger.debug(f"[sender:{self.worker_id}]:{chunk_id} sent at {chunk.chunk_length_bytes * 8 / t.elapsed / MB:.2f}Mbps")
            print(f"[sender:{self.worker_id}]:{chunk_id} sent at {wire_length * 8 / t.elapsed / MB:.2f}Mbps")

            if dst_host not in self.sent_chunk_ids:
                self.sent_chunk_ids[dst_host] = []
            self.sent_chunk_ids[dst_host].append(chunk_req.chunk.chunk_id)

        # success, so return true
        return True


class GatewayRandomDataGen(GatewayOperator):
    def __init__(
        self,
        handle: str,
        region: str,
        input_queue: GatewayQueue,
        output_queue: GatewayQueue,
        error_event,
        error_queue: Queue,
        chunk_store: ChunkStore,
        size_mb: int,
        n_processes: Optional[int] = 1,
    ):
        super().__init__(handle, region, input_queue, output_queue, error_event, error_queue, chunk_store, n_processes)
        self.size_mb = size_mb

    def process(self, chunk_req: ChunkRequest):
        # wait until enough space available
        fpath = str(self.chunk_store.get_chunk_file_path(chunk_req.chunk.chunk_id).absolute())
        size_bytes = int(self.size_mb * MB)
        assert size_bytes > 0, f"Invalid size {size_bytes} for fallocate"

        while True:
            # create file with random data
            try:
                os.system(f"fallocate -l {size_bytes} {fpath}")
                file_size = os.path.getsize(fpath)
                if file_size == size_bytes:
                    break
            except Exception:
                print(f"[gen_data] Chunk store full, waiting before generating {chunk_req.chunk.chunk_id}")
                time.sleep(0.1)
                continue

        logger.info(f"[{self.handle}:{self.worker_id}] Wrote chunk {chunk_req.chunk.chunk_id} with size {file_size} to {fpath}")
        chunk_req.chunk.chunk_length_bytes = os.path.getsize(fpath)

        return True


class GatewayWriteLocal(GatewayOperator):
    def __init__(
        self,
        handle: str,
        region: str,
        input_queue: GatewayQueue,
        output_queue: GatewayQueue,
        error_event,
        error_queue: Queue,
        chunk_store: ChunkStore,
        n_processes: int = 1,
    ):
        super().__init__(handle, region, input_queue, output_queue, error_event, error_queue, chunk_store, n_processes)

    def process(self, chunk_req: ChunkRequest):
        # do nothing (already written locally)
        return True


class GatewayObjStoreOperator(GatewayOperator):
    def __init__(
        self,
        handle: str,
        region: str,
        bucket_name: str,
        bucket_region: str,
        input_queue: GatewayQueue,
        output_queue: GatewayQueue,
        error_event,
        error_queue: Queue,
        n_processes: Optional[int] = 1,
        chunk_store: Optional[ChunkStore] = None,
    ):
        super().__init__(handle, region, input_queue, output_queue, error_event, error_queue, chunk_store, n_processes)
        self.bucket_name = bucket_name
        self.bucket_region = bucket_region
        self.src_requester_pays = cloud_config.get_flag("requester_pays")

        # process-local state
        self.worker_id: Optional[int] = None
        self.obj_store_interfaces: Dict[str, ObjectStoreInterface] = {}

    # interact with object store
    def get_obj_store_interface(self, region: str, bucket: str) -> ObjectStoreInterface:
        key = f"{region}:{bucket}"
        if key not in self.obj_store_interfaces:
            logger.warning(f"[gateway_daemon] ObjectStoreInterface not cached for {key}")
            try:
                self.obj_store_interfaces[key] = ObjectStoreInterface.create(region, bucket)
            except Exception as e:
                raise ValueError(f"Failed to create obj store interface {str(e)}")
        return self.obj_store_interfaces[key]


class GatewayObjStoreReadOperator(GatewayObjStoreOperator):
    def __init__(
        self,
        handle: str,
        region: str,
        bucket_name: str,
        bucket_region: str,
        input_queue: GatewayQueue,
        output_queue: GatewayQueue,
        error_event,
        error_queue: Queue,
        n_processes: int = 32,
        chunk_store: Optional[ChunkStore] = None,
    ):
        super().__init__(
            handle, region, bucket_name, bucket_region, input_queue, output_queue, error_event, error_queue, n_processes, chunk_store
        )

    def process(self, chunk_req: ChunkRequest, **args):
        fpath = str(self.chunk_store.get_chunk_file_path(chunk_req.chunk.chunk_id).absolute())
        # wait for free space
        logger.debug(f"[{self.handle}:{self.worker_id}] Start download {chunk_req.chunk.chunk_id} from {self.bucket_name}")

        obj_store_interface = self.get_obj_store_interface(self.bucket_region, self.bucket_name)

        if self.src_requester_pays:
            obj_store_interface.set_requester_bool(True)

        # while self.chunk_store.remaining_bytes() < chunk_req.chunk.chunk_length_bytes * self.n_processes:
        #    time.sleep(0.1)

        # assert chunk_req.chunk.chunk_length_bytes > 0, f"Cannot have size 0 chunk {chunk_req.chunk}" # actually ok

        if chunk_req.chunk.chunk_length_bytes == 0:
            # nothing to do
            # create empty file
            Path(fpath).touch()
            return True

        while True:
            # if self.chunk_store.remaining_bytes() < chunk_req.chunk.chunk_length_bytes * self.n_processes:
            #    time.sleep(0.1)
            #    continue
            try:
                md5sum = retry_backoff(
                    partial(
                        obj_store_interface.download_object,
                        chunk_req.chunk.src_key,
                        fpath,
                        chunk_req.chunk.file_offset_bytes,
                        chunk_req.chunk.chunk_length_bytes,
                        generate_md5=True,
                    ),
                    max_retries=1,  # TODO: fix this - not a good solution
                )

                # ensure properly downloaded
                file_size = os.path.getsize(fpath)
                if file_size == chunk_req.chunk.chunk_length_bytes:
                    break

            except Exception as e:
                logger.error(f"[obj_store:{self.worker_id}] Error reading key {chunk_req.chunk.src_key}: {str(e)}")
                print(f"[obj_store:{self.worker_id}] Error reading key {chunk_req.chunk.src_key}: {str(e)}")
                time.sleep(1)

        # update md5sum for chunk requests
        # TODO: create checksum operator
        # if not md5sum:
        #    logger.error(f"[obj_store:{self.worker_id}] Checksum was not generated for {chunk_req.chunk.src_key}")
        # else:
        #    self.chunk_store.update_chunk_checksum(chunk_req.chunk.chunk_id, md5sum)

        received_chunk_size = self.chunk_store.get_chunk_file_path(chunk_req.chunk.chunk_id).stat().st_size
        assert (
            received_chunk_size == chunk_req.chunk.chunk_length_bytes
        ), f"Downloaded chunk {chunk_req.chunk.chunk_id} to {fpath} has incorrect size (expected {chunk_req.chunk.chunk_length_bytes} but got {received_chunk_size}, {chunk_req.chunk.chunk_length_bytes})"
        logger.debug(f"[obj_store:{self.worker_id}] Downloaded {chunk_req.chunk.chunk_id} from {self.bucket_name}")
        return True


class GatewayObjStoreWriteOperator(GatewayObjStoreOperator):
    def __init__(
        self,
        handle: str,
        region: str,
        bucket_name: str,
        bucket_region: str,
        input_queue: GatewayQueue,
        output_queue: GatewayQueue,
        error_event,
        error_queue: Queue,
        upload_id_map: DictProxy,  # map of upload_id mappings from client
        n_processes: Optional[int] = 32,
        chunk_store: Optional[ChunkStore] = None,
        prefix: Optional[str] = "",
    ):
        super().__init__(
            handle, region, bucket_name, bucket_region, input_queue, output_queue, error_event, error_queue, n_processes, chunk_store
        )
        self.chunk_store = chunk_store
        self.upload_id_map = upload_id_map
        self.prefix = prefix

    def process(self, chunk_req: ChunkRequest):
        fpath = str(self.chunk_store.get_chunk_file_path(chunk_req.chunk.chunk_id).absolute())
        logger.debug(
            f"[{self.handle}:{self.worker_id}] Start upload {chunk_req.chunk.chunk_id} to {self.bucket_name}, key {chunk_req.chunk.dest_key}"
        )

        # TODO: cache object store interface
        obj_store_interface = self.get_obj_store_interface(self.bucket_region, self.bucket_name)

        if chunk_req.chunk.multi_part:
            upload_id_key = self.bucket_region + chunk_req.chunk.src_key  # format for upload id key
            assert upload_id_key in self.upload_id_map, f"Upload id for {upload_id_key} not found {self.upload_id_map}"
            upload_id = self.upload_id_map[upload_id_key]
        else:
            upload_id = None

        key = self.prefix + chunk_req.chunk.dest_key
        logger.debug(f"[obj_store:{self.worker_id}] Uploading to id {upload_id} for key {key} bucket {self.bucket_name}")
        retry_backoff(
            partial(
                obj_store_interface.upload_object,
                fpath,
                key,
                chunk_req.chunk.part_number,
                upload_id,
                check_md5=chunk_req.chunk.md5_hash,
            ),
            max_retries=1,
        )
        logger.debug(
            f"[obj_store:{self.worker_id}] Uploaded {chunk_req.chunk.chunk_id} partition {chunk_req.chunk.part_number} to {self.bucket_name}"
        )
        return True
