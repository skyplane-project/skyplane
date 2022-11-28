import argparse
from pprint import pprint
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
from typing import Dict, List

from skyplane import MB
from skyplane.chunk import ChunkState
from skyplane.gateway.chunk_store import ChunkStore
from skyplane.gateway.gateway_daemon_api import GatewayDaemonAPI
from skyplane.utils import logger

from skyplane.gateway.gateway_queue import GatewayQueue, GatewayANDQueue

from skyplane.gateway.operators.gateway_operator import (
    GatewaySender,
    GatewayRandomDataGen,
    GatewayWriteLocal,
    GatewayObjStoreReadOperator,
    GatewayObjStoreWriteOperator,
    GatewayWaitReciever,
)
from skyplane.gateway.operators.gateway_receiver import GatewayReceiver

from queue import Empty
from collections import defaultdict

# TODO: add default partition ID to main
# create gateway broadcast


class GatewayDaemon:
    def __init__(
        self, region: str, chunk_dir: PathLike, max_incoming_ports=64, use_tls=True, use_e2ee=False,
    ):
        # read gateway program
        gateway_program_path = Path(os.environ["GATEWAY_PROGRAM_FILE"]).expanduser()
        gateway_program = json.load(open(gateway_program_path, "r"))

        self.use_tls = use_tls

        # todo max_incoming_ports should be configurable rather than static
        self.region = region
        self.max_incoming_ports = max_incoming_ports

        # the chunk store managest the incoming queue of chunks and outgoing queue of chunk status updates
        self.chunk_store = ChunkStore(chunk_dir)

        self.error_event = Event()
        self.error_queue = Queue()
        if use_e2ee:
            e2ee_key_path = Path(os.environ["E2EE_KEY_FILE"]).expanduser()
            with open(e2ee_key_path, "rb") as f:
                self.e2ee_key_bytes = f.read()
        else:
            self.e2ee_key_bytes = None

        # create gateway operators
        self.terminal_operators = defaultdict(list)  # track terminal operators per partition
        self.operators = self.create_gateway_operators(gateway_program["_plan"])

        # single gateway reciever
        self.gateway_receiver = GatewayReceiver(
            "reciever",
            region=region,
            chunk_store=self.chunk_store,
            error_event=self.error_event,
            error_queue=self.error_queue,
            max_pending_chunks=max_incoming_ports,
            use_tls=self.use_tls,
            use_compression=False,  # use_compression,
            e2ee_key_bytes=self.e2ee_key_bytes,
        )

        # API server
        self.api_server = GatewayDaemonAPI(
            self.chunk_store, self.gateway_receiver, self.error_event, self.error_queue, self.terminal_operators
        )
        self.api_server.start()
        atexit.register(self.api_server.shutdown)
        logger.info(f"[gateway_daemon] API started at {self.api_server.url}")

    def create_gateway_operators(self, gateway_program: Dict):
        """Create a gateway plan from a gateway program"""

        operators = {}

        def create_output_queue(operator: Dict):
            # create output data queue
            if len(operator["children"]) == 0:
                return None
            if operator["children"][0]["op_type"] == "mux_and":
                return GatewayANDQueue()
            return GatewayQueue()

        def get_child_operators(operator):
            if len(operator["children"]) == 0:
                return []
            if operator["children"][0]["op_type"] == "mux_and" or operator["children"][0]["op_type"] == "mux_or":
                return operator["children"][0]["children"]
            return operator["children"]

        def create_gateway_operators_helper(input_queue, program: List[Dict], partition_id: str):
            for op in program:

                handle = op["op_type"] + "_" + op["handle"]
                input_queue.register_handle(handle)

                # get child operators
                child_operators = get_child_operators(op)

                if op["op_type"] == "mux_or":
                    # parent must have been mux_and
                    assert isinstance(input_queue, GatewayANDQueue), f"Parent must have been mux_and {handle}, instead was {input_queue}"

                    input_queue = input_queue.get_handle_queue(handle)
                    # recurse to children with single queue
                    create_gateway_operators_helper(input_queue, child_operators, partition_id)
                    continue

                # create output data queue
                output_queue = create_output_queue(op)
                if output_queue is None:
                    # track what opeartors need to complete processing the chunk
                    self.terminal_operators[partition_id].append(op["handle"])

                # create operators
                if op["op_type"] == "receive":
                    # wait for chunks from reciever
                    operators[handle] = GatewayWaitReciever(
                        handle=handle,
                        region=self.region,
                        input_queue=input_queue,
                        output_queue=output_queue,
                        n_processes=1,
                        chunk_store=self.chunk_store,
                        error_event=self.error_event,
                        error_queue=self.error_queue,
                    )
                elif op["op_type"] == "read_object_store":
                    operators[handle] = GatewayObjStoreReadOperator(
                        handle=handle,
                        region=self.region,
                        input_queue=input_queue,
                        output_queue=output_queue,
                        error_queue=self.error_queue,
                        error_event=self.error_event,
                        n_processes=op["num_connections"],
                        chunk_store=self.chunk_store,
                        bucket_name=op["bucket_name"],
                        bucket_region=op["bucket_region"],
                    )
                elif op["op_type"] == "gen_data":
                    operators[handle] = GatewayRandomDataGen(
                        handle=handle,
                        region=self.region,
                        input_queue=input_queue,
                        output_queue=output_queue,
                        error_queue=self.error_queue,
                        error_event=self.error_event,
                        chunk_store=self.chunk_store,
                        size_mb=op["size_mb"],
                    )
                elif op["op_type"] == "send":
                    operators[handle] = GatewaySender(
                        handle,
                        region=self.region,
                        ip_addr=op["ip_address"],
                        input_queue=input_queue,
                        output_queue=output_queue,
                        error_event=self.error_event,
                        error_queue=self.error_queue,
                        chunk_store=self.chunk_store,
                        use_tls=self.use_tls,
                        use_compression=False,  # operator["compress"],
                        e2ee_key_bytes=self.e2ee_key_bytes,
                        n_processes=32,  # op["num_connections"],
                    )
                elif op["op_type"] == "write_object_store":
                    operators[handle] = GatewayObjStoreWriteOperator(
                        handle=handle,
                        region=self.region,
                        input_queue=input_queue,
                        output_queue=output_queue,
                        error_queue=self.error_queue,
                        error_event=self.error_event,
                        n_processes=op["num_connections"],
                        chunk_store=self.chunk_store,
                        bucket_name=op["bucket_name"],
                        bucket_region=op["bucket_region"],
                    )
                elif op["op_type"] == "write_local":
                    operators[handle] = GatewayWriteLocal(
                        handle=handle,
                        region=self.region,
                        input_queue=input_queue,
                        output_queue=output_queue,
                        error_queue=self.error_queue,
                        error_event=self.error_event,
                        chunk_store=self.chunk_store,
                    )
                else:
                    raise ValueError(f"Unsupported op_type {op['op_type']}")
                # recursively create for child operators
                create_gateway_operators_helper(output_queue, child_operators, partition_id)

        pprint(gateway_program)

        # create operator tree for each partition
        for partition, program in gateway_program.items():
            partition = str(partition)

            # create initial queue for partition
            self.chunk_store.add_partition(partition)

            create_gateway_operators_helper(
                self.chunk_store.chunk_requests[partition],  # incoming chunk requests for partition
                program,  # single partition program
                partition,
            )
        return operators

    def run(self):
        exit_flag = Event()

        def exit_handler(signum, frame):
            logger.warning("[gateway_daemon] Received signal {}. Exiting...".format(signum))
            exit_flag.set()
            for operator in self.operators.values():
                operator.stop_workers()
            sys.exit(0)

        for operator in self.operators.values():
            logger.info(f"[gateway_daemon] Starting gateway operator {operator.handle} workers")
            operator.start_workers()

        signal.signal(signal.SIGINT, exit_handler)
        signal.signal(signal.SIGTERM, exit_handler)

        logger.info("[gateway_daemon] Starting daemon loop")
        try:
            while not exit_flag.is_set() and not self.error_event.is_set():
                self.api_server.pull_chunk_status_queue()
        except Exception as e:
            self.error_queue.put(e)
            self.error_event.set()
            logger.error(f"[gateway_daemon] Exception in daemon loop: {e}")
            logger.exception(e)

        # shut down workers except for API to report status
        logger.info("[gateway_daemon] Exiting all workers except for API")
        for operator in self.operators.values():
            operator.stop_workers()
        logger.info("[gateway_daemon] Done")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Skyplane Gateway Daemon")
    parser.add_argument("--region", type=str, required=True, help="Region tag (provider:region")
    parser.add_argument("--chunk-dir", type=Path, default="/tmp/skyplane/chunks", help="Directory to store chunks")
    parser.add_argument("--disable-tls", action="store_true")
    parser.add_argument("--use-compression", action="store_true")  # TODO: remove
    parser.add_argument("--disable-e2ee", action="store_true")  # TODO: remove
    args = parser.parse_args()

    os.makedirs(args.chunk_dir)
    daemon = GatewayDaemon(region=args.region, chunk_dir=args.chunk_dir, use_tls=not args.disable_tls,)
    daemon.run()
