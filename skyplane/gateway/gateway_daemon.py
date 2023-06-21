import argparse
from multiprocessing import Manager
from pprint import pprint
import atexit
import json
import os
import signal
import sys
from multiprocessing import Event, Queue
from os import PathLike
from pathlib import Path
from typing import Dict, List, Optional

from skyplane.utils import logger

from skyplane.gateway.gateway_queue import GatewayQueue, GatewayANDQueue
from skyplane.gateway.chunk_store import ChunkStore
from skyplane.gateway.gateway_daemon_api import GatewayDaemonAPI
from skyplane.gateway.operators.gateway_operator import (
    GatewaySender,
    GatewayRandomDataGen,
    GatewayWriteLocal,
    GatewayObjStoreReadOperator,
    GatewayObjStoreWriteOperator,
    GatewayWaitReceiver,
)
from skyplane.gateway.operators.gateway_receiver import GatewayReceiver
from skyplane.utils import logger
from collections import defaultdict


# TODO: add default partition ID to main
# create gateway br
class GatewayDaemon:
    def __init__(
        self,
        region: str,
        chunk_dir: PathLike,
        max_incoming_ports=64,
        use_tls=True,
        use_e2ee=True,  # TODO: read from operator field
        use_compression=True,  # TODO: read from operator field
    ):
        # read gateway program
        gateway_program_path = Path(os.environ["GATEWAY_PROGRAM_FILE"]).expanduser()
        gateway_program = json.load(open(gateway_program_path, "r"))

        self.upload_id_map = Manager().dict()

        pprint(gateway_program)

        # read gateway info
        gateway_info_path = Path(os.environ["GATEWAY_INFO_FILE"]).expanduser()
        self.gateway_info = json.load(open(gateway_info_path, "r"))

        print("starting gateway daemon", gateway_program_path)
        pprint(gateway_program)
        assert len(gateway_program) > 0, f"Cannot have empty gateway program {gateway_program}"

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
            print("Server side E2EE key loaded: ", self.e2ee_key_bytes)
        else:
            self.e2ee_key_bytes = None

        # create gateway operators
        self.terminal_operators = defaultdict(list)  # track terminal operators per partition
        self.num_required_terminal = {}
        self.operators = self.create_gateway_operators(gateway_program)

        # single gateway receiver
        self.gateway_receiver = GatewayReceiver(
            "reciever",
            region=region,
            chunk_store=self.chunk_store,
            error_event=self.error_event,
            error_queue=self.error_queue,
            max_pending_chunks=max_incoming_ports,
            use_tls=self.use_tls,
            use_compression=use_compression,
            e2ee_key_bytes=self.e2ee_key_bytes,
        )

        # API server
        self.api_server = GatewayDaemonAPI(
            self.chunk_store,
            self.gateway_receiver,
            self.error_event,
            self.error_queue,
            terminal_operators=self.terminal_operators,
            num_required_terminal=self.num_required_terminal,
            upload_id_map=self.upload_id_map,
        )
        self.api_server.start()
        atexit.register(self.api_server.shutdown)
        logger.info(f"[gateway_daemon] API started at {self.api_server.url}")

    def print_operator_graph(self):
        def print_operator_graph_helper(partition, queue: GatewayQueue, prefix: str):
            if queue is None:
                return
            print(f"{prefix} {partition}: Input queue:", queue, "handles:", queue.get_handles())
            for handle in queue.get_handles():
                print(f"{prefix} {partition}: Operator {handle}")
                # TODO: causes error sometimes for mux_or
                next_queue = self.operators[handle].output_queue
                print_operator_graph_helper(partition, next_queue, prefix + "--")

        for partition, queue in self.chunk_store.chunk_requests.items():
            print(f"Partition {partition}, {queue}")
            print_operator_graph_helper(partition, queue, "")

    def create_gateway_operators(self, gateway_program: Dict):
        """Create a gateway plan from a gateway program"""

        operators = {}

        def create_output_queue(operator: Dict):
            # create output data queue
            if len(operator["children"]) == 0:
                return None
            print("get output queue", operator["op_type"], operator["children"][0]["op_type"])
            if operator["children"][0]["op_type"] == "mux_and":
                return GatewayANDQueue()
            return GatewayQueue()

        def get_child_operators(operator):
            if len(operator["children"]) == 0:
                return []
            if operator["children"][0]["op_type"] == "mux_and" or operator["children"][0]["op_type"] == "mux_or":
                return operator["children"][0]["children"]
            return operator["children"]

        def create_gateway_operators_helper(input_queue, program: List[Dict], partition_ids: List[str]):
            total_p = 0
            for op in program:
                handle = op["op_type"] + "_" + op["handle"]
                print(f"Input queue {input_queue}, adding handle {handle} (current handles {input_queue.get_handles()}")
                input_queue.register_handle(handle)

                # get child operators
                child_operators = get_child_operators(op)

                if op["op_type"] == "mux_or":
                    # parent must have been mux_and
                    assert isinstance(
                        input_queue, GatewayANDQueue
                    ), f"Parent must have been mux_and {handle}, instead was {input_queue} {gateway_program}"

                    # recurse to children with single queue
                    total_p += create_gateway_operators_helper(input_queue.get_handle_queue(handle), child_operators, partition_ids)
                    continue

                # create output data queue
                output_queue = create_output_queue(op)
                if isinstance(output_queue, GatewayANDQueue):
                    # update number of operations that must be completed
                    for partition in partition_ids:
                        self.num_required_terminal[str(partition)] = self.num_required_terminal[str(partition)] - 1 + len(child_operators)

                print(f"Input queue {input_queue} handle {handle}: created output queue {output_queue}")
                print(f"Input queue {input_queue} handle {handle}: has children {child_operators}")
                if output_queue is None:
                    # track what opeartors need to complete processing the chunk
                    for partition in partition_ids:
                        self.terminal_operators[str(partition)].append(handle)

                # create operators
                if op["op_type"] == "receive":
                    # wait for chunks from receiver
                    operators[handle] = GatewayWaitReceiver(
                        handle=handle,
                        region=self.region,
                        input_queue=input_queue,
                        output_queue=output_queue,
                        n_processes=1,  # dummy wait thread, not actual receiver
                        chunk_store=self.chunk_store,
                        error_event=self.error_event,
                        error_queue=self.error_queue,
                    )
                    total_p += 1
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
                    total_p += op["num_connections"]
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
                    # TODO: handle private ips for GCP->GCP
                    target_gateway_info = self.gateway_info[op["target_gateway_id"]]
                    ip_addr = target_gateway_info["private_ip_address"] if op["private_ip"] else target_gateway_info["public_ip_address"]
                    print("Gateway sender sending to ", ip_addr, "private", op["private_ip"])
                    operators[handle] = GatewaySender(
                        handle,
                        region=self.region,
                        ip_addr=ip_addr,
                        input_queue=input_queue,
                        output_queue=output_queue,
                        error_event=self.error_event,
                        error_queue=self.error_queue,
                        chunk_store=self.chunk_store,
                        use_tls=self.use_tls,
                        use_compression=op["compress"],
                        e2ee_key_bytes=self.e2ee_key_bytes,
                        n_processes=op["num_connections"],
                    )
                    total_p += op["num_connections"]
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
                        upload_id_map=self.upload_id_map,
                        prefix=op["key_prefix"],
                    )
                    total_p += op["num_connections"]
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
                    total_p += 1
                else:
                    raise ValueError(f"Unsupported op_type {op['op_type']}")
                # recursively create for child operators
                total_p += create_gateway_operators_helper(output_queue, child_operators, partition_ids)
            return total_p

        pprint(gateway_program)

        # create operator tree for each partition
        total_p = 0
        for program_group in gateway_program:
            partitions = program_group["partitions"]
            program = program_group["value"]

            print("partitions", partitions)
            # create initial queue for partition
            if program[0]["op_type"] == "mux_and":
                queue = GatewayANDQueue()
                print(f"First operator is mux_and: queue {queue}")
                assert len(program) == 1, f"mux_and cannot have siblings"
                program = program[0]["children"]
                for partition in partitions:
                    self.num_required_terminal[str(partition)] = len(program)
            else:
                queue = GatewayQueue()
                print(f"First operator is ", program[0], "queue", queue)

                for partition in partitions:
                    self.num_required_terminal[str(partition)] = 1

            # link all partitions to same queue reference
            for partition in partitions:
                self.chunk_store.add_partition(str(partition), queue)

            # create DAG for this partition group
            total_p += create_gateway_operators_helper(
                self.chunk_store.chunk_requests[str(partition)],  # incoming chunk requests for partition
                program,  # single partition program
                partitions,
            )
        # print("TOTAL NUMBER OF PROCESSES", total_p)
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
    daemon = GatewayDaemon(
        region=args.region,
        chunk_dir=args.chunk_dir,
        use_tls=not args.disable_tls,
    )
    daemon.run()
