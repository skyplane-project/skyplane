import logging
from collections import defaultdict
import logging.handlers
import os
import threading
from multiprocessing import Queue
from queue import Empty
from traceback import TracebackException
from typing import Dict, List, Tuple, Optional

from flask import Flask, jsonify, request
from werkzeug.serving import make_server

from skyplane.chunk import ChunkRequest, ChunkState
from skyplane.broadcast.gateway.chunk_store import ChunkStore
from skyplane.broadcast.gateway.operators.gateway_receiver import GatewayReceiver
from skyplane.utils import logger


class GatewayDaemonAPI(threading.Thread):
    """
    API documentation:
    * GET /api/v1/status - returns status of API
    * GET /api/v1/servers - returns list of running servers
    * POST /api/v1/servers - starts a new server
    * DELETE /api/v1/servers/<int:port> - stops a server
    * GET /api/v1/chunk_requests - returns list of chunk requests (use {'state': '<state>'} to filter)
    * GET /api/v1/chunk_requests/<int:chunk_id> - returns chunk request
    * POST /api/v1/chunk_requests - adds a new chunk request
    * PUT /api/v1/chunk_requests/<int:chunk_id> - updates chunk request
    * GET /api/v1/chunk_status_log - returns list of chunk status log entries
    """

    def __init__(
        self,
        chunk_store: ChunkStore,
        gateway_receiver: GatewayReceiver,
        error_event,
        error_queue: Queue,
        terminal_operators: Optional[Dict[str, List[str]]] = None,
        host="0.0.0.0",
        port=8081,
    ):
        super().__init__()
        self.app = Flask("gateway_metadata_server")
        self.chunk_store = chunk_store
        self.gateway_receiver = gateway_receiver
        self.error_event = error_event
        self.error_queue = error_queue
        self.terminal_operators = terminal_operators
        self.error_list: List[TracebackException] = []
        self.error_list_lock = threading.Lock()

        # load routes
        self.register_global_routes(self.app)
        self.register_server_routes(self.app)
        self.register_request_routes(self.app)
        self.register_error_routes(self.app)
        self.register_socket_profiling_routes(self.app)

        # make server
        self.host = host
        self.port = port
        self.url = "http://{}:{}".format(host, port)

        # chunk status log
        self.state_update_lock = threading.Lock()
        self.chunk_status: Dict[int, str] = {}  # TODO: maintain as chunk_status_log is dumped
        self.chunk_requests: Dict[int, ChunkRequest] = {}
        self.sender_compressed_sizes: Dict[str, Tuple[int, int]] = {}  # TODO: maintain as chunks are completed
        self.chunk_status_log: List[Dict] = []
        self.chunk_completions = defaultdict(list)

        # socket profiles
        # TODO: actually fill these out
        self.sender_socket_profiles: List[Dict] = []
        self.sender_socket_profiles_lock = threading.Lock()
        self.receiver_socket_profiles: List[Dict] = []
        self.receiver_socket_profiles_lock = threading.Lock()

        logging.getLogger("werkzeug").setLevel(logging.DEBUG)
        self.server = make_server(host, port, self.app, threaded=True)

    def pull_chunk_status_queue(self, timeout=0.5):
        with self.state_update_lock:
            while True:
                try:
                    elem = self.chunk_store.chunk_status_queue.get(timeout=timeout)
                except Empty:
                    #print("[gateway_api] Chunk status queue empty, no more updates")
                    break

                handle = elem["handle"]
                state = elem["state"]
                chunk_id = elem["chunk_id"]

                # if terminal operator, then mark a chunk completion
                if handle in self.terminal_operators[elem["partition"]] and state == ChunkState.complete.name:
                    self.chunk_completions[elem["chunk_id"]].append(handle)

                # if all terminal operators complete, then mark chunk complete
                if self.chunk_status.get(chunk_id, None) != ChunkState.complete.name and len(self.chunk_completions[chunk_id]) == len(
                    self.terminal_operators[elem["partition"]]
                ):
                    # TODO: set this somewhere else
                    self.chunk_status[chunk_id] = ChunkState.complete.name

                    print(f"[gateway_api] chunk {chunk_id}: complete, all operators have uploaded {self.terminal_operators}")
                    # remove chunk file
                    chunk_file_path = self.chunk_store.get_chunk_file_path(elem["chunk_id"])
                    if os.path.exists(chunk_file_path):
                        logging.info(f"[gateway_api] Removing chunk file {chunk_file_path}")
                        chunk_file_path.unlink()

                    # record compressed size
                    if "metadata" in elem and "compressed_size_bytes" in elem["metadata"] and "uncompressed_size_bytes" in elem["metadata"]:
                        self.sender_compressed_sizes[chunk_id] = (
                            elem["metadata"]["compressed_size_bytes"],
                            elem["metadata"]["uncompressed_size_bytes"],
                        )
                else:
                    if elem["state"] == ChunkState.complete.name:
                       print(
                            f"[gateway_api] chunk {chunk_id}: not complete "
                            + f"operators {self.chunk_completions[chunk_id]} have uploaded "
                            + f"out of {self.terminal_operators[elem['partition']]}"
                        )
                    else:
                        print(f"[gateway_api] chunk {chunk_id}: state = {elem['state']}")
                self.chunk_status_log.append(elem)

    def run(self):
        self.server.serve_forever()

    def shutdown(self):
        self.server.shutdown()

    def register_global_routes(self, app):
        @app.route("/", methods=["GET"])
        def get_index():
            return jsonify({"version": "v1"})

        @app.route("/api/v1", methods=["GET"])
        def get_v1_index():
            output = ""
            for rule in sorted(self.app.url_map.iter_rules(), key=lambda r: r.rule):
                if rule.endpoint != "static":
                    methods = set(m for m in rule.methods if m not in ["HEAD", "OPTIONS"])
                    output += f"<a href='{rule.rule}'>{rule.rule}</a>: {methods}<br>"
            return output

        @app.route("/api/v1/status", methods=["GET"])
        def get_status():
            return jsonify({"status": "ok"})

        @app.route("/api/v1/shutdown", methods=["POST"])
        def shutdown():
            self.shutdown()
            logger.error("Shutdown complete. Hard exit.")
            os._exit(1)

    def register_server_routes(self, app):
        @app.route("/api/v1/servers", methods=["GET"])
        def get_server_ports():
            return jsonify({"server_ports": self.gateway_receiver.server_ports})

        @app.route("/api/v1/servers", methods=["POST"])
        def add_server():
            new_port = self.gateway_receiver.start_server()
            return jsonify({"server_port": new_port})

        @app.route("/api/v1/servers/<int:port>", methods=["DELETE"])
        def remove_server(port: int):
            try:
                self.gateway_receiver.stop_server(port)
                return jsonify({"status": "ok"})
            except ValueError as e:
                return jsonify({"error": str(e)}), 400

    def register_request_routes(self, app):
        def make_chunk_req_payload(chunk_req: ChunkRequest):
            state = self.chunk_status[chunk_req.chunk.chunk_id]
            state_name = state if state is not None else "unknown"
            return {"req": chunk_req.as_dict(), "state": state_name}

        def get_chunk_reqs(state=None) -> Dict[int, Dict]:
            out = {}
            for chunk_id in list(self.chunk_status.keys()):
                chunk_state = self.chunk_status[chunk_id]
                if state is None or chunk_state == state:
                    chunk_req = self.chunk_requests[chunk_id]
                    out[chunk_id] = make_chunk_req_payload(chunk_req)
            return out

        def add_chunk_req(body, state):
            if isinstance(body, dict):
                chunk_req = ChunkRequest.from_dict(body)
                self.chunk_requests[chunk_req.chunk.chunk_id] = chunk_req
                self.chunk_store.add_chunk_request(chunk_req, state)
                return 1
            elif isinstance(body, list):
                for row in body:
                    chunk_req = ChunkRequest.from_dict(row)
                    self.chunk_requests[chunk_req.chunk.chunk_id] = chunk_req
                    self.chunk_store.add_chunk_request(chunk_req, state)
                return len(body)

        @app.route("/api/v1/chunk_requests", methods=["GET"])
        def get_chunk_requests():
            state_param = request.args.get("state")
            if state_param is not None:
                try:
                    state = ChunkState.from_str(state_param)
                except ValueError:
                    return jsonify({"error": "invalid state"}), 400
                return jsonify({"chunk_requests": get_chunk_reqs(state)})
            else:
                return jsonify({"chunk_requests": get_chunk_reqs()})

        @app.route("/api/v1/incomplete_chunk_requests", methods=["GET"])
        def get_incomplete_chunk_requests():
            return jsonify({"chunk_requests": {k: v for k, v in get_chunk_reqs().items() if v["state"] != "upload_complete"}})

        # lookup chunk request given chunk worker_id
        @app.route("/api/v1/chunk_requests/<int:chunk_id>", methods=["GET"])
        def get_chunk_request(chunk_id: int):
            chunk_req = self.chunk_requests.get(chunk_id)
            if chunk_req:
                return jsonify({"chunk_requests": [make_chunk_req_payload(chunk_req)]})
            else:
                return jsonify({"error": f"Chunk {chunk_id} not found"}), 404

        # add a new chunk request with default state registered
        @app.route("/api/v1/chunk_requests", methods=["POST"])
        def add_chunk_request():
            logging.debug(f"[gateway_api] Recieved chunk request {request.json}")
            state_param = request.args.get("state", "registered")
            n_added = add_chunk_req(request.json, ChunkState.from_str(state_param))
            # TODO: Add to chunk manager queue
            return jsonify({"status": "ok", "n_added": n_added})

        # update chunk request
        @app.route("/api/v1/chunk_requests/<int:chunk_id>", methods=["PUT"])
        def update_chunk_request(chunk_id: int):
            chunk_req = self.chunk_requests.get(chunk_id)
            if chunk_req is None:
                return jsonify({"error": f"Chunk {chunk_id} not found"}), 404
            else:
                if "state" in request.args:
                    try:
                        state = ChunkState.from_str(request.args.get("state"))
                    except ValueError:
                        return jsonify({"error": "invalid state"}), 400
                    self.chunk_store.log_chunk_state(chunk_req, state)
                    return jsonify({"status": "ok"})
                else:
                    return jsonify({"error": "update not supported"}), 400

        # list chunk status log
        @app.route("/api/v1/chunk_status_log", methods=["GET"])
        def get_chunk_status_log():
            n_entries = len(self.chunk_status_log)
            status_log_copy = []  # copy to support concurrent access
            for i in range(n_entries):
                status_log_copy.append(self.chunk_status_log[i])
            return jsonify({"chunk_status_log": status_log_copy})

    def register_error_routes(self, app):
        @app.route("/api/v1/errors", methods=["GET"])
        def get_errors():
            with self.error_list_lock:
                while True:
                    try:
                        elem = self.error_queue.get_nowait()
                        self.error_list.append(elem)
                    except Empty:
                        break
                # convert TracebackException to list
                error_list_str = [str(e) for e in self.error_list]
                return jsonify({"errors": error_list_str})

    def register_socket_profiling_routes(self, app):
        @app.route("/api/v1/profile/socket/receiver", methods=["GET"])
        def get_receiver_socket_profiles():
            with self.receiver_socket_profiles_lock:
                while True:
                    try:
                        elem = self.gateway_receiver.socket_profiler_event_queue.get_nowait()
                        self.receiver_socket_profiles.append(elem)
                    except Empty:
                        break
                return jsonify({"socket_profiles": self.receiver_socket_profiles})

        @app.route("/api/v1/profile/compression", methods=["GET"])
        def get_receiver_compression_profile():
            total_size_compressed_bytes, total_size_uncompressed_bytes = 0, 0
            for _, (compressed_size, uncompressed_size) in self.sender_compressed_sizes.items():
                total_size_compressed_bytes += compressed_size
                total_size_uncompressed_bytes += uncompressed_size
            return jsonify(
                {
                    "compressed_bytes_sent": total_size_compressed_bytes,
                    "uncompressed_bytes_sent": total_size_uncompressed_bytes,
                    "compression_ratio": total_size_uncompressed_bytes / total_size_compressed_bytes
                    if total_size_compressed_bytes > 0
                    else 0,
                }
            )
