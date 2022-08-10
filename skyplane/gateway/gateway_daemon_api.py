import logging
import logging.handlers
import os
import threading
from multiprocessing import Queue
from queue import Empty
from traceback import TracebackException
from typing import Dict, List

from flask import Flask, jsonify, request
from werkzeug.serving import make_server

from skyplane.chunk import ChunkRequest, ChunkState
from skyplane.gateway.chunk_store import ChunkStore
from skyplane.gateway.gateway_receiver import GatewayReceiver
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
        self, chunk_store: ChunkStore, gateway_receiver: GatewayReceiver, error_event, error_queue: Queue, host="0.0.0.0", port=8081
    ):
        super().__init__()
        self.app = Flask("gateway_metadata_server")
        self.chunk_store = chunk_store
        self.gateway_receiver = gateway_receiver
        self.error_event = error_event
        self.error_queue = error_queue
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
        self.chunk_status_log: List[Dict] = []
        self.chunk_status_log_lock = threading.Lock()

        # socket profiles
        self.sender_socket_profiles: List[Dict] = []
        self.sender_socket_profiles_lock = threading.Lock()
        self.receiver_socket_profiles: List[Dict] = []
        self.receiver_socket_profiles_lock = threading.Lock()

        logging.getLogger("werkzeug").setLevel(logging.WARNING)
        self.server = make_server(host, port, self.app, threaded=True)

    def run(self):
        self.server.serve_forever()

    def shutdown(self):
        self.server.shutdown()

    def register_global_routes(self, app):
        # index route returns API version
        @app.route("/", methods=["GET"])
        def get_index():
            return jsonify({"version": "v1"})

        # index for v1 api routes, return all available routes as HTML page with links
        @app.route("/api/v1", methods=["GET"])
        def get_v1_index():
            output = ""
            for rule in sorted(self.app.url_map.iter_rules(), key=lambda r: r.rule):
                if rule.endpoint != "static":
                    methods = set(m for m in rule.methods if m not in ["HEAD", "OPTIONS"])
                    output += f"<a href='{rule.rule}'>{rule.rule}</a>: {methods}<br>"
            return output

        # status route returns if API is up
        @app.route("/api/v1/status", methods=["GET"])
        def get_status():
            return jsonify({"status": "ok"})

        # shutdown route
        @app.route("/api/v1/shutdown", methods=["POST"])
        def shutdown():
            self.shutdown()
            logger.error("Shutdown complete. Hard exit.")
            os._exit(1)

    def register_server_routes(self, app):
        # list running gateway servers w/ ports
        @app.route("/api/v1/servers", methods=["GET"])
        def get_server_ports():
            return jsonify({"server_ports": self.gateway_receiver.server_ports})

        # add a new server
        @app.route("/api/v1/servers", methods=["POST"])
        def add_server():
            new_port = self.gateway_receiver.start_server()
            return jsonify({"server_port": new_port})

        # remove a server
        @app.route("/api/v1/servers/<int:port>", methods=["DELETE"])
        def remove_server(port: int):
            try:
                self.gateway_receiver.stop_server(port)
                return jsonify({"status": "ok"})
            except ValueError as e:
                return jsonify({"error": str(e)}), 400

    def register_request_routes(self, app):
        def make_chunk_req_payload(chunk_req: ChunkRequest):
            state = self.chunk_store.get_chunk_state(chunk_req.chunk.chunk_id)
            state_name = state.name if state is not None else "unknown"
            return {"req": chunk_req.as_dict(), "state": state_name}

        def get_chunk_reqs(state=None) -> Dict[int, Dict]:
            out = {}
            for chunk_req in self.chunk_store.get_chunk_requests(state):
                out[chunk_req.chunk.chunk_id] = make_chunk_req_payload(chunk_req)
            return out

        def add_chunk_req(body, state):
            if isinstance(body, dict):
                self.chunk_store.add_chunk_request(ChunkRequest.from_dict(body), state)
                return 1
            elif isinstance(body, list):
                for chunk_req in body:
                    self.chunk_store.add_chunk_request(ChunkRequest.from_dict(chunk_req), state)
                return len(body)

        # list all chunk requests
        # body json options:
        #   if state is set in body, then filter by state
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
            chunk_req = self.chunk_store.get_chunk_request(chunk_id)
            if chunk_req:
                return jsonify({"chunk_requests": [make_chunk_req_payload(chunk_req)]})
            else:
                return jsonify({"error": f"Chunk {chunk_id} not found"}), 404

        # add a new chunk request with default state registered
        @app.route("/api/v1/chunk_requests", methods=["POST"])
        def add_chunk_request():
            state_param = request.args.get("state", "registered")
            n_added = add_chunk_req(request.json, ChunkState.from_str(state_param))
            return jsonify({"status": "ok", "n_added": n_added})

        # update chunk request
        @app.route("/api/v1/chunk_requests/<int:chunk_id>", methods=["PUT"])
        def update_chunk_request(chunk_id: int):
            chunk_req = self.chunk_store.get_chunk_request(chunk_id)
            if chunk_req is None:
                return jsonify({"error": f"Chunk {chunk_id} not found"}), 404
            else:
                if "state" in request.args:
                    try:
                        state = ChunkState.from_str(request.args.get("state"))
                    except ValueError:
                        return jsonify({"error": "invalid state"}), 400
                    self.chunk_store.set_chunk_state(chunk_id, state)
                    return jsonify({"status": "ok"})
                else:
                    return jsonify({"error": "update not supported"}), 400

        # list chunk status log
        @app.route("/api/v1/chunk_status_log", methods=["GET"])
        def get_chunk_status_log():
            with self.chunk_status_log_lock:
                self.chunk_status_log.extend(self.chunk_store.drain_chunk_status_queue())
                return jsonify({"chunk_status_log": self.chunk_status_log})

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
            for k, v in self.chunk_store.sender_compressed_sizes.items():
                total_size_compressed_bytes += v
                total_size_uncompressed_bytes += self.chunk_store.get_chunk_request(k).chunk.chunk_length_bytes
            return jsonify(
                {
                    "compressed_bytes_sent": total_size_compressed_bytes,
                    "uncompressed_bytes_sent": total_size_uncompressed_bytes,
                    "compression_ratio": total_size_uncompressed_bytes / total_size_compressed_bytes
                    if total_size_compressed_bytes > 0
                    else 0,
                }
            )
