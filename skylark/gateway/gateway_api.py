from flask import Flask, jsonify, request
from skylark.gateway.chunk_store import ChunkRequest
from skylark.gateway.gateway import Gateway


class GatewayMetadataServer:
    """
    API documentation:
    * GET /api/v1/status - returns status of API
    * GET /api/v1/servers - returns list of running servers
    * POST /api/v1/servers - starts a new server
    * DELETE /api/v1/servers/<int:port> - stops a server
    * GET /api/v1/chunks - returns list of chunks
    * GET /api/v1/chunks/<int:chunk_id> - returns chunk details
    * GET /api/v1/chunk_requests - returns list of pending chunk requests
    * GET /api/v1/chunk_requests/<int:chunk_id> - returns chunk request details
    * POST /api/v1/chunk_requests - adds a new chunk request to end of pending requests
    """

    def __init__(self, gateway: Gateway):
        self.app = Flask("gateway_metadata_server")
        self.gateway = gateway
        self.register_global_routes()
        self.register_server_routes()
        self.register_chunk_routes()
        self.register_request_routes()

    def run(self, host="0.0.0.0", port=8080):
        self.app.run(host=host, port=port, debug=True)

    def register_global_routes(self):
        # index route returns API version
        @self.app.route("/", methods=["GET"])
        def get_index():
            return jsonify({"version": "v1"})

        # index for v1 api routes
        @self.app.route("/api/v1", methods=["GET"])
        def get_v1_index():
            return jsonify({"version": "v1"})

        # status route returns if API is up
        @self.app.route("/api/v1/status", methods=["GET"])
        def get_status():
            return jsonify({"status": "ok"})

    def register_server_routes(self):
        # list running gateway servers w/ ports
        @self.app.route("/api/v1/servers", methods=["GET"])
        def get_server_ports():
            return jsonify({"server_ports": self.gateway.server_ports})

        # add a new server
        @self.app.route("/api/v1/servers", methods=["POST"])
        def add_server():
            new_port = self.gateway.start_server()
            return jsonify({"server_port": new_port})

        # remove a server
        @self.app.route("/api/v1/servers/<int:port>", methods=["DELETE"])
        def remove_server(port: int):
            try:
                self.gateway.stop_server(port)
                return jsonify({"status": "ok"})
            except ValueError as e:
                return jsonify({"error": str(e)}), 400

    def register_chunk_routes(self):
        # list chunks
        @self.app.route("/api/v1/chunks", methods=["GET"])
        def get_chunks():
            reply = {}
            for chunk_data in self.gateway.chunk_store.get_chunks():
                chunk_id = chunk_data.chunk_id
                reply[chunk_id] = chunk_data.copy()
            return jsonify(reply)

        # get chunk details
        @self.app.route("/api/v1/chunks/<int:chunk_id>", methods=["GET"])
        def get_chunk(chunk_id: int):
            if chunk_id in self.gateway.chunks:
                return jsonify(dict(self.gateway.chunk_store.get_chunk(chunk_id)))
            else:
                return jsonify({"error": f"Chunk {chunk_id} not found"}), 404

    def register_request_routes(self):
        # list all chunk requests
        @self.app.route("/api/v1/chunk_requests", methods=["GET"])
        def get_all_chunk_requests():
            pending, downloaded, uploaded = self.gateway.chunk_store.get_chunk_requests()
            return jsonify({"pending": pending, "downloaded": downloaded, "uploaded": uploaded})

        # list pending chunk requests
        @self.app.route("/api/v1/chunk_requests/pending", methods=["GET"])
        def get_pending_chunk_requests():
            pending = self.gateway.chunk_store.get_chunk_requests()[0]
            return jsonify({"pending": pending})

        # list downloaded chunk requests
        @self.app.route("/api/v1/chunk_requests/downloaded", methods=["GET"])
        def get_downloaded_chunk_requests():
            downloaded = self.gateway.chunk_store.get_chunk_requests()[1]
            return jsonify({"downloaded": downloaded})

        # list uploaded chunk requests
        @self.app.route("/api/v1/chunk_requests/uploaded", methods=["GET"])
        def get_uploaded_chunk_requests():
            uploaded = self.gateway.chunk_store.get_chunk_requests()[2]
            return jsonify({"uploaded": uploaded})

        # lookup chunk request given chunk id
        @self.app.route("/api/v1/chunk_requests/<int:chunk_id>", methods=["GET"])
        def get_chunk_request(chunk_id: int):
            chunk_req = self.gateway.chunk_store.get_chunk_request(chunk_id)
            if chunk_req is None:
                return jsonify({"error": f"Chunk {chunk_id} not found"}), 404
            else:
                return jsonify(chunk_req)

        # add a new chunk request to end of pending requests
        @self.app.route("/api/v1/chunk_requests/pending", methods=["POST"])
        def add_chunk_request():
            if isinstance(request.json, dict):
                self.gateway.chunk_store.add_chunk_request_pending(ChunkRequest.from_dict(request.json))
                return jsonify({"status": "ok"})
            elif isinstance(request.json, list):
                for chunk_req in request.json:
                    self.gateway.chunk_store.add_chunk_request_pending(ChunkRequest.from_dict(chunk_req))
                return jsonify({"status": "ok"})

        # add a new chunk request to end of downloaded requests
        @self.app.route("/api/v1/chunk_requests/downloaded", methods=["POST"])
        def add_downloaded_chunk_request():
            if isinstance(request.json, dict):
                self.gateway.chunk_store.add_chunk_request_downloaded(ChunkRequest.from_dict(request.json))
                return jsonify({"status": "ok"})
            elif isinstance(request.json, list):
                for chunk_req in request.json:
                    self.gateway.chunk_store.add_chunk_request_downloaded(ChunkRequest.from_dict(chunk_req))
                return jsonify({"status": "ok"})

        # add a new chunk request to end of uploaded requests
        @self.app.route("/api/v1/chunk_requests/uploaded", methods=["POST"])
        def add_uploaded_chunk_request():
            if isinstance(request.json, dict):
                self.gateway.chunk_store.add_chunk_request_uploaded(ChunkRequest.from_dict(request.json))
                return jsonify({"status": "ok"})
            elif isinstance(request.json, list):
                for chunk_req in request.json:
                    self.gateway.chunk_store.add_chunk_request_uploaded(ChunkRequest.from_dict(chunk_req))
                return jsonify({"status": "ok"})
