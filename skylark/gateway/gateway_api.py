from flask import Flask, jsonify, request
from skylark.gateway.gateway import Gateway


class GatewayMetadataServer:
    def __init__(self, gateway: Gateway):
        self.app = Flask("gateway_metadata_server")
        self.gateway = gateway
        self.register_server_routes()
        self.register_chunk_routes()
        self.register_request_routes()

    def run(self, host="0.0.0.0", port=8080):
        self.app.run(host=host, port=port)

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
            for chunk_id, chunk_data in self.gateway.chunks.items():
                reply[chunk_id] = chunk_data.copy()
            return jsonify(reply)

        # get chunk details
        @self.app.route("/api/v1/chunks/<int:chunk_id>", methods=["GET"])
        def get_chunk(chunk_id: int):
            if chunk_id in self.gateway.chunks:
                return jsonify(dict(self.gateway.chunks[chunk_id]))
            else:
                return jsonify({"error": f"Chunk {chunk_id} not found"}), 404

    def register_request_routes(self):
        # list pending chunk requests
        @self.app.route("/api/v1/chunk_requests", methods=["GET"])
        def get_chunk_requests():
            raise NotImplementedError()
