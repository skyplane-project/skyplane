import logging
import logging.handlers
import requests
import threading

from flask import Flask, jsonify
from werkzeug.serving import make_server


class ReplicatorClientDashboard(threading.Thread):
    def __init__(self, host="locahost", port=8080):
        super().__init__()
        self.status_df = None
        self.app = Flask("gateway_client_dashboard")
        self.register_status_route()

        # disable logging
        logging.getLogger("werkzeug").setLevel(logging.ERROR)
        self.server = make_server(host, port, self.app, threaded=True)

        ip = requests.get("https://api.ipify.org").content.decode("utf8")
        self.dashboard_url = f"http://{ip}:{port}"

    def update_status_df(self, status_df):
        self.status_df = status_df

    def register_status_route(self):
        @self.app.route("/client_api/v1/grouped_status", methods=["GET"])
        def grouped_status_route():
            if self.status_df is None:
                return jsonify([])
            last_log_df = (
                self.status_df.groupby(["chunk_id"])
                .apply(lambda df: df.sort_values(["path_idx", "hop_idx", "time"], ascending=False).head(1))
                .reset_index(drop=True)
            )
            num_hops_per_path = self.status_df.groupby(["chunk_id", "path_idx"])["hop_idx"].max().reset_index(drop=True).to_dict()

            # remove complete rows
            row_match_fn = lambda row: row["state"] == "upload_complete" and row["hop_idx"] == num_hops_per_path[row["path_idx"]]
            last_log_df = last_log_df[~last_log_df.apply(row_match_fn, axis=1)]

            out_rows = []
            for group, df in last_log_df.sort_values(["path_idx", "hop_idx", "chunk_id"]).groupby(["path_idx", "hop_idx"]):
                states = {}
                for state, state_df in df.groupby("state"):
                    states[state.name] = list(sorted(state_df["chunk_id"].astype(int).tolist()))
                out_rows.append({"path_idx": int(group[0]), "hop_idx": int(group[1]), "chunk_states": states})
            return jsonify(out_rows)

        @self.app.route("/", methods=["GET"])
        def index_route():
            return self.app.send_static_file("index.html")

    def run(self):
        self.server.serve_forever()

    def shutdown(self):
        self.server.shutdown()

    def close(self):
        self.server.shutdown()
