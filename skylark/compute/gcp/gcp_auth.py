from functools import lru_cache
import os
import subprocess
import googleapiclient.discovery

from skylark import cloud_config


class GCPAuthentication:
    def __init__(self, project_id: str = cloud_config.gcp_project_id):
        self.project_id = project_id

    def enabled(self):
        return self.project_id is not None

    @staticmethod
    def infer_project_id():
        if "GOOGLE_PROJECT_ID" in os.environ:
            return os.environ["GOOGLE_PROJECT_ID"]
        try:
            return subprocess.check_output(["gcloud", "config", "get-value", "project"]).decode("utf-8").strip()
        except subprocess.CalledProcessError:
            return None

    def get_gcp_client(self, service_name="compute", version="v1"):
        return googleapiclient.discovery.build(service_name, version)

    def get_gcp_instances(self, gcp_region: str):
        return self.get_gcp_client().instances().list(project=self.project_id, zone=gcp_region).execute()
