from typing import List

import googleapiclient.discovery

from skylark.compute.gcp.gcp_server import GCPServer, DEFAULT_GCP_PRIVATE_KEY_PATH
from skylark.compute.cloud_providers import CloudProvider


class GCPCloudProvider(CloudProvider):
    def __init__(self, gcp_project, private_key_path=DEFAULT_GCP_PRIVATE_KEY_PATH):
        super().__init__()
        self.gcp_project = gcp_project
        self.private_key_path = private_key_path

    @property
    def region_list(self):
        return [
            "us-central1-a",
            "us-east1-b",
            "us-east4-a",
            "us-west1-a",
            "us-west2-a",
            "southamerica-east1-a",
            "europe-north1-a",
            "europe-west1-b",
            "asia-east2-a",
        ]

    def get_instance_list(self, region) -> List[GCPServer]:
        gcp_instance_result = GCPServer.gcp_instances(self.gcp_project, region)
        if "items" in gcp_instance_result:
            instance_list = []
            for i in gcp_instance_result["items"]:
                instance_list.append(GCPServer(f"gcp:{region}", self.gcp_project, i["name"], ssh_private_key=self.private_key_path))
            return instance_list
        else:
            return []
