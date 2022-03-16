import threading
from typing import Optional
import googleapiclient.discovery
import google.auth

from skylark import cloud_config


class GCPAuthentication:
    __cached_credentials = threading.local()
    
    def __init__(self, project_id: Optional[str] = cloud_config.gcp_project_id):
        self.credentials, self.project_id = self.make_credential(project_id)
    
    def make_credential(self, project_id):
        cached_credential = getattr(self.__cached_credentials, f"credential_{project_id}", (None, None))
        if cached_credential == (None, None):
            cached_credential = google.auth.default(quota_project_id=project_id)
            setattr(self.__cached_credentials, f"credential_{project_id}", cached_credential)
        return cached_credential

    def enabled(self):
        return self.credentials is not None and self.project_id is not None

    def get_gcp_client(self, service_name="compute", version="v1"):
        return googleapiclient.discovery.build(service_name, version)

    def get_gcp_instances(self, gcp_region: str):
        return self.get_gcp_client().instances().list(project=self.project_id, zone=gcp_region).execute()
