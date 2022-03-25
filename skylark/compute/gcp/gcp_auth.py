import threading
from typing import Optional
import googleapiclient.discovery
import google.auth

from skylark import cloud_config
from skylark.config import SkylarkConfig
from skylark import config_path


class GCPAuthentication:
    __cached_credentials = threading.local()

    def __init__(self, project_id: Optional[str] = cloud_config.gcp_project_id):
        # load credentials lazily and then cache across threads
        self.config = SkylarkConfig.load_config(config_path)
        self.inferred_project_id = project_id
        self._credentials = None
        self._project_id = None

    @property
    def credentials(self):
        if self._credentials is None:
            self._credentials, self._project_id = self.make_credential(self.inferred_project_id)
        return self._credentials

    @property
    def project_id(self):
        if self._project_id is None:
            self._credentials, self._project_id = self.make_credential(self.inferred_project_id)
        return self._project_id

    def make_credential(self, project_id):
        cached_credential = getattr(self.__cached_credentials, f"credential_{project_id}", (None, None))
        if cached_credential == (None, None):
            cached_credential = google.auth.default(quota_project_id=project_id)
            setattr(self.__cached_credentials, f"credential_{project_id}", cached_credential)
        return cached_credential

    def enabled(self):
        return self.config.gcp_enabled and self.credentials is not None and self.project_id is not None

    def get_gcp_client(self, service_name="compute", version="v1"):
        return googleapiclient.discovery.build(service_name, version)

    def get_gcp_instances(self, gcp_region: str):
        return self.get_gcp_client().instances().list(project=self.project_id, zone=gcp_region).execute()
