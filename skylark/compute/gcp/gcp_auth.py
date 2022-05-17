import threading
from typing import Optional

import google.auth
from google.cloud import storage  # type: ignore
from googleapiclient import discovery

from skylark import cloud_config, config_path, gcp_config_path
from skylark.config import SkylarkConfig
from skylark.utils import logger


class GCPAuthentication:
    __cached_credentials = threading.local()

    def __init__(self, config: Optional[SkylarkConfig] = None, project_id: Optional[str] = cloud_config.gcp_project_id):
        # load credentials lazily and then cache across threads
        if not config == None:
            self.config = config
        else:
            self.config = SkylarkConfig.load_config(config_path)
        self.inferred_project_id = project_id
        self._credentials = None
        self._project_id = None

    def save_region_config(self, project_id=None):
        project_id = project_id if project_id is not None else self.project_id
        if project_id is None:
            self.clear_region_config()
            print(
                f"    No project ID detected when trying to save GCP region list! Consquently, the GCP region list is empty. Run 'skylark init --reinit-gcp' or file an issue to remedy this."
            )
            return
        with gcp_config_path.open("w") as f:
            region_list = []
            credentials = self.credentials
            service = discovery.build("compute", "beta", credentials=credentials)
            request = service.zones().list(project=project_id)
            while request is not None:
                response = request.execute()
                # In reality, these are zones. However, we shall call them regions to be self-consistent.
                for region in response["items"]:
                    region_list.append(region["description"])

                request = service.regions().list_next(previous_request=request, previous_response=response)

            f.write("\n".join(region_list))
            print(f"    GCP region config file saved to {gcp_config_path}")

    def clear_region_config(self):
        with gcp_config_path.open("w") as f:
            f.write("")

    @staticmethod
    def get_region_config():
        try:
            f = open(gcp_config_path, "r")
        except FileNotFoundError:
            print("    No GCP config detected! Consquently, the GCP region list is empty. Run 'skylark init --reinit-gcp' to remedy this.")
            return []
        return [r for r in map(str.strip, f.readlines()) if r]

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
            try:
                inferred_cred, inferred_project = google.auth.default(quota_project_id=project_id)
            except google.auth.exceptions.DefaultCredentialsError as e:
                logger.error(f"Failed to load GCP credentials for project {project_id}: {e}")
                inferred_cred, inferred_project = (None, None)
            setattr(self.__cached_credentials, f"credential_{project_id}", (inferred_cred, project_id or inferred_project))
            return inferred_cred, inferred_project
        return cached_credential

    def enabled(self):
        return self.config.gcp_enabled and self.credentials is not None and self.project_id is not None

    def get_gcp_client(self, service_name="compute", version="v1"):
        return discovery.build(service_name, version, credentials=self.credentials, client_options={"quota_project_id": self.project_id})

    def get_storage_client(self):
        return storage.Client(project=self.project_id, credentials=self.credentials)

    def get_gcp_instances(self, gcp_region: str):
        return self.get_gcp_client().instances().list(project=self.project_id, zone=gcp_region).execute()
