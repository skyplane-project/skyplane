from typing import Optional

import google.auth
from google.cloud import storage  # type: ignore
from googleapiclient import discovery

from skyplane import cloud_config, config_path, gcp_config_path
from skyplane.config import SkyplaneConfig
from skyplane.utils import logger


class GCPAuthentication:
    def __init__(self, config: Optional[SkyplaneConfig] = None, project_id: Optional[str] = cloud_config.gcp_project_id):
        if not config == None:
            self.config = config
        else:
            self.config = SkyplaneConfig.load_config(config_path)
        self._credentials = None

    def save_region_config(self):
        if self.project_id is None:
            print(
                f"    No project ID detected when trying to save GCP region list! Consquently, the GCP region list is empty. Run 'skyplane init --reinit-gcp' or file an issue to remedy this."
            )
            self.clear_region_config()
            return
        with gcp_config_path.open("w") as f:
            region_list = []
            credentials = self.credentials
            service = discovery.build("compute", "beta", credentials=credentials)
            request = service.zones().list(project=self.project_id)
            while request is not None:
                response = request.execute()
                # In reality, these are zones. However, we shall call them regions to be self-consistent.
                for region in response["items"]:
                    region_list.append(region["description"])

                request = service.regions().list_next(previous_request=request, previous_response=response)

            f.write("\n".join(region_list))
            print(f"    GCP region config file saved to {gcp_config_path}")

    @staticmethod
    def clear_region_config():
        with gcp_config_path.open("w") as f:
            f.write("")

    @staticmethod
    def get_region_config():
        try:
            f = open(gcp_config_path, "r")
        except FileNotFoundError:
            print("    No GCP config detected! Consquently, the GCP region list is empty. Run 'skyplane init --reinit-gcp' to remedy this.")
            return []
        return [r for r in map(str.strip, f.readlines()) if r]

    @property
    def credentials(self):
        if self._credentials is None:
            self._credentials, _ = self.get_adc_credential(self.project_id)
        return self._credentials

    @property
    def project_id(self):
        return self.config.gcp_project_id

    @staticmethod
    def get_adc_credential(project_id=None):
        try:
            inferred_cred, inferred_project = google.auth.default(quota_project_id=project_id)
        except google.auth.exceptions.DefaultCredentialsError as e:
            logger.error(f"Failed to load GCP credentials for project {project_id}: {e}")
            inferred_cred, inferred_project = (None, None)
        if project_id is not None and project_id != inferred_project:
            logger.warning(
                f"Google project ID error: Project ID from config {project_id} does not match inferred project from google.auth ADC {inferred_project}. Defaulting to config project."
            )
            inferred_project = project_id
        return inferred_cred, inferred_project

    def enabled(self):
        return self.config.gcp_enabled and self.credentials is not None and self.project_id is not None

    def get_gcp_client(self, service_name="compute", version="v1"):
        return discovery.build(service_name, version, credentials=self.credentials, client_options={"quota_project_id": self.project_id})

    def get_storage_client(self):
        return storage.Client(project=self.project_id, credentials=self.credentials)

    def get_gcp_instances(self, gcp_region: str):
        return self.get_gcp_client().instances().list(project=self.project_id, zone=gcp_region).execute()
