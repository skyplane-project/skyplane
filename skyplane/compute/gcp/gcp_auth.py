from re import I
from typing import Optional
import base64
import json
import os

import google.auth
from google.cloud import storage  # type: ignore
from googleapiclient import discovery

from skyplane import cloud_config, config_path, gcp_config_path, key_root
from skyplane.config import SkyplaneConfig
from skyplane.utils import logger
from google.oauth2 import service_account



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
            return []
        return [r for r in map(str.strip, f.readlines()) if r]

    @property
    def credentials(self):
        if self._credentials is None:
            self._credentials, _ = self.get_adc_credential(self.project_id)
            self.service_account = self.create_service_account("skyplane", self.project_id)
        return self._credentials

    @property
    def project_id(self):
        return self.config.gcp_project_id

    @staticmethod
    def get_adc_credential(project_id=None):
        try:
            inferred_cred, inferred_project = google.auth.default(quota_project_id=project_id)
        except google.auth.exceptions.DefaultCredentialsError as e:
            logger.warning(f"Failed to load GCP credentials for project {project_id}: {e}")
            inferred_cred, inferred_project = (None, None)
        if project_id is not None and project_id != inferred_project:
            logger.warning(
                f"Google project ID error: Project ID from config {project_id} does not match inferred project from google.auth ADC {inferred_project}. Defaulting to config project."
            )
            inferred_project = project_id
        return inferred_cred, inferred_project

    def set_service_account_credentials(self, service_name):
        _, key_file = self.get_service_account(service_name)
        key = json.loads(open(key_file, "r").read())
        self._credentails = service_account.Credentials.from_service_account_info(key)
 
    def get_service_account(self, service_name): 
        service = self.get_gcp_client(service_name="iam")
        service_account_email = f"{service_name}@{self.project_id}.iam.gserviceaccount.com"
        response = service.projects().serviceAccounts().keys().list(
            name='projects/-/serviceAccounts/' + service_account_email).execute()
        if len(response['keys']) > 0:
            # use existing key
            key = response['keys'][0]
        else:
            # create key
            key = service.projects().serviceAccounts().keys().create(
                name='projects/-/serviceAccounts/' + service_account_email, body={}
            ).execute()

        # write key file
        if not os.path.exists(key_root / "gcp" / "service_account.json"):
            open(key_root / "gcp" / "service_account.json", "w").write(json.dumps(key))
            json_key_file = base64.b64decode(key['privateKeyData']).decode('utf-8')
            open(key_root / "gcp" / "service_account_key.json", "w").write(json_key_file)

        return os.path.join(key_root, "gcp", "service_account.json"), os.path.join(key_root, "gcp", "service_account_key.json")

    def create_service_account(self, service_name, project_id):
        service = self.get_gcp_client(service_name="iam")
        service_accounts = service.projects().serviceAccounts().list(name='projects/' + project_id).execute()["accounts"]
        print(service_accounts)

        for account in service_accounts:
            if account["displayName"] == service_name: 
                return account

        # create service account
        account = service.projects().serviceAccounts().create(
            name='projects/' + project_id,
            body={
                'accountId': service_name,
                'serviceAccount': {
                    'displayName': service_name 
                }
            }).execute()
        print(account)
        return account


    def enabled(self):
        return self.config.gcp_enabled and self.credentials is not None and self.project_id is not None

    def get_gcp_client(self, service_name="compute", version="v1"):
        return discovery.build(service_name, version, credentials=self.credentials, client_options={"quota_project_id": self.project_id})

    def get_storage_client(self):
        return storage.Client(project=self.project_id, credentials=self.credentials)

    def get_gcp_instances(self, gcp_region: str):
        return self.get_gcp_client().instances().list(project=self.project_id, zone=gcp_region).execute()
