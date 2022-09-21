from pathlib import Path
from typing import Optional
import base64
import os

from skyplane import config_path, gcp_config_path, key_root
from skyplane.config import SkyplaneConfig
from skyplane.utils import logger, imports
from skyplane.utils.retry import retry_backoff


class GCPAuthentication:
    def __init__(self, config: Optional[SkyplaneConfig] = None):
        if not config == None:
            self.config = config
        else:
            self.config = SkyplaneConfig.load_config(config_path)
        self._credentials = None
        self._service_credentials_file = None

    @imports.inject("googleapiclient.discovery", pip_extra="gcp")
    def save_region_config(discovery, self):
        if self.project_id is None:
            print(
                f"    No project ID detected when trying to save GCP region list! Consquently, the GCP region list is empty. Run 'skyplane init --reinit-gcp' or file an issue to remedy this."
            )
            self.clear_region_config()
            return
        with gcp_config_path.open("w") as f:
            region_list = []
            credentials = self.credentials
            service_account_credentials_file = self.service_account_credentials  # force creation of file
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
        return self._credentials

    @property
    def service_account_credentials(self):
        if self._service_credentials_file is None:
            self._service_account_email = self.create_service_account(self.service_account_name)
            self._service_credentials_file = self.get_service_account_key(self._service_account_email)
        return self._service_credentials_file

    @property
    def project_id(self):
        assert (
            self.config.gcp_project_id is not None
        ), "No project ID detected. Run 'skyplane init --reinit-gcp' or file an issue to remedy this."
        return self.config.gcp_project_id

    @staticmethod
    @imports.inject("google.auth", pip_extra="gcp")
    def get_adc_credential(google_auth, project_id=None):
        try:
            inferred_cred, inferred_project = google_auth.default(quota_project_id=project_id)
        except google_auth.exceptions.DefaultCredentialsError as e:
            logger.warning(f"Failed to load GCP credentials for project {project_id}: {e}")
            inferred_cred, inferred_project = (None, None)
        if project_id is not None and project_id != inferred_project:
            logger.warning(
                f"Google project ID error: Project ID from config {project_id} does not match inferred project from google.auth ADC {inferred_project}. Defaulting to config project."
            )
            inferred_project = project_id
        return inferred_cred, inferred_project

    @property
    def service_account_name(self):
        return self.config.get_flag("gcp_service_account_name")

    def get_service_account_key(self, service_account_email):
        service = self.get_gcp_client(service_name="iam")

        if "GCP_SERVICE_ACCOUNT_FILE" in os.environ:
            key_path = Path(os.environ["GCP_SERVICE_ACCOUNT_FILE"]).expanduser()
        else:
            key_path = key_root / "gcp" / "service_account_key.json"
        # write key file
        if not os.path.exists(key_path):

            # list existing keys
            keys = service.projects().serviceAccounts().keys().list(name="projects/-/serviceAccounts/" + service_account_email).execute()

            # cannot have more than 10 keys per service account
            if len(keys["keys"]) >= 10:
                raise ValueError(
                    f"Service account {service_account_email} has too many keys. Make sure to copy keys to {key_path} or create a new service account."
                )

            # create key
            key = (
                service.projects()
                .serviceAccounts()
                .keys()
                .create(name="projects/-/serviceAccounts/" + service_account_email, body={})
                .execute()
            )

            # create service key files
            os.makedirs(os.path.dirname(key_path), exist_ok=True)
            json_key_file = base64.b64decode(key["privateKeyData"]).decode("utf-8")
            open(key_path, "w").write(json_key_file)

        return key_path

    def create_service_account(self, service_name):
        service = self.get_gcp_client(service_name="iam")
        service_accounts = service.projects().serviceAccounts().list(name="projects/" + self.project_id).execute()["accounts"]

        # search for pre-existing service account
        account = None
        for service_account in service_accounts:
            if service_account["email"].split("@")[0] == service_name:
                account = service_account
                break

        # create service account
        if account is None:
            account = (
                service.projects()
                .serviceAccounts()
                .create(
                    name="projects/" + self.project_id, body={"accountId": service_name, "serviceAccount": {"displayName": service_name}}
                )
                .execute()
            )

        def read_modify_write():
            # modify service account with storage.admin role
            service = self.get_gcp_client("cloudresourcemanager", "v1")
            policy = service.projects().getIamPolicy(resource=self.project_id).execute()
            account_handle = f"serviceAccount:{account['email']}"

            # modify policy
            modified = False
            roles = [role["role"] for role in policy["bindings"]]
            target_role = "roles/storage.admin"
            if target_role not in roles:
                # role does not exist
                policy["bindings"].append({"role": target_role, "members": [account_handle]})
                modified = True
            else:
                for role in policy["bindings"]:
                    if role["role"] == target_role:
                        if account_handle not in role["members"]:
                            role["members"].append(account_handle)  # do NOT override
                            modified = True
            if modified:  # execute policy change
                service.projects().setIamPolicy(resource=self.project_id, body={"policy": policy}).execute()
            return account["email"]

        return retry_backoff(read_modify_write)  # retry loop needed for concurrent policy modifications

    def enabled(self):
        return self.config.gcp_enabled and self.credentials is not None and self.project_id is not None

    def check_api_enabled(self, api_name: str):
        service_usage = self.get_gcp_client(service_name="serviceusage")
        services = service_usage.services().get(name=f"projects/{self.project_id}/services/{api_name}.googleapis.com").execute()
        return services.get("state") == "ENABLED"

    def enable_api(self, service_name: str):
        service_usage = self.get_gcp_client(service_name="serviceusage")
        return service_usage.services().enable(name=f"projects/{self.project_id}/services/{service_name}.googleapis.com").execute()

    @imports.inject("googleapiclient.discovery", pip_extra="gcp")
    def get_gcp_client(discovery, self, service_name="compute", version="v1"):
        return discovery.build(service_name, version, credentials=self.credentials, client_options={"quota_project_id": self.project_id})

    @imports.inject("google.cloud.storage", pip_extra="gcp")
    def get_storage_client(storage, self):
        # must use service account for XML storage API
        return storage.Client.from_service_account_json(self.service_account_credentials)

    def get_gcp_instances(self, gcp_region: str):
        return self.get_gcp_client().instances().list(project=self.project_id, zone=gcp_region).execute()

    def check_compute_engine_enabled(self):
        """Check if the GCP compute engine API is enabled"""
