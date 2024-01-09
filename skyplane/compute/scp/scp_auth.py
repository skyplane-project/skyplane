# (C) Copyright Samsung SDS. 2023

#

# Licensed under the Apache License, Version 2.0 (the "License");

# you may not use this file except in compliance with the License.

# You may obtain a copy of the License at

#

#     http://www.apache.org/licenses/LICENSE-2.0

#

# Unless required by applicable law or agreed to in writing, software

# distributed under the License is distributed on an "AS IS" BASIS,

# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

# See the License for the specific language governing permissions and

# limitations under the License.
import json
from typing import List, Optional
from skyplane.config_paths import config_path, scp_config_path, scp_quota_path

from skyplane.compute.server import key_root
from skyplane.config import SkyplaneConfig
from skyplane.compute.scp.scp_utils import SCPClient
from skyplane.compute.scp.scp_utils import CREDENTIALS_PATH


class SCPAuthentication:
    def __init__(self, config: Optional[SkyplaneConfig] = None):
        if not config is None:
            self.config = config
        else:
            self.config = SkyplaneConfig.load_config(config_path)

        self._scp_access_key = self.config.scp_access_key
        self._scp_secret_key = self.config.scp_secret_key
        self._scp_project_id = self.config.scp_project_id

    def _get_scp_vm_quota(self, region):
        """scp vm quota - no restirction on vcpu, but if no enough quota, scp will return error"""
        result_list = []
        for region_name in region:
            region_dict = {"on_demand_standard_vcpus": 500, "service_zone_name": region_name}
            result_list.append(region_dict)

        return result_list

    def save_region_config(self, config: SkyplaneConfig):
        if self.config.scp_enabled == False:
            self.clear_region_config()
            return
        region_list = self.get_region_config()

        with open(scp_config_path, "w") as f:
            for region in region_list:
                f.write(region + "\n")

        quota_infos = self._get_scp_vm_quota(region_list)
        with scp_quota_path.open("w") as f:
            f.write(json.dumps(quota_infos, indent=2))

    @staticmethod
    def get_zones() -> dict:
        scp_client = SCPClient()
        url = f"/project/v3/projects/{scp_client.project_id}/zones"

        response = scp_client._get(url)
        return response

    @staticmethod
    def get_region_config() -> List[str]:
        zones = SCPAuthentication.get_zones()
        # print(zones)
        service_zone_locations = [zone["serviceZoneName"] for zone in zones]
        return service_zone_locations

    def clear_region_config():
        with scp_config_path.open("w") as f:
            f.write("")
        with scp_quota_path.open("w") as f:
            f.write("")

    def enabled(self):
        return self.config.scp_enabled

    def get_zone_location(self, zone_name):
        zones = self.get_zones()
        for zone in zones:
            if zone["serviceZoneName"] == zone_name:
                return zone["serviceZoneLocation"]
        return None

    @property
    def credential_path(self):
        credential_path = key_root / "scp" / "scp_credential"
        return credential_path

    @property
    def scp_credential_path(self):
        return CREDENTIALS_PATH

    @property
    def scp_access_key(self):
        if self._scp_access_key is None:
            self._scp_access_key, self._scp_secret_key, self._scp_project_id = self.infer_credentials()
        return self._scp_access_key

    @property
    def scp_secret_key(self):
        if self._scp_secret_key is None:
            self._scp_access_key, self._scp_secret_key, self._scp_project_id = self.infer_credentials()
        return self._scp_secret_key

    @property
    def scp_project_id(self):
        if self._scp_project_id is None:
            self._scp_access_key, self._scp_secret_key, self._scp_project_id = self.infer_credentials()
        return self._scp_project_id

    def infer_credentials(self):
        scp_client = SCPClient()
        return scp_client.access_key, scp_client.secret_key, scp_client.project_id
