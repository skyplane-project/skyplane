#
# (C) Copyright Cloudlab URV 2020
# (C) Copyright IBM Corp. 2023
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
#

import uuid


DEFAULT_CONFIG_KEYS = {
    "master_profile_name": "bx2-2x8",
    "worker_profile_name": "bx2-2x8",
    "boot_volume_profile": "general-purpose",
    "ssh_username": "root",
    "ssh_password": str(uuid.uuid4()),
    "ssh_key_filename": "~/.ssh/id_rsa",
    "delete_on_dismantle": True,
    "max_workers": 100,
    "worker_processes": 2,
    "boot_volume_capacity": 100,
}

VPC_ENDPOINT = "https://{}.iaas.cloud.ibm.com"

REGIONS = ["us-south", "us-east", "eu-de", "jp-tok", "jp-osa", "au-syd", "eu-gb", "br-sao", "ca-tor"]


def load_config(config_data):
    if "ibm" in config_data and config_data["ibm"] is not None:
        config_data["ibm_gen2"].update(config_data["ibm"])

    for key in DEFAULT_CONFIG_KEYS:
        if key not in config_data["ibm_gen2"]:
            config_data["ibm_gen2"][key] = DEFAULT_CONFIG_KEYS[key]

    if "profile_name" in config_data["ibm_gen2"]:
        config_data["worker_profile_name"] = config_data["profile_name"]

    if "region" not in config_data["ibm_gen2"] and "endpoint" not in config_data["ibm_gen2"]:
        msg = "'region' or 'endpoint' parameter is mandatory in 'ibm_gen2' section of the configuration"
        raise Exception(msg)

    if "region" in config_data["ibm_gen2"]:
        region = config_data["ibm_gen2"]["region"]
        if region not in REGIONS:
            msg = f"'region' conig parameter in 'ibm_gen2' section must be one of {REGIONS}"
            raise Exception(msg)
        config_data["ibm_gen2"]["endpoint"] = VPC_ENDPOINT.format(region)

    config_data["ibm_gen2"]["endpoint"] = config_data["ibm_gen2"]["endpoint"].replace("/v1", "")
