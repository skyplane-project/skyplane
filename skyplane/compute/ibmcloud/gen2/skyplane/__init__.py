from skyplane.compute.ibmcloud.gen2.skyplane.api_key import SkyApiKeyConfig
from skyplane.compute.ibmcloud.gen2.skyplane.endpoint import SkyEndpointConfig
from skyplane.compute.ibmcloud.gen2.skyplane.floating_ip import FloatingIpConfig
from skyplane.compute.ibmcloud.gen2.skyplane.image import SkyImageConfig
from skyplane.compute.ibmcloud.gen2.skyplane.ssh_key import SkySshKeyConfig
from skyplane.compute.ibmcloud.gen2.skyplane.vpc import SkyVPCConfig
from skyplane.compute.ibmcloud.gen2.skyplane.profile import SkyProfileConfig

MODULES = [SkyApiKeyConfig, SkyEndpointConfig, SkyVPCConfig, SkySshKeyConfig, SkyImageConfig, FloatingIpConfig, SkyProfileConfig]

from skyplane.compute.ibmcloud.gen2.utils import load_base_config


def load_config(
    iam_api_key,
    region=None,
    image_id=None,
    profile_name="bx2-2x8",
    key_id=None,
    ssh_key_filename=None,
    vpc_id=None,
    min_workers=0,
    max_workers=0,
):

    base_config = load_base_config()

    base_config["provider"]["iam_api_key"] = iam_api_key
    base_config["available_node_types"]["ray_head_default"]["node_config"]["vpc_id"] = vpc_id
    base_config["available_node_types"]["ray_head_default"]["node_config"]["image_id"] = image_id
    base_config["available_node_types"]["ray_head_default"]["node_config"]["instance_profile_name"] = profile_name
    base_config["available_node_types"]["ray_head_default"]["node_config"]["key_id"] = key_id
    base_config["auth"]["ssh_private_key"] = ssh_key_filename

    base_config["provider"]["region"] = region
    base_config["provider"]["endpoint"] = f"https://{region}.iaas.cloud.ibm.com"

    base_config["max_workers"] = max_workers
    base_config["available_node_types"]["ray_head_default"]["min_workers"] = min_workers
    base_config["available_node_types"]["ray_head_default"]["max_workers"] = max_workers

    return base_config


def parse_config(config):
    res = {"iam_api_key": config["provider"]["iam_api_key"]}

    for available_node_type in config["available_node_types"]:
        res["vpc_id"] = config["available_node_types"][available_node_type]["node_config"]["vpc_id"]
        res["key_id"] = config["available_node_types"][available_node_type]["node_config"]["key_id"]
        res["subnet_id"] = config["available_node_types"][available_node_type]["node_config"]["subnet_id"]

    res["endpoint"] = config["provider"]["endpoint"]

    if "iam_endpoint" in config["provider"]:
        res["iam_endpoint"] = config["provider"]["iam_endpoint"]

    return res
