import os

from skyplane.gateway_version import gateway_version

KB = 1024
MB = 1024 * 1024
GB = 1024 * 1024 * 1024


def format_bytes(bytes: int):
    if bytes < KB:
        return f"{bytes}B"
    elif bytes < MB:
        return f"{bytes / KB:.2f}KB"
    elif bytes < GB:
        return f"{bytes / MB:.2f}MB"
    else:
        return f"{bytes / GB:.2f}GB"


is_gateway_env = os.environ.get("SKYPLANE_IS_GATEWAY", None) == "1"


def gateway_docker_image():
    return "public.ecr.aws/s6m1p0n8/skyplane:" + gateway_version
