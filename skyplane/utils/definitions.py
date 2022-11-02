import os

from skyplane.gateway_version import gateway_version

KB = 1024
MB = 1024 * 1024
GB = 1024 * 1024 * 1024


def format_bytes(bytes_int: int):
    if bytes_int < KB:
        return f"{bytes_int}B"
    elif bytes_int < MB:
        return f"{bytes_int / KB:.2f}KB"
    elif bytes_int < GB:
        return f"{bytes_int / MB:.2f}MB"
    else:
        return f"{bytes_int / GB:.2f}GB"


is_gateway_env = os.environ.get("SKYPLANE_IS_GATEWAY", None) == "1"


def gateway_docker_image():
    return "public.ecr.aws/s6m1p0n8/skyplane:" + gateway_version
