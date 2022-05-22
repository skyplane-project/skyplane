import shlex
import subprocess
from functools import lru_cache

from skyplane.utils import logger


@lru_cache(maxsize=1)
def query_which_cloud() -> str:
    check_exit_code = lambda cmd: subprocess.call(shlex.split(cmd), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    aws_metadata_url = "curl -f --connect-timeout 1 --noproxy * http://169.254.169.254/1.0/meta-data/instance-id"
    azure_metadata_url = (
        "curl -f --connect-timeout 1 -H Metadata:true --noproxy * http://169.254.169.254/metadata/instance?api-version=2021-02-01"
    )
    gcp_metadata_url = "curl -f --connect-timeout 1 -noproxy * http://metadata.google.internal/computeMetadata/v1/instance/hostname"
    if check_exit_code(aws_metadata_url) == 0:
        return "aws"
    elif check_exit_code(azure_metadata_url) == 0:
        return "azure"
    elif check_exit_code(gcp_metadata_url) == 0:
        return "gcp"
    else:
        return "unknown"


def make_dozzle_command(port):
    cmd = """sudo docker run -d --rm --name dozzle \
        -p {log_viewer_port}:8080 \
        --volume=/var/run/docker.sock:/var/run/docker.sock \
        amir20/dozzle:latest"""
    return "nohup {} > /dev/null 2>&1 &".format(cmd.format(log_viewer_port=port))


def make_sysctl_tcp_tuning_command(cc="cubic"):
    # sam's suggested improvements:
    # net.core.rmem_max = 2147483647
    # net.core.wmem_max = 2147483647
    # net.ipv4.tcp_rmem = 4096 87380 2147483647
    # net.ipv4.tcp_wmem = 4096 65536 2147483647
    # net.ipv4.tcp_mem = 8388608 8388608 8388608
    # net.ipv4.tcp_keepalive_time = 240
    # net.ipv4.tcp_keepalive_intvl = 65
    # net.ipv4.tcp_keepalive_probes = 5
    sysctl_updates = {
        "net.core.rmem_max": 134217728,  # from 212992
        "net.core.wmem_max": 134217728,  # from 212992
        "net.ipv4.tcp_rmem": "4096 87380 67108864",  # from "4096 131072 6291456"
        "net.ipv4.tcp_wmem": "4096 65536 67108864",  # from "4096 16384 4194304"
        "net.core.somaxconn": 65535,
        "fs.file-max": 1024 * 1024 * 1024,
    }
    if cc == "bbr":
        logger.fs.warning("Using BBR, make sure you indend to!")
        sysctl_updates["net.core.default_qdisc"] = "fq"
        sysctl_updates["net.ipv4.tcp_congestion_control"] = "bbr"
    elif cc == "cubic":
        sysctl_updates["net.ipv4.tcp_congestion_control"] = "cubic"
    else:
        raise ValueError("Unknown congestion control algorithm: {}".format(cc))
    return "sudo sysctl -w {}".format(" ".join(f'"{k}={v}"' for k, v in sysctl_updates.items())).strip()


def make_autoshutdown_script():
    return """#!/bin/bash
TIMEOUTMINUTES=${1:-35}
if [ -f /tmp/autoshutdown.pid ]; then
    (kill -9 $(cat /tmp/autoshutdown.pid) && rm -f /tmp/autoshutdown.pid) || true
fi
(sleep $(($TIMEOUTMINUTES*60)) && sudo poweroff |& tee /tmp/autoshutdown.out) > /dev/null 2>&1 < /dev/null & echo $! > /tmp/autoshutdown.pid"""
