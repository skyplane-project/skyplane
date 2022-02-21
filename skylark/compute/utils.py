from skylark.utils import logger


def make_dozzle_command(port):
    cmd = """sudo docker run -d --rm --name dozzle \
        -p {log_viewer_port}:8080 \
        --volume=/var/run/docker.sock:/var/run/docker.sock \
        amir20/dozzle:latest"""
    return cmd.format(log_viewer_port=port)


def make_sysctl_tcp_tuning_command(cc="cubic"):
    sysctl_updates = {
        "net.core.rmem_max": 134217728,  # from 212992
        "net.core.wmem_max": 134217728,  # from 212992
        "net.ipv4.tcp_rmem": "4096 87380 67108864",  # from "4096 131072 6291456"
        "net.ipv4.tcp_wmem": "4096 65536 67108864",  # from "4096 16384 4194304"
        "net.core.somaxconn": 65535,
        "fs.file-max": 1024 * 1024 * 1024,
    }
    if cc == "bbr":
        logger.warning("Using BBR, make sure you indend to!")
        sysctl_updates["net.core.default_qdisc"] = "fq"
        sysctl_updates["net.ipv4.tcp_congestion_control"] = "bbr"
    elif cc == "cubic":
        sysctl_updates["net.ipv4.tcp_congestion_control"] = "cubic"
    else:
        raise ValueError("Unknown congestion control algorithm: {}".format(cc))
    return "sudo sysctl -w {}".format(" ".join(f'"{k}={v}"' for k, v in sysctl_updates.items())).strip()
