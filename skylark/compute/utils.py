from skylark.utils import logger


def make_netdata_command(port, netdata_hostname=None):
    cmd = """sudo docker run -d --rm --name=netdata \
        -p {port}:19999 \
        -v netdataconfig:/etc/netdata \
        -v netdatalib:/var/lib/netdata \
        -v netdatacache:/var/cache/netdata \
        -v /etc/passwd:/host/etc/passwd:ro \
        -v /etc/group:/host/etc/group:ro \
        -v /proc:/host/proc:ro \
        -v /sys:/host/sys:ro \
        -v /etc/os-release:/host/etc/os-release:ro \
        --cap-add SYS_PTRACE \
        --security-opt apparmor=unconfined \
        {docker_args} netdata/netdata:stable"""
    docker_args = "--hostname={}".format(netdata_hostname) if netdata_hostname else ""
    return cmd.format(port=port, docker_args=docker_args)


def make_dozzle_command(port):
    cmd = """sudo docker run -d --rm --name dozzle \
        -p {log_viewer_port}:8080 \
        --volume=/var/run/docker.sock:/var/run/docker.sock \
        amir20/dozzle:latest"""
    return cmd.format(log_viewer_port=port)


def make_glances_command(port):
    cmd = """sudo docker run -d --rm --name glances '
        -p {port}-{port + 1}:61208-61209 \
        -e GLANCES_OPT='-w' -v /var/run/docker.sock:/var/run/docker.sock:ro \
        --pid host \
        nicolargo/glances:latest-full"""
    return cmd.format(port=port)


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
