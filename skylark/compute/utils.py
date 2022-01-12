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
