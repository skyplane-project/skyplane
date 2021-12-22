import os
import argparse
from datetime import datetime
import json
import re
import time
from typing import List, Tuple

from loguru import logger
from tqdm import tqdm

from skylark import skylark_root
from skylark.benchmark.utils import provision, split_list
from skylark.compute.aws.aws_cloud_provider import AWSCloudProvider
from skylark.compute.aws.aws_server import AWSServer
from skylark.compute.gcp.gcp_cloud_provider import GCPCloudProvider
from skylark.compute.gcp.gcp_server import GCPServer
from skylark.compute.server import Server
from skylark.utils import do_parallel


def parse_args():
    full_region_list = []
    full_region_list += [f"aws:{r}" for r in AWSCloudProvider.region_list()]
    full_region_list += [f"gcp:{r}" for r in GCPCloudProvider.region_list()]
    parser = argparse.ArgumentParser(description="Test throughput with Skylark Gateway")
    parser.add_argument("--aws_instance_class", type=str, default="c5.4xlarge", help="Instance class")
    parser.add_argument("--gcp_instance_class", type=str, default="n1-highcpu-8", help="Instance class")
    parser.add_argument("--gcp_project", type=str, default="skylark-333700", help="GCP project")
    parser.add_argument(
        "--gcp_test_standard_network", action="store_true", help="Test GCP standard network in addition to premium (default)"
    )
    parser.add_argument("--src_region", default="aws:us-east-1", choices=full_region_list, help="Source region")
    parser.add_argument("--dst_region", default="aws:us-west-1", choices=full_region_list, help="Destination region")
    parser.add_argument("--gateway_docker_image", type=str, default="ghcr.io/parasj/skylark-docker:latest", help="Gateway docker image")
    return parser.parse_args()

def setup(tup):
    server, docker_image = tup
    docker_installed = 'Docker version' in server.run_command(f"sudo docker --version")[0]
    if not docker_installed:
        logger.debug(f"[{server.region_tag}] Installing docker")
        server.run_command("curl -fsSL https://get.docker.com -o get-docker.sh && sudo sh get-docker.sh")
    out, err = server.run_command("sudo docker run hello-world")
    assert "Hello from Docker!" in out
    server.run_command("sudo docker pull {}".format(docker_image))

def parse_output(output):
    stdout, stderr = output
    last_line = stdout.strip().split('\n')
    if len(last_line) > 0:
        try:
            return json.loads(last_line[-1])
        except json.decoder.JSONDecodeError:
            logger.error(f"JSON parse error, stdout = '{stdout}', stderr = '{stderr}'")
    else:
        logger.error(f"No output from server, stderr = {stderr}")
        return None

def main(args):
    data_dir = skylark_root / "data"
    log_dir = data_dir / "logs" / "gateway_test" / datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_dir.mkdir(exist_ok=True, parents=True)

    aws = AWSCloudProvider()
    gcp = GCPCloudProvider(args.gcp_project)

    # provision and setup servers
    aws_regions = [r.split(':')[1] for r in [args.src_region, args.dst_region] if r.startswith("aws:")]
    gcp_regions = [r.split(':')[1] for r in [args.src_region, args.dst_region] if r.startswith("gcp:")]
    aws_instances, gcp_instances = provision(
        aws=aws,
        gcp=gcp,
        aws_regions_to_provision=aws_regions,
        gcp_regions_to_provision=gcp_regions,
        aws_instance_class=args.aws_instance_class,
        gcp_instance_class=args.gcp_instance_class,
        gcp_use_premium_network=not args.gcp_test_standard_network,
        log_dir=str(log_dir),
    )
    src_server = aws_instances[args.src_region.split(':')[1]][0] if args.src_region.startswith("aws:") else gcp_instances[args.src_region.split(':')[1]][0]
    dst_server = aws_instances[args.dst_region.split(':')[1]][0] if args.dst_region.startswith("aws:") else gcp_instances[args.dst_region.split(':')[1]][0]
    do_parallel(setup, [(src_server, args.gateway_docker_image), (dst_server, args.gateway_docker_image)], progress_bar=True)

    # generate random 1GB file on src server in /dev/shm/skylark/chunks_in
    src_server.run_command("mkdir -p /dev/shm/skylark/chunks_in")
    src_server.run_command("sudo dd if=/dev/urandom of=/dev/shm/skylark/chunks_in/1 bs=1G count=1 iflag=fullblock")
    assert src_server.run_command("ls /dev/shm/skylark/chunks_in/1 | wc -l")[0].strip() == "1"
    
    dst_server.run_command("mkdir -p /dev/shm/skylark/chunks_in")
    dst_server.run_command("sudo dd if=/dev/urandom of=/dev/shm/skylark/chunks_in/1 bs=1G count=1 iflag=fullblock")
    assert dst_server.run_command("ls /dev/shm/skylark/chunks_in/1 | wc -l")[0].strip() == "1"

    # stop existing gateway containers
    src_server.run_command("sudo docker stop $(sudo docker ps -q)")
    dst_server.run_command("sudo docker stop $(sudo docker ps -q)")
    time.sleep(1)

    # start gateway on dst server    
    gateway_cmd_tmpl = "sudo docker run --rm --init --ipc host -p 8050-8150:8050-8150 {image} /env/bin/python -u {cmd}"
    server_cmd = "/pkg/skylark/replicate/gateway_server.py"
    server_cmd = gateway_cmd_tmpl.format(image=args.gateway_docker_image, cmd=server_cmd)
    server_cmd = f"({server_cmd}) &> /tmp/gateway_server.log &"
    logger.info(f"[{dst_server.region_tag}, {dst_server.public_ip}] Starting gateway server")
    logger.debug(f"[{dst_server.region_tag}, {dst_server.public_ip}] {server_cmd}")
    dst_server.run_command(server_cmd)
    time.sleep(1)
    while True:
        server_log, err = dst_server.run_command("cat /tmp/gateway_server.log")
        if "Listening on port" in server_log:
            break
        time.sleep(1)
    
    # benchmark src to dst copy
    gateway_cmd_tmpl = "sudo docker run --rm --init --ipc host {image} /env/bin/python -u {cmd}"
    client_cmd = f"/pkg/skylark/replicate/gateway_client.py --dst_host {dst_server.public_ip} --dst_port 8100 --chunk_id 1"
    client_cmd = gateway_cmd_tmpl.format(image=args.gateway_docker_image, cmd=client_cmd)
    logger.info(f"[{src_server.region_tag}, {src_server.public_ip}] Starting gateway client")
    logger.debug(f"[{src_server.region_tag}, {src_server.public_ip}] {client_cmd}")

    # assert     
    dst_data = parse_output(src_server.run_command(client_cmd))
    logger.info(f"Src to dst copy: {dst_data}")

    dst_server.run_command("sudo docker stop $(sudo docker ps -q)")

    server_log = src_server.run_command("cat /tmp/gateway_server.log")[0]
    logger.warning(f"Server log:\n{server_log}")

if __name__ == "__main__":
    args = parse_args()
    main(args)