import argparse
import json
import time
from datetime import datetime

from loguru import logger

from skylark import skylark_root
from skylark.benchmark.utils import provision
from skylark.compute.aws.aws_cloud_provider import AWSCloudProvider
from skylark.compute.gcp.gcp_cloud_provider import GCPCloudProvider
from skylark.utils.utils import do_parallel


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
    parser.add_argument("--dst_region", default="aws:us-east-2", choices=full_region_list, help="Destination region")
    parser.add_argument("--gateway_docker_image", type=str, default="ghcr.io/parasj/skylark:latest", help="Gateway docker image")
    return parser.parse_args()


def setup(tup):
    server, docker_image = tup
    server.run_command("sudo apt-get update && sudo apt-get install -y iperf3")
    docker_installed = "Docker version" in server.run_command(f"sudo docker --version")[0]
    if not docker_installed:
        logger.debug(f"[{server.region_tag}] Installing docker")
        server.run_command("curl -fsSL https://get.docker.com -o get-docker.sh && sudo sh get-docker.sh")
    out, err = server.run_command("sudo docker run --rm hello-world")
    assert "Hello from Docker!" in out
    server.run_command("sudo docker pull {}".format(docker_image))


def parse_output(output):
    stdout, stderr = output
    last_line = stdout.strip().split("\n")
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
    aws_regions = [r.split(":")[1] for r in [args.src_region, args.dst_region] if r.startswith("aws:")]
    gcp_regions = [r.split(":")[1] for r in [args.src_region, args.dst_region] if r.startswith("gcp:")]
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

    # select servers
    src_cloud_region = args.src_region.split(":")[1]
    if args.src_region.startswith("aws:"):
        src_server = aws_instances[src_cloud_region][0]
    elif args.src_region.startswith("gcp:"):
        src_server = gcp_instances[src_cloud_region][0]
    else:
        raise ValueError(f"Unknown region {args.src_region}")
    dst_cloud_region = args.dst_region.split(":")[1]
    if args.dst_region.startswith("aws:"):
        dst_server = aws_instances[dst_cloud_region][0]
    elif args.dst_region.startswith("gcp:"):
        dst_server = gcp_instances[dst_cloud_region][0]
    else:
        raise ValueError(f"Unknown region {args.dst_region}")
    do_parallel(
        setup,
        [(src_server, args.gateway_docker_image), (dst_server, args.gateway_docker_image)],
        progress_bar=True,
        arg_fmt=lambda tup: tup[0].region_tag,
    )

    # generate random 1GB file on src server in /dev/shm/skylark/chunks_in
    src_server.run_command("mkdir -p /dev/shm/skylark/chunks_in")
    src_server.run_command("sudo dd if=/dev/urandom of=/dev/shm/skylark/chunks_in/1 bs=100M count=10 iflag=fullblock")
    assert src_server.run_command("ls /dev/shm/skylark/chunks_in/1 | wc -l")[0].strip() == "1"

    # stop existing gateway containers
    src_server.run_command("sudo docker kill gateway_server")
    dst_server.run_command("sudo docker kill gateway_server")

    # start gateway on dst server
    dst_ip = dst_server.run_command("dig +short myip.opendns.com @resolver1.opendns.com")[0].strip()
    server_cmd = f"sudo docker run -d --rm --ipc=host --network=host --name=gateway_server {args.gateway_docker_image} /env/bin/python /pkg/skylark/replicate/gateway_server.py --port 3333 --num_connections 1"
    dst_server.run_command(server_cmd)

    # wait for port to appear on dst server
    while True:
        if dst_server.run_command("sudo netstat -tulpn | grep 3333")[0].strip() != "":
            break
        time.sleep(1)

    # benchmark src to dst copy
    client_cmd = f"sudo docker run --rm --ipc=host --network=host --name=gateway_client {args.gateway_docker_image} /env/bin/python /pkg/skylark/replicate/gateway_client.py --dst_host {dst_server.public_ip} --dst_port 3333 --chunk_id 1"
    dst_data = parse_output(src_server.run_command(client_cmd))
    src_server.run_command("sudo docker kill gateway_server")
    logger.info(f"Src to dst copy: {dst_data}")

    # run iperf server on dst
    out, err = dst_server.run_command("iperf3 -s -D")
    out, err = src_server.run_command("iperf3 -c {} -t 10".format(dst_server.public_ip))
    dst_server.run_command("sudo pkill iperf3")
    print(out)


if __name__ == "__main__":
    args = parse_args()
    main(args)
