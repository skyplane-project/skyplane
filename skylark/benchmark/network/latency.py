import argparse
import json
import re
from pathlib import Path
from typing import List, Tuple

from loguru import logger
from tqdm import tqdm

from skylark import skylark_root
from skylark.benchmark.utils import provision
from skylark.compute.aws.aws_cloud_provider import AWSCloudProvider
from skylark.compute.aws.aws_server import AWSServer
from skylark.compute.gcp.gcp_cloud_provider import GCPCloudProvider
from skylark.compute.gcp.gcp_server import GCPServer
from skylark.compute.server import Server
from skylark.utils.utils import do_parallel


def parse_args():
    aws_regions = AWSCloudProvider.region_list()
    gcp_regions = GCPCloudProvider.region_list()
    parser = argparse.ArgumentParser(description="Provision EC2 instances")
    parser.add_argument("--aws_instance_class", type=str, default="i3en.large", help="Instance class")
    parser.add_argument("--aws_region_list", type=str, nargs="+", default=aws_regions)
    parser.add_argument("--gcp_instance_class", type=str, default="n1-highcpu-8", help="Instance class")
    parser.add_argument("--use-premium-network", action="store_true", help="Use premium network")
    parser.add_argument("--gcp_project", type=str, default="bair-commons-307400", help="GCP project")
    parser.add_argument("--gcp_region_list", type=str, nargs="+", default=gcp_regions)
    return parser.parse_args()


def main(args):
    data_dir = skylark_root / "data"
    log_dir = data_dir / "logs"
    log_dir.mkdir(exist_ok=True, parents=True)

    aws = AWSCloudProvider()
    gcp = GCPCloudProvider(args.gcp_project)
    aws_instances: dict[str, list[AWSServer]]
    gcp_instances: dict[str, list[GCPServer]]
    aws_instances, gcp_instances = provision(
        aws=aws,
        gcp=gcp,
        aws_regions_to_provision=args.aws_region_list,
        gcp_regions_to_provision=args.gcp_region_list,
        aws_instance_class=args.aws_instance_class,
        gcp_instance_class=args.gcp_instance_class,
    )
    instance_list: List[Server] = [i for ilist in aws_instances.values() for i in ilist]
    instance_list.extend([i for ilist in gcp_instances.values() for i in ilist])

    # compute pairwise latency by running ping
    def compute_latency(arg_pair: Tuple[Server, Server]) -> str:
        instance_src, instance_dst = arg_pair
        stdout, stderr = instance_src.run_command(f"ping -c 10 {instance_dst.public_ip()}")
        latency_result = stdout.strip().split("\n")[-1]
        tqdm.write(f"Latency from {instance_src.region_tag} to {instance_dst.region_tag} is {latency_result}")
        return latency_result

    instance_pairs = [(i1, i2) for i1 in instance_list for i2 in instance_list if i1 != i2]
    latency_results = do_parallel(
        compute_latency,
        instance_pairs,
        progress_bar=True,
        n=24,
        desc="Latency",
        arg_fmt=lambda x: f"{x[0].region_tag} to {x[1].region_tag}",
    )

    def parse_ping_result(string):
        """make regex with named groups"""
        try:
            regex = r"rtt min/avg/max/mdev = (?P<min>\d+\.\d+)/(?P<avg>\d+\.\d+)/(?P<max>\d+\.\d+)/(?P<mdev>\d+\.\d+) ms"
            m = re.search(regex, string)
            return dict(min=float(m.group("min")), avg=float(m.group("avg")), max=float(m.group("max")), mdev=float(m.group("mdev")))
        except Exception as e:
            logger.exception(e)
            return {}

    # save results
    latency_results_out = []
    for (i1, i2), r in latency_results:
        row = dict(src=i1.region_tag, dst=i2.region_tag, ping_str=r, **parse_ping_result(r))
        logger.info(row)
        latency_results_out.append(row)

    with open(str(data_dir / "latency.json"), "w") as f:
        json.dump(latency_results_out, f)


if __name__ == "__main__":
    main(parse_args())
