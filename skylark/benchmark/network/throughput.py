import argparse
import json
from pathlib import Path
from typing import Dict, List, Tuple

from loguru import logger
from tqdm import tqdm

from skylark import skylark_root
from skylark.benchmark.utils import provision
from skylark.compute.aws.aws_cloud_provider import AWSCloudProvider
from skylark.compute.aws.aws_server import AWSServer
from skylark.compute.gcp.gcp_cloud_provider import GCPCloudProvider
from skylark.compute.gcp.gcp_server import GCPServer
from skylark.compute.server import Server
from skylark.utils import do_parallel


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
    parser.add_argument("--setup_script", type=str, default=None, help="Setup script to run on each instance (URL), optional")
    parser.add_argument("--iperf_connection_list", type=int, nargs="+", default=[128], help="List of connections to test")
    parser.add_argument("--iperf3_runtime", type=int, default=4, help="Runtime for iperf3 in seconds")
    return parser.parse_args()


def main(args):
    data_dir = Path(__file__).parent.parent / "data"
    log_dir = data_dir / "logs"
    log_dir.mkdir(exist_ok=True, parents=True)

    gcp_private_key = str(skylark_root / "data" / "keys" / "gcp-cert.pem")
    gcp_public_key = str(skylark_root / "data" / "keys" / "gcp-cert.pub")

    aws = AWSCloudProvider()
    gcp = GCPCloudProvider(args.gcp_project, gcp_private_key, gcp_public_key)
    aws_instances: dict[str, list[AWSServer]]
    gcp_instances: dict[str, list[GCPServer]]
    aws_instances, gcp_instances = provision(
        aws=aws,
        gcp=gcp,
        aws_regions_to_provision=args.aws_region_list,
        gcp_regions_to_provision=args.gcp_region_list,
        aws_instance_class=args.aws_instance_class,
        gcp_instance_class=args.gcp_instance_class,
        setup_script=args.setup_script,
    )
    instance_list: List[Server] = [i for ilist in aws_instances.values() for i in ilist]
    instance_list.extend([i for ilist in gcp_instances.values() for i in ilist])

    # compute pairwise latency by running ping
    def compute_latency(arg_pair: Tuple[Server, Server]) -> str:
        instance_src, instance_dst = arg_pair
        src_ip, dst_ip = instance_src.public_ip, instance_dst.public_ip
        stdout, stderr = instance_src.run_command(f"ping -c 10 {dst_ip}")
        latency_result = stdout.strip().split("\n")[-1]
        tqdm.write(f"Latency from {instance_src.region_tag} to {instance_dst.region_tag} is {latency_result}")
        return latency_result

    instance_pairs = [(i1, i2) for i1 in instance_list for i2 in instance_list if i1 != i2]
    latency_results = do_parallel(compute_latency, instance_pairs, progress_bar=True, n=24)

    # save results
    latency_results_dict = {}
    for (i1, i2), r in latency_results:
        if i1 not in latency_results_dict:
            latency_results_dict[i1.region_tag] = {}
        latency_results_dict[i1.region_tag][i2.region_tag] = r
    with open(str(data_dir / "latency.json"), "w") as f:
        json.dump(latency_results_dict, f)


if __name__ == "__main__":
    main(parse_args())
