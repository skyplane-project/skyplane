import argparse
from datetime import datetime
import json
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

# traceroute parser from https://github.com/ckreibich/tracerouteparser.py
from tracerouteparser import TracerouteParser

def parse_args():
    aws_regions = AWSCloudProvider.region_list()
    gcp_regions = GCPCloudProvider.region_list()
    parser = argparse.ArgumentParser(description="Provision EC2 instances")
    parser.add_argument("--aws_instance_class", type=str, default="i3en.large", help="Instance class")
    parser.add_argument("--aws_region_list", type=str, nargs="*", default=aws_regions)

    parser.add_argument("--gcp_project", type=str, default="bair-commons-307400", help="GCP project")
    parser.add_argument("--gcp_instance_class", type=str, default="n1-highcpu-8", help="Instance class")
    parser.add_argument("--gcp_region_list", type=str, nargs="*", default=gcp_regions)
    parser.add_argument(
        "--gcp_test_standard_network", action="store_true", help="Test GCP standard network in addition to premium (default)"
    )

    parser.add_argument("--setup_script", type=str, default=None, help="Setup script to run on each instance (URL), optional")
    parser.add_argument("--iperf_connection_list", type=int, nargs="+", default=[128], help="List of connections to test")
    parser.add_argument("--iperf3_runtime", type=int, default=4, help="Runtime for iperf3 in seconds")
    parser.add_argument("--iperf3_congestion", type=str, default="cubic", help="Congestion control algorithm for iperf3")
    args = parser.parse_args()

    # filter by valid regions
    args.aws_region_list = [r for r in args.aws_region_list if r in aws_regions]
    args.gcp_region_list = [r for r in args.gcp_region_list if r in gcp_regions]

    return args


def main(args):
    data_dir = skylark_root / "data"
    log_dir = data_dir / "logs" / "throughput" / datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_dir.mkdir(exist_ok=True, parents=True)

    aws = AWSCloudProvider()
    gcp = GCPCloudProvider(args.gcp_project)
    aws_instances, gcp_instances = provision(
        aws=aws,
        gcp=gcp,
        aws_regions_to_provision=args.aws_region_list,
        gcp_regions_to_provision=args.gcp_region_list,
        aws_instance_class=args.aws_instance_class,
        gcp_instance_class=args.gcp_instance_class,
        gcp_use_premium_network=True,
        setup_script=args.setup_script,
        log_dir=str(log_dir),
    )
    instance_list: List[Server] = [i for ilist in aws_instances.values() for i in ilist]
    instance_list.extend([i for ilist in gcp_instances.values() for i in ilist])
    if args.gcp_test_standard_network:
        logger.info(f"Provisioning standard GCP instances")
        _, gcp_standard_instances = provision(
            aws=aws,
            gcp=gcp,
            aws_regions_to_provision=[],
            gcp_regions_to_provision=args.gcp_region_list,
            aws_instance_class=args.aws_instance_class,
            gcp_instance_class=args.gcp_instance_class,
            gcp_use_premium_network=False,
            setup_script=args.setup_script,
            log_dir=str(log_dir),
        )
        instance_list.extend([i for ilist in gcp_standard_instances.values() for i in ilist])

    def setup(instance: Server):
        instance.run_command("sudo apt-get update")
        instance.run_command("sudo apt-get install traceroute")

    do_parallel(setup, instance_list, progress_bar=True, n=24, desc="Setup")

    # run traceroute on each pair of instances
    def run_traceroute(arg_pair: Tuple[Server, Server]):
        instance_src, instance_dst = arg_pair
        src_ip, dst_ip = instance_src.public_ip, instance_dst.public_ip
        logger.info(f"traceroute {instance_src.region_tag} -> {instance_dst.region_tag} ({dst_ip})")
        stdout, stderr = instance_src.run_command(f"traceroute {dst_ip}")

        parser = TracerouteParser()
        parser.parse_data(stdout)
        result = {}
        for idx in range(len(parser.hops)):
            result[idx] = []
            for probe in parser.hops[idx].probes: 
                result[idx].append({"ipaddr": probe.ipaddr, "name": probe.name, "rtt": probe.rtt})
        tqdm.write(
                f"({instance_src.region_tag}:{instance_src.network_tier} -> {instance_dst.region_tag}:{instance_dst.network_tier}) hops: {len(result.keys())}"
        )
        instance_src.close_server()
        instance_dst.close_server()
       
        return result

    traceroute_results = []
    instance_pairs = [(i1, i2) for i1 in instance_list for i2 in instance_list if i1 != i2]
    with tqdm(total=len(instance_pairs), desc="Total traceroute evaluation") as pbar:
        groups = split_list(instance_pairs)
        for group_idx, group in enumerate(groups):
            results = do_parallel(
                run_traceroute,
                group,
                progress_bar=True,
                desc=f"Parallel eval group {group_idx}",
                n=36,
                arg_fmt=lambda x: f"{x[0].region_tag}:{x[0].network_tier} to {x[1].region_tag}:{x[1].network_tier}",
            )
            for pair, result in results:
                pbar.update(1)
                result_rec = {}
                result_rec["congestion"] = args.iperf3_congestion
                result_rec["src"] = pair[0].region_tag
                result_rec["dst"] = pair[1].region_tag
                result_rec["src_instance_class"] = pair[0].instance_class
                result_rec["dst_instance_class"] = pair[1].instance_class
                result_rec["src_network_tier"] = pair[0].network_tier
                result_rec["dst_network_tier"] = pair[1].network_tier
                result_rec["result"] = result
                traceroute_results.append(result_rec)

    traceroute_dir = data_dir / "traceroute" / "iperf3"
    traceroute_dir.mkdir(exist_ok=True, parents=True)
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    with open(str(throughput_dir / f"traceroute_{timestamp}.json"), "w") as f:
        json.dump(throughput_results, f)


if __name__ == "__main__":
    main(parse_args())
