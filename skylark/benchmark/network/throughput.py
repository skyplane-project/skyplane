import argparse
import json
from datetime import datetime
from typing import List, Tuple
import re

from loguru import logger
from tqdm import tqdm

from skylark import skylark_root
from skylark.benchmark.utils import provision, split_list
from skylark.compute.aws.aws_cloud_provider import AWSCloudProvider
from skylark.compute.gcp.gcp_cloud_provider import GCPCloudProvider
from skylark.compute.server import Server
from skylark.utils import do_parallel


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
    parser.add_argument("--iperf3_mode", type=str, default="tcp", help="Mode for iperf3")
    args = parser.parse_args()

    # filter by valid regions
    args.aws_region_list = [r for r in args.aws_region_list if r in aws_regions]
    args.gcp_region_list = [r for r in args.gcp_region_list if r in gcp_regions]

    return args


def main(args):
    data_dir = skylark_root / "data"
    log_dir = data_dir / "logs" / f"throughput_{args.iperf3_mode}" / datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
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
        instance.run_command("(sudo apt-get update && sudo apt-get install -y iperf3 nuttcp); pkill iperf3 nuttcp")
        if args.iperf3_mode == "tcp":
            instance.run_command("iperf3 -s -D -J")
        else:
            instance.run_command("nuttcp -S -u -w4m -l1460")
        if args.iperf3_congestion == "bbr":
            instance.run_command(
                "sudo sysctl -w net.ipv4.tcp_congestion_control=bbr; sudo sysctl -w net.core.default_qdisc=fq; sudo sysctl -w net.ipv4.tcp_available_congestion_control=bbr"
            )

    do_parallel(setup, instance_list, progress_bar=True, n=24, desc="Setup")

    # start iperf3 clients on each pair of instances
    def start_iperf3_client(arg_pair: Tuple[Server, Server]):
        instance_src, instance_dst = arg_pair
        if args.iperf3_mode == "tcp":
            stdout, stderr = instance_src.run_command(
                f"iperf3 -J -Z -C {args.iperf3_congestion} -t {args.iperf3_runtime} -P 32 -c {instance_dst.public_ip()}"
            )
            try:
                result = json.loads(stdout)
            except json.JSONDecodeError:
                logger.error(f"({instance_src.region_tag} -> {instance_dst.region_tag}) iperf3 client failed: {stdout} {stderr}")
                return None

            throughput_sent = result["end"]["sum_sent"]["bits_per_second"]
            throughput_received = result["end"]["sum_received"]["bits_per_second"]
            cpu_utilization = result["end"]["cpu_utilization_percent"]["host_total"]
            tqdm.write(
                f"({instance_src.region_tag}:{instance_src.network_tier()} -> {instance_dst.region_tag}:{instance_dst.network_tier()}) is {throughput_sent / 1e9:0.2f} Gbps"
            )
            out_rec = dict(throughput_sent=throughput_sent, throughput_received=throughput_received, cpu_utilization=cpu_utilization)
        elif args.iperf3_mode == "udp":
            stdout, stderr = instance_src.run_command(f"nuttcp -uu -l1460 -w4m -T {args.iperf3_runtime} {instance_dst.public_ip()}")
            # example return:
            # "  659.1631 MB /   2.00 sec = 2764.6834 Mbps 99 %TX 53 %RX 36045 / 711028 drop/pkt 5.07 %loss"
            # "   208.2891 MB /   4.00 sec =  436.7620 Mbps 11 %TX 8 %RX 0 / 213288 drop/pkt 0.00 %loss"
            # " 1490.9326 MB /   4.00 sec = 3126.9937 Mbps 99 %TX 55 %RX 179 / 1526894 drop/pkt 0.01172 %loss"
            # returns: data_sent, runtime, throughput, cpu_tx, cpu_rx, dropped_packets, total_packets, loss_percent
            regex = r"\s*(?P<data_sent>\d+\.\d+)\s*MB\s*/\s*(?P<runtime>\d+\.\d+)\s*sec\s*=\s*(?P<throughput_mbps>\d+\.\d+)\s*Mbps\s*(?P<cpu_tx>\d+)\s*%TX\s*(?P<cpu_rx>\d+)\s*%RX\s*(?P<dropped_packets>\d+)\s*\/\s*(?P<total_packets>\d+)\s*drop/pkt\s*(?P<loss_percent>\d+\.?\d*)\s*%loss"
            match = re.search(regex, stdout)
            if match is None:
                logger.error(f"({instance_src.region_tag} -> {instance_dst.region_tag}) nuttcp client failed: {stdout} {stderr}")
                return None
            else:
                out_rec = match.groupdict()
                out_rec["throughput_mbps"] = float(out_rec["throughput_mbps"])
                out_rec["cpu_tx"] = float(out_rec["cpu_tx"])
                out_rec["cpu_rx"] = float(out_rec["cpu_rx"])
                out_rec["dropped_packets"] = float(out_rec["dropped_packets"])
                out_rec["total_packets"] = float(out_rec["total_packets"])
                out_rec["loss_percent"] = float(out_rec["loss_percent"])
                tqdm.write(
                    f"({instance_src.region_tag}:{instance_src.network_tier()} -> {instance_dst.region_tag}:{instance_dst.network_tier()}) is {out_rec['throughput_mbps'] / 1e3:0.2f} Gbps w/ loss {out_rec['loss_percent']:0.2f}%"
                )
        instance_src.close_server()
        instance_dst.close_server()
        return out_rec

    throughput_results = []
    instance_pairs = [(i1, i2) for i1 in instance_list for i2 in instance_list if i1 != i2]
    with tqdm(total=len(instance_pairs), desc="Total throughput evaluation") as pbar:
        groups = split_list(instance_pairs)
        for group_idx, group in enumerate(groups):
            results = do_parallel(
                start_iperf3_client,
                group,
                progress_bar=True,
                desc=f"Parallel eval group {group_idx}",
                n=36,
                arg_fmt=lambda x: f"{x[0].region_tag}:{x[0].network_tier()} to {x[1].region_tag}:{x[1].network_tier()}",
            )
            for pair, result in results:
                pbar.update(1)
                result_rec = {
                    "congestion": args.iperf3_congestion,
                    "src": pair[0].region_tag,
                    "dst": pair[1].region_tag,
                    "src_instance_class": pair[0].instance_class(),
                    "dst_instance_class": pair[1].instance_class(),
                    "src_network_tier": pair[0].network_tier(),
                    "dst_network_tier": pair[1].network_tier(),
                }
                if result is not None:
                    result_rec.update(result)
                throughput_results.append(result_rec)

    throughput_dir = data_dir / "throughput" / "iperf3"
    throughput_dir.mkdir(exist_ok=True, parents=True)
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    with open(str(throughput_dir / f"throughput_{timestamp}.json"), "w") as f:
        json.dump(throughput_results, f)


if __name__ == "__main__":
    main(parse_args())
