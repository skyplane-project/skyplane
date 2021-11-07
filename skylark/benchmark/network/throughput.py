import argparse
import json
from pathlib import Path
import pickle
from typing import List, Tuple

from loguru import logger

from skylark import skylark_root
from skylark.compute.aws.aws_cloud_provider import AWSCloudProvider
from skylark.compute.cloud_providers import CloudProvider
from skylark.compute.gcp.gcp_cloud_provider import GCPCloudProvider
from skylark.compute.server import Server, ServerState
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

    parser.add_argument(
        "--setup_script",
        type=str,
        default=None,
        help="Setup script to run on each instance (URL), optional",
    )

    parser.add_argument(
        "--iperf_connection_list",
        type=int,
        nargs="+",
        default=[128],
        help="List of connections to test",
    )
    parser.add_argument("--iperf3_runtime", type=int, default=4, help="Runtime for iperf3 in seconds")
    args = parser.parse_args()
    logger.info(f"Arguments: {args}")
    return args


def refresh_instance_list(provider: CloudProvider, region_list, filter={"tags": {"skylark": "true"}}):
    instances = {}
    fn = lambda region: provider.get_matching_instances(region, **filter)
    results = do_parallel(fn, region_list, progress_bar=True)
    for region, instance_list in results:
        if instance_list:
            instances[region] = instance_list
    return instances


def split_list(l):
    pairs = set(l)
    groups = []
    elems_in_last_group = set()
    while pairs:
        group = []
        for x, y in pairs:
            if x not in elems_in_last_group and y not in elems_in_last_group:
                group.append((x, y))
                elems_in_last_group.add(x)
                elems_in_last_group.add(y)
        groups.append(group)
        elems_in_last_group = set()
        pairs -= set(group)
    return groups


def main(args):
    data_dir = Path(__file__).parent.parent / "data"
    log_dir = data_dir / "logs"
    log_dir.mkdir(exist_ok=True, parents=True)

    gcp_private_key = str(skylark_root / "data" / "keys" / "gcp.pem")
    gcp_public_key = str(skylark_root / "data" / "keys" / "gcp.pub")

    aws = AWSCloudProvider()
    gcp = GCPCloudProvider(args.gcp_project, gcp_private_key, gcp_public_key)
    aws_instance_filter = {
        "tags": {"skylark": "true"},
        "instance_type": args.aws_instance_class,
        "state": [ServerState.PENDING, ServerState.RUNNING],
    }
    gcp_instance_filter = {
        "tags": {"skylark": "true"},
        "instance_type": args.gcp_instance_class,
        "state": [ServerState.PENDING, ServerState.RUNNING],
    }

    # setup
    logger.info("(aws) configuring security group")
    do_parallel(aws.add_ip_to_security_group, args.aws_region_list, progress_bar=True)
    logger.info("(gcp) creating ssh key")
    gcp.create_ssh_key()
    logger.info("(gcp) configuring firewall")
    gcp.configure_default_firewall()

    # regions to provision
    logger.info("refreshing region list")
    aws_instances = refresh_instance_list(aws, args.aws_region_list, aws_instance_filter)
    gcp_instances = refresh_instance_list(gcp, args.gcp_region_list, gcp_instance_filter)

    # provision missing regions using do_parallel
    missing_aws_regions = [r for r in args.aws_region_list if r not in aws_instances]
    missing_gcp_regions = [r for r in args.gcp_region_list if r not in gcp_instances]
    if missing_aws_regions:
        logger.info(f"(aws) provisioning missing regions: {missing_aws_regions}")
        aws_provisioner = lambda region: aws.provision_instance(region, args.aws_instance_class)
        results = do_parallel(aws_provisioner, missing_aws_regions, progress_bar=True)
        for region, result in results:
            aws_instances[region] = [result]
            logger.info(f"(aws:{region}) provisioned {result}, waiting for ready")
            result.wait_for_ready()
            logger.info(f"(aws:{region}) ready")
        aws_instances = refresh_instance_list(aws, args.aws_region_list, aws_instance_filter)

    if missing_gcp_regions:
        logger.info(f"(gcp) provisioning missing regions: {missing_gcp_regions}")
        gcp_provisioner = lambda region: gcp.provision_instance(region, args.gcp_instance_class)
        results = do_parallel(gcp_provisioner, missing_gcp_regions, progress_bar=True)
        for region, result in results:
            gcp_instances[region] = [result]
            logger.info(f"(gcp:{region}) provisioned {result}")
        gcp_instances = refresh_instance_list(gcp, args.gcp_region_list, gcp_instance_filter)

    # run setup script on each instance (use do_parallel)
    if args.setup_script:

        def run_setup_script(instance: Server):
            dest_path = str("~/scripts/latency/" + Path(args.setup_script).name)
            stdout, stderr = instance.run_command("mkdir -p ~/scripts/latency")
            print(stderr)
            instance.copy_file(args.setup_script, dest_path)
            instance.run_command(f"chmod +x {dest_path}")
            stdout, stderr = instance.run_command(f"{dest_path}")
            return stdout, stderr

        do_parallel(run_setup_script, [i for _, ilist in aws_instances.items() for i in ilist], progress_bar=True)
        do_parallel(run_setup_script, [i for _, ilist in gcp_instances.items() for i in ilist], progress_bar=True)

    # kill (if any) iperf3 instances on the server then start iperf3 server on each instance
    def stop_iperf3_server(instance: Server):
        stdout, stderr = instance.run_command("pkill iperf3")
        return stdout, stderr

    def start_iperf3_server(instance: Server):
        stdout, stderr = instance.run_command("iperf3 -s")
        return stdout, stderr

    logger.info("Starting iperf3 server on each instance")
    do_parallel(stop_iperf3_server, [i for _, ilist in aws_instances.items() for i in ilist], progress_bar=True)
    do_parallel(stop_iperf3_server, [i for _, ilist in gcp_instances.items() for i in ilist], progress_bar=True)
    do_parallel(start_iperf3_server, [i for _, ilist in aws_instances.items() for i in ilist], progress_bar=True)
    do_parallel(start_iperf3_server, [i for _, ilist in gcp_instances.items() for i in ilist], progress_bar=True)

    # start iperf3 clients on each pair of instances
    def start_iperf3_client(arg_pair: Tuple[Server, Server]):
        instance_src, instance_dst = arg_pair
        src_ip, dst_ip = instance_src.public_ip, instance_dst.public_ip
        logger.info(f"Computing throughput from {src_ip} to {dst_ip}")
        stdout, stderr = instance_src.run_command(f"iperf3 -J -t {args.iperf3_runtime} -P 128 -c {dst_ip}")
        try:
            result = json.loads(stdout)
        except json.JSONDecodeError:
            logger.info(f"iperf3 client failed: {stdout} {stderr}")
            return None
        throughput = result["end"]["sum_sent"]["bits_per_second"]
        logger.info(f"({instance_src.region_tag} -> {instance_dst.region_tag}) is {throughput}")
        return throughput

    instance_list = [i for _, ilist in aws_instances.items() for i in ilist] + [i for _, ilist in gcp_instances.items() for i in ilist]
    instance_pairs = [(i1, i2) for i1 in instance_list for i2 in instance_list if i1 != i2]
    groups = split_list(instance_pairs)

    # run benchmark in parallel using do_parallel for each group
    def run_benchmark(group: List[Tuple[Server, Server]]):
        return do_parallel(start_iperf3_client, group, progress_bar=True)

    throughput_results = {}
    for group in groups:
        pair_str = ", ".join([f"{i1.region_tag}->{i2.region_tag}" for i1, i2 in group])
        logger.info(f"Running benchmark for {len(group)} instances, [{pair_str}]")
        results = do_parallel(run_benchmark, [group], progress_bar=True)
        for pair, result in results:
            key = (pair[0].region_tag, pair[1].region_tag)
            throughput_results[key] = result

    # stop iperf3 server on each instance
    do_parallel(stop_iperf3_server, [i for _, ilist in aws_instances.items() for i in ilist], progress_bar=True)
    do_parallel(stop_iperf3_server, [i for _, ilist in gcp_instances.items() for i in ilist], progress_bar=True)

    # save results
    logger.info("Saving results")
    with open(str(data_dir / "throughput.pickle"), "wb") as f:
        pickle.dump(throughput_results, f)


if __name__ == "__main__":
    args = parse_args()
    main(args)
