"""
provision_instances.py: script to provision

In each of region_list (default is [us-east-1, us-east-2, us-west-1, af-south-1, ap-east-1])
provision an EC2 instance instance_class (default c5.large) with an AMI read from a list indexed by region.
SSH into each instance, download a setup script (script argument) and run it on each instance.
"""

import argparse
import json
import os
import pickle
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import boto3
import paramiko
from loguru import logger
from tqdm import tqdm


def do_parallel(func, args_list, n=6):
    """Run list of jobs in parallel with tqdm progress bar"""
    results = []
    with tqdm(total=len(args_list), leave=False) as pbar:
        with ThreadPoolExecutor(max_workers=n) as executor:
            future_list = [executor.submit(func, args) for args in args_list]
            for args, future in zip(args_list, as_completed(future_list)):
                pbar.set_description(str(args))
                pbar.update()
                results.append(future.result())
    return results


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


def get_ubuntu_ami_id(region):
    """
    Using boto3 api, lookup AMI ID for Ubuntu 18.04 amd64 hvm:ebs-ssd from the ubuntu account (099720109477) for a given region.
    Select the image that was built the latest (date at end of AMI name.
    """
    client = boto3.client("ec2", region_name=region)
    response = client.describe_images(
        Filters=[
            {
                "Name": "name",
                "Values": [
                    "ubuntu/images/hvm-ssd/ubuntu-bionic-18.04-amd64-server-*",
                ],
            },
            {
                "Name": "owner-id",
                "Values": [
                    "099720109477",
                ],
            },
        ]
    )
    if len(response["Images"]) == 0:
        print("No AMI found for region {}".format(region))
        sys.exit(1)
    else:
        # Sort the images by date and return the last one
        image_list = sorted(response["Images"], key=lambda x: x["CreationDate"], reverse=True)
        return (region, image_list[0]["ImageId"])


def provision_instance_wrapper(args):
    region, ami_id, instance_class, instance_prefix, use_existing = args
    ec2 = boto3.resource("ec2", region_name=region)
    instance_name = "{}-{}".format(instance_prefix, region)
    running_instances = list(
        ec2.instances.filter(
            Filters=[
                {"Name": "tag:Name", "Values": [instance_name]},
                {"Name": "instance-state-name", "Values": ["running", "pending"]},
            ]
        )
    )
    if use_existing and len(running_instances) > 0:
        return region, running_instances[0].id
    else:
        ec2 = boto3.resource("ec2", region_name=region)
        ec2.instances.filter(Filters=[{"Name": "tag:Name", "Values": [instance_name]}]).terminate()
        instance = ec2.create_instances(
            ImageId=ami_id,
            InstanceType=instance_class,
            MinCount=1,
            MaxCount=1,
            KeyName=region,
            TagSpecifications=[
                {
                    "ResourceType": "instance",
                    "Tags": [{"Key": "Name", "Value": instance_name}],
                }
            ],
        )
        return region, instance[0].id


def run_script(args):
    region, instance_id, script_path = args
    ec2 = boto3.resource("ec2", region_name=region)
    instance = ec2.Instance(instance_id)
    instance.wait_until_running()
    logger.debug(f"({region}) Instance {instance_id} is running at IP {instance.public_ip_address}")

    # ssh into IP with paramiko
    key_file = os.path.expanduser(f"~/.ssh/{region}.pem")
    key = paramiko.RSAKey.from_private_key_file(key_file)
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(instance.public_ip_address, username="ubuntu", pkey=key)
    logger.debug(f"({region}) Connected to {instance.public_ip_address}")

    # upload script
    _, _, _ = client.exec_command(f"mkdir -p ~/scripts")
    sftp = client.open_sftp()
    sftp.put(script_path, f"/home/ubuntu/scripts/{os.path.basename(script_path)}")
    sftp.close()

    # run script
    _, _, _ = client.exec_command(f"chmod +x ~/scripts/{os.path.basename(script_path)}")
    _, stdout, stderr = client.exec_command(f"~/scripts/{os.path.basename(script_path)}")
    results = (stdout.read().decode("utf-8"), stderr.read().decode("utf-8"))
    client.close()
    return region, results


def main():
    parser = argparse.ArgumentParser(description="Provision EC2 instances")
    parser.add_argument("--instance_class", type=str, default="i3en.large", help="Instance class")
    parser.add_argument(
        "--region_list",
        type=str,
        nargs="+",
        default=[
            "us-east-1",
            "us-east-2",
            "us-west-1",
            # "us-west-2",
            "ap-northeast-1",
            # "ap-northeast-2",
            "ap-southeast-1",
            # "ap-southeast-2",
            "eu-central-1",
            "eu-west-1",
            # "eu-west-2",
        ],
        help="List of regions to provision instances",
    )
    parser.add_argument(
        "--script",
        type=str,
        default=None,
        help="Script to run on each instance (URL), optional",
    )
    parser.add_argument(
        "--instance_prefix",
        type=str,
        default="aws-skydata",
        help="Prefix to name instances",
    )
    parser.add_argument(
        "--use_existing",
        action="store_true",
        help="Use existing instances with the same name",
    )
    parser.add_argument("--stop_all", action="store_true", help="Stop all instances")
    parser.add_argument("--bench_latency", action="store_true", help="Throughput benchmark")
    parser.add_argument("--bench_throughput", action="store_true", help="Throughput benchmark")
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

    # log dir is script directory's parent / data / logs (use pathlib)
    data_dir = Path(__file__).parent.parent / "data"
    log_dir = data_dir / "logs"
    log_dir.mkdir(exist_ok=True, parents=True)

    if args.stop_all:

        def stop_instances(region):
            ec2 = boto3.resource("ec2", region_name=region)
            instance_name = "{}-{}".format(args.instance_prefix, region)
            stopped_instances = ec2.instances.filter(
                Filters=[
                    {"Name": "tag:Name", "Values": [instance_name]},
                    {
                        "Name": "instance-state-name",
                        "Values": ["running", "pending", "stopping", "stopped"],
                    },
                ]
            ).terminate()
            logger.info(f"({region}) Stopped {len(stopped_instances)} instances, {stopped_instances}")

        # noinspection PyUnresolvedReferences
        do_parallel(stop_instances, all_ec2_regions)
        sys.exit(0)

    # make keypairs and save to ~/.ssh for each region if they don't exist
    for region in args.region_list:
        key_file = os.path.expanduser(f"~/.ssh/{region}.pem")
        if not os.path.exists(key_file):
            ec2 = boto3.resource("ec2", region_name=region)
            key_pair = ec2.create_key_pair(KeyName=region)
            with open(key_file, "w") as f:
                f.write(key_pair.key_material)
            os.chmod(key_file, 0o600)
            logger.info(f"({region}) Created keypair and saved to {key_file}")

    # update default security group rules to allow all traffic from all IPs
    for region in args.region_list:
        ec2 = boto3.resource("ec2", region_name=region)
        default_sg_id = [i for i in ec2.security_groups.filter(GroupNames=["default"]).all()][0].id
        default_sg = ec2.SecurityGroup(default_sg_id)
        # if allow all rule doesn't exist, add it
        if not any(
            rule["IpProtocol"] == "-1" and len(rule["IpRanges"]) > 0 and rule["IpRanges"][0]["CidrIp"] == "0.0.0.0/0"
            for rule in default_sg.ip_permissions
        ):
            default_sg.authorize_ingress(IpProtocol="-1", FromPort=0, ToPort=65535, CidrIp="0.0.0.0/0")
            logger.info(
                f"({region}) Updated default security group {default_sg_id} (name = {default_sg.group_name}) to allow all traffic from all IPs"
            )

    # Get the list of AMIs for each region, use multithreading pool
    ami_list = dict(do_parallel(get_ubuntu_ami_id, args.region_list))
    logger.info(f"AMI list: {ami_list}")

    # Provision the instances in parallel
    instance_ids = dict(
        do_parallel(
            provision_instance_wrapper,
            [
                (
                    region,
                    ami_id,
                    args.instance_class,
                    args.instance_prefix,
                    args.use_existing,
                )
                for region, ami_id in ami_list.items()
            ],
        )
    )
    logger.info(f"Instance IDs: {instance_ids}")

    # SSH into the instances and run the script
    if args.script is not None:
        logger.info("Waiting for instances to boot")
        time.sleep(5)
        setup_results = dict(
            do_parallel(
                run_script,
                [(region, instance_id, args.script) for region, instance_id in instance_ids.items()],
            )
        )
        for region, results in setup_results.items():
            with (log_dir / f"{instance_ids[region]}.stdout.log").open("w") as f:
                f.write(results[0])
            with (log_dir / f"{instance_ids[region]}.stderr.log").open("w") as f:
                f.write(results[1])

    # Print the instance IDs with IPs as a table
    print(f"{'Region':<15} {'Instance ID':<30} IP")
    for region, instance_id in instance_ids.items():
        ec2 = boto3.resource("ec2", region_name=region)
        instance = ec2.Instance(instance_id)
        print(f"{region:<15} {instance_id:<30} {instance.public_ip_address}")

    # SSH into each instance and compute pairwise latency
    if args.bench_latency:
        latency_pairs = dict()
        pbar = tqdm(total=len(args.region_list) * len(args.region_list))
        for region, instance_id in instance_ids.items():
            ec2 = boto3.resource("ec2", region_name=region)
            instance = ec2.Instance(instance_id)
            logger.info(f"({region}) Instance {instance_id} is running at IP {instance.public_ip_address}")

            # ssh into IP with paramiko
            key_file = os.path.expanduser(f"~/.ssh/{region}.pem")
            key = paramiko.RSAKey.from_private_key_file(key_file)
            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            client.connect(instance.public_ip_address, username="ubuntu", pkey=key)
            logger.debug(f"({region}) Connected to {instance.public_ip_address}")

            # run script
            for dest_region, dest_instance_id in instance_ids.items():
                pbar.set_description(f"{region} -> {dest_region}")
                ec2_dest = boto3.resource("ec2", region_name=dest_region)
                dest_ip = ec2_dest.Instance(dest_instance_id).public_ip_address
                _, stdout, stderr = client.exec_command(f"ping -c 16 {dest_ip}")
                stdout_parsed = stdout.read().decode("utf-8").strip()
                latency_pairs[(region, dest_region)] = stdout_parsed.split("\n")[-1]
                logger.info(
                    f"({region} -> {dest_region}) {instance_id} -> {dest_instance_id} latency: {latency_pairs[(region, dest_region)]}"
                )
                (data_dir / "logs" / "latency").mkdir(exist_ok=True, parents=True)
                with (data_dir / "logs" / "latency" / f"{region}-{dest_region}.log").open("w") as f:
                    f.write(stdout_parsed)
                pbar.update(1)
            client.close()
        pbar.close()
        with (data_dir / "latency_pairs.pickle").open("wb") as f:
            pickle.dump(latency_pairs, f)

    if args.bench_throughput:
        """
        SSH into pairs of instances and use iperf3 to test throughput.
        On all servers, launch iperf3 server in daemon mode: iperf3 -s -D
        For each client, run iperf3 client: iperf3 -J -P <num_threads> -w <packet_size> -c <server_ip>
        Test number of threads at 1, 2, 4, 8, 16, 32

        Make data_dir / throughput_log directory and log_dir / throughput_log directory.
        Save stdout and stderr of client to log files at log_dir / throughput_log / <region>-<dest_region>.std{out|err}.log.
        Each client writes JSON. Write pickle of dict of (region, dest_region) -> JSON to data_dir / throughput_log / throughput_pairs.pickle.
        """

        def start_iperf_server(region):
            instance_id = instance_ids[region]
            ec2_dest = boto3.resource("ec2", region_name=region)
            ip = ec2_dest.Instance(instance_id).public_ip_address

            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            key_file = os.path.expanduser(f"~/.ssh/{region}.pem")
            key = paramiko.RSAKey.from_private_key_file(key_file)
            client.connect(ip, username="ubuntu", pkey=key)
            logger.info(f"({region}) Connected to {ip}")

            _, stdout, stderr = client.exec_command(f"iperf3 -s -D")
            stdout_parsed = stdout.read().decode("utf-8").strip()
            stderr_parsed = stderr.read().decode("utf-8").strip()
            client.close()

            logger.info(f"{region} server started")
            (log_dir / "throughput_log").mkdir(exist_ok=True, parents=True)
            with (log_dir / "throughput_log" / f"{region}_server.stdout.log").open("w") as f:
                f.write(stdout_parsed)
            with (log_dir / "throughput_log" / f"{region}_server.stderr.log").open("w") as f:
                f.write(stderr_parsed)

        def kill_iperf_server(region):
            instance_id = instance_ids[region]
            ec2_dest = boto3.resource("ec2", region_name=region)
            ip = ec2_dest.Instance(instance_id).public_ip_address

            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            key_file = os.path.expanduser(f"~/.ssh/{region}.pem")
            key = paramiko.RSAKey.from_private_key_file(key_file)
            client.connect(ip, username="ubuntu", pkey=key)
            logger.info(f"({region}) Connected to {ip}")

            _, stdout, stderr = client.exec_command(f"kill -9 $(pidof iperf3)")
            stdout_parsed = stdout.read().decode("utf-8").strip()
            stderr_parsed = stderr.read().decode("utf-8").strip()
            client.close()

            logger.info(f"{region} server killed")
            (log_dir / "throughput_log").mkdir(exist_ok=True, parents=True)
            with (log_dir / "throughput_log" / f"{region}_server_kill.stdout.log").open("w") as f:
                f.write(stdout_parsed)
            with (log_dir / "throughput_log" / f"{region}_server_kill.stderr.log").open("w") as f:
                f.write(stderr_parsed)

        def start_iperf_client(config):
            region, dest_region, num_threads_list = config
            instance_id = instance_ids[region]
            ec2 = boto3.resource("ec2", region_name=region)
            source_ip = ec2.Instance(instance_id).public_ip_address

            ec2_dest = boto3.resource("ec2", region_name=dest_region)
            dest_ip = ec2_dest.Instance(instance_ids[dest_region]).public_ip_address

            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            key_file = os.path.expanduser(f"~/.ssh/{region}.pem")
            key = paramiko.RSAKey.from_private_key_file(key_file)
            client.connect(source_ip, username="ubuntu", pkey=key)
            logger.info(f"({dest_region} -> {region}) Connected to {source_ip}")

            results = {}
            for num_threads in num_threads_list:
                _, stdout, stderr = client.exec_command(f"iperf3 -J -t {args.iperf3_runtime} -P {num_threads} -c {dest_ip}")
                stdout_parsed = stdout.read().decode("utf-8").strip()
                stderr_parsed = stderr.read().decode("utf-8").strip()
                client.close()

                result = json.loads(stdout_parsed)
                throughput = result["end"]["sum_sent"]["bits_per_second"]
                logger.info(f"({dest_region} -> {region}) {instance_id} -> {instance_id} throughput: {throughput}")
                results[num_threads] = result
                (log_dir / "throughput_log").mkdir(exist_ok=True, parents=True)
                with (log_dir / "throughput_log" / f"{dest_region}-{region}-{num_threads}.stdout.log").open("w") as f:
                    f.write(stdout_parsed)
                with (log_dir / "throughput_log" / f"{dest_region}-{region}-{num_threads}.stderr.log").open("w") as f:
                    f.write(stderr_parsed)
            return config, results

        # run script
        throughput_pairs = dict()
        pbar = tqdm(total=len(instance_ids) * len(instance_ids))
        configs = [(s, d) for s in instance_ids.keys() for d in instance_ids.keys()]
        parallel_config_groups = split_list(configs)  # list of disjoint configurations that can run in parallel
        for config_group in parallel_config_groups:
            logger.info(f"Running {len(config_group)} configs in parallel, {config_group}")
        exit(1)
        # start iperf servers
        do_parallel(start_iperf_server, instance_ids.keys())
        time.sleep(args.iperf3_runtime + 10)

        # start iperf clients
        for parallel_config_group in parallel_config_groups:
            configs = [(s, d, args.iperf_connection_list) for s, d in parallel_config_group]
            results = do_parallel(start_iperf_client, configs)
            for config, result in results:
                region, dest_region, num_threads_list = config
                for num_threads, result in result.items():
                    throughput_pairs[(region, dest_region, num_threads)] = result
                pbar.update(1)
        pbar.close()

        # kill iperf servers
        do_parallel(kill_iperf_server, instance_ids.keys())

        logger.info("Writing logs")
        with (data_dir / "throughput_pairs.pickle").open("wb") as f:
            pickle.dump(throughput_pairs, f)
        logger.info("Done!")


if __name__ == "__main__":
    main()
