import json
import os
import random
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional, Tuple

import pandas as pd
import typer

from rich.progress import Progress

from skyplane import GB, skyplane_root
from skyplane.cli.experiments.provision import provision
from skyplane.compute.aws.aws_cloud_provider import AWSCloudProvider
from skyplane.compute.azure.azure_cloud_provider import AzureCloudProvider
from skyplane.compute.const_cmds import make_sysctl_tcp_tuning_command
from skyplane.compute.gcp.gcp_cloud_provider import GCPCloudProvider
from skyplane.compute.gcp.gcp_server import GCPServer
from skyplane.compute.server import Server
from skyplane.utils import logger
from skyplane.utils.fn import do_parallel

all_aws_regions = AWSCloudProvider.region_list()
all_azure_regions = AzureCloudProvider.region_list()
all_gcp_regions = GCPCloudProvider.region_list()
all_gcp_regions_standard = GCPCloudProvider.region_list_standard()


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


def start_iperf3_client(arg_pair: Tuple[Server, Server], iperf3_log_dir: Path, iperf3_runtime: int, iperf3_connections: int):
    instance_src, instance_dst = arg_pair
    tag = f"{instance_src.region_tag}:{instance_src.network_tier()}_{instance_dst.region_tag}:{instance_dst.network_tier()}"

    # run benchmark
    stdout, stderr = instance_src.run_command(
        f"iperf3 -J -Z -C cubic -t {iperf3_runtime} -P {iperf3_connections} -c {instance_dst.public_ip()}"
    )

    # save logs
    with (iperf3_log_dir / f"{tag}.stdout").open("w") as f:
        f.write(stdout)
    if stderr:
        with (iperf3_log_dir / f"{tag}.stderr").open("w") as f:
            f.write(stderr)
        logger.error(f"{tag} stderr: {stderr}")

    out_rec = dict(tag=tag, stdout_path=str(iperf3_log_dir / f"{tag}.stdout"), stderr_path=str(iperf3_log_dir / f"{tag}.stderr"))
    try:
        result = json.loads(stdout)
        out_rec["throughput_sent"] = result["end"]["sum_sent"]["bits_per_second"]
        out_rec["throughput_recieved"] = result["end"]["sum_received"]["bits_per_second"]
        out_rec["cpu_utilization"] = result["end"]["cpu_utilization_percent"]["host_total"]
        out_rec["success"] = True
    except Exception as e:
        logger.exception(e)
        logger.error(f"({instance_src.region_tag} -> {instance_dst.region_tag}) iperf3 client failed: {stdout} {stderr}")
        out_rec["success"] = False
        out_rec["exception"] = str(e)
        out_rec["raw_output"] = str(stdout)
        return out_rec

    instance_src.close_server()
    instance_dst.close_server()
    return out_rec


def throughput_grid(
    resume: Optional[Path] = typer.Option(
        None, help="Resume from a past result. Pass the resulting CSV for the past result to resume. Default is None."
    ),
    copy_resume_file: bool = typer.Option(True, help="Copy the resume file to the output CSV. Default is True."),
    # regions
    aws_region_list: List[str] = typer.Option(all_aws_regions, "-aws"),
    azure_region_list: List[str] = typer.Option(all_azure_regions, "-azure"),
    gcp_region_list: List[str] = typer.Option(all_gcp_regions, "-gcp"),
    gcp_standard_region_list: List[str] = typer.Option(all_gcp_regions_standard, "-gcp-standard"),
    enable_aws: bool = typer.Option(True),
    enable_azure: bool = typer.Option(True),
    enable_gcp: bool = typer.Option(True),
    enable_gcp_standard: bool = typer.Option(True),
    # instances to provision
    aws_instance_class: str = typer.Option("m5.8xlarge", help="AWS instance class to use"),
    azure_instance_class: str = typer.Option("Standard_D32_v5", help="Azure instance class to use"),
    gcp_instance_class: str = typer.Option("n2-standard-32", help="GCP instance class to use"),
    # iperf3 options
    iperf3_runtime: int = typer.Option(5, help="Runtime for iperf3 in seconds"),
    iperf3_connections: int = typer.Option(64, help="Number of connections to test"),
):
    def check_stderr(tup):
        assert tup[1].strip() == "", f"Command failed, err: {tup[1]}"

    if resume:
        index_key = [
            "iperf3_connections",
            "iperf3_runtime",
            "src_instance_class",
            "dst_instance_class",
            "src_tier",
            "dst_tier",
            "src_region",
            "dst_region",
        ]
        resume_from_trial = pd.read_csv(resume).set_index(index_key)
        resume_keys = resume_from_trial.index.unique().tolist()
    else:
        resume_keys = []

    # validate arguments
    aws_region_list = aws_region_list if enable_aws else []
    azure_region_list = azure_region_list if enable_azure else []
    gcp_region_list = gcp_region_list if enable_gcp else []
    if not enable_aws and not enable_azure and not enable_gcp:
        logger.error("At least one of -aws, -azure, -gcp must be enabled.")
        raise typer.Abort()

    # validate AWS regions
    if not enable_aws:
        aws_region_list = []
    elif not all(r in all_aws_regions for r in aws_region_list):
        logger.error(f"Invalid AWS region list: {aws_region_list}")
        raise typer.Abort()

    # validate Azure regions
    if not enable_azure:
        azure_region_list = []
    elif not all(r in all_azure_regions for r in azure_region_list):
        logger.error(f"Invalid Azure region list: {azure_region_list}")
        raise typer.Abort()

    # validate GCP regions
    assert not enable_gcp_standard or enable_gcp, f"GCP is disabled but GCP standard is enabled"
    if not enable_gcp:
        gcp_region_list = []
    elif not all(r in all_gcp_regions for r in gcp_region_list):
        logger.error(f"Invalid GCP region list: {gcp_region_list}")
        raise typer.Abort()

    # validate GCP standard instances
    if not enable_gcp_standard:
        gcp_standard_region_list = []
    if not all(r in all_gcp_regions_standard for r in gcp_standard_region_list):
        logger.error(f"Invalid GCP standard region list: {gcp_standard_region_list}")
        raise typer.Abort()

    # provision servers
    aws = AWSCloudProvider()
    azure = AzureCloudProvider()
    gcp = GCPCloudProvider()
    aws_instances, azure_instances, gcp_instances = provision(
        aws=aws,
        azure=azure,
        gcp=gcp,
        aws_regions_to_provision=aws_region_list,
        azure_regions_to_provision=azure_region_list,
        gcp_regions_to_provision=gcp_region_list,
        aws_instance_class=aws_instance_class,
        azure_instance_class=azure_instance_class,
        gcp_instance_class=gcp_instance_class,
        gcp_use_premium_network=True,
    )
    instance_list: List[Server] = [i for ilist in aws_instances.values() for i in ilist]
    instance_list.extend([i for ilist in azure_instances.values() for i in ilist])
    instance_list.extend([i for ilist in gcp_instances.values() for i in ilist])

    # provision standard tier servers
    _, _, gcp_standard_instances = provision(
        aws=aws,
        azure=azure,
        gcp=gcp,
        aws_regions_to_provision=[],
        azure_regions_to_provision=[],
        gcp_regions_to_provision=gcp_standard_region_list,
        aws_instance_class=aws_instance_class,
        azure_instance_class=azure_instance_class,
        gcp_instance_class=gcp_instance_class,
        gcp_use_premium_network=False,
    )
    instance_list.extend([i for ilist in gcp_standard_instances.values() for i in ilist])

    # setup instances
    def setup(server: Server):
        check_stderr(server.run_command("echo 'debconf debconf/frontend select Noninteractive' | sudo debconf-set-selections"))
        check_stderr(
            server.run_command(
                "(sudo apt-get update && sudo apt-get install -y dialog apt-utils && sudo apt-get install -y iperf3); pkill iperf3; iperf3 -s -D -J"
            )
        )
        check_stderr(server.run_command(make_sysctl_tcp_tuning_command(cc="cubic")))

    do_parallel(setup, instance_list, spinner=True, n=-1, desc="Setup")

    # build experiment
    instance_pairs_all = [(i1, i2) for i1 in instance_list for i2 in instance_list if i1 != i2]
    instance_pairs = []
    for i1, i2 in instance_pairs_all:
        exp_key = (
            iperf3_connections,
            iperf3_runtime,
            i1.instance_class(),
            i2.instance_class(),
            i1.network_tier(),
            i2.network_tier(),
            i1.region_tag,
            i2.region_tag,
        )
        if exp_key in resume_keys:
            logger.info(f"Key already in resume set: {exp_key}")
        else:
            instance_pairs.append((i1, i2))
    groups = split_list(instance_pairs)

    # confirm experiment
    experiment_tag_words = os.popen("bash scripts/get_random_word_hash.sh").read().strip()
    timestamp = datetime.now(timezone.utc).strftime("%Y.%m.%d_%H.%M")
    experiment_tag = f"{timestamp}_{experiment_tag_words}_{iperf3_runtime}s_{iperf3_connections}c"
    data_dir = skyplane_root / "data"
    log_dir = data_dir / "logs" / "throughput_grid" / f"{experiment_tag}"
    raw_iperf3_log_dir = log_dir / "raw_iperf3_logs"

    # ask for confirmation
    typer.secho(f"\nExperiment configuration: (total pairs = {len(instance_pairs)})", fg="red", bold=True)
    for group_idx, group in enumerate(groups):
        typer.secho(f"\tGroup {group_idx}: ({len(group)} items)", fg="green", bold=True)
        for instance_pair in group:
            typer.secho(
                f"\t{instance_pair[0].region_tag}:{instance_pair[0].network_tier()} -> {instance_pair[1].region_tag}:{instance_pair[1].network_tier()}"
            )
    gbyte_sent = len(instance_pairs) * 5.0 / 8 * iperf3_runtime
    typer.secho(f"\niperf_runtime={iperf3_runtime}, iperf3_connections={iperf3_connections}", fg="blue")
    typer.secho(f"Approximate runtime: {len(groups) * (10 + iperf3_runtime)}s (assuming 10s startup time)", fg="blue")
    typer.secho(f"Approximate data to send: {gbyte_sent:.2f}GB (assuming 5Gbps)", fg="blue")
    typer.secho(f"Approximate cost: ${gbyte_sent * 0.1:.2f} (assuming $0.10/GB)", fg="red")
    logger.debug(f"Experiment tag: {experiment_tag}")
    logger.debug(f"Log directory: {log_dir}")
    sys.stdout.flush()
    sys.stderr.flush()
    if not typer.confirm(f"\nRun experiment? (tag: {experiment_tag})", default=True):
        logger.error("Exiting")
        sys.exit(1)

    # make experiment directory
    logger.debug(f"Raw iperf3 log directory: {raw_iperf3_log_dir}")
    raw_iperf3_log_dir.mkdir(exist_ok=True, parents=True)
    log_dir.mkdir(exist_ok=True, parents=True)

    # define iperf3 client function
    def client_fn(instance_pair):
        instance_src, instance_dst = instance_pair
        result_rec = dict(
            src_region=instance_src.region_tag,
            src_tier=instance_src.network_tier(),
            src_instance_class=instance_src.instance_class(),
            dst_region=instance_dst.region_tag,
            dst_tier=instance_dst.network_tier(),
            dst_instance_class=instance_dst.instance_class(),
            iperf3_connections=iperf3_connections,
            iperf3_runtime=iperf3_runtime,
        )
        rec = start_iperf3_client(
            instance_pair, iperf3_log_dir=raw_iperf3_log_dir, iperf3_runtime=iperf3_runtime, iperf3_connections=iperf3_connections
        )
        if rec is not None:
            result_rec.update(rec)
        pbar.console.print(f"{result_rec['tag']}: {result_rec.get('throughput_sent', 0.) / GB:.2f}Gbps")
        pbar.update(task_total, advance=1)
        return result_rec

    # run experiment
    new_througput_results = []
    output_file = log_dir / "throughput.csv"
    with Progress() as pbar:
        task_total = pbar.add_task("Total throughput evaluation", total=len(instance_pairs))
        for group_idx, group in enumerate(groups):
            tag_fmt = lambda x: f"{x[0].region_tag}:{x[0].network_tier()} to {x[1].region_tag}:{x[1].network_tier()}"
            results = do_parallel(
                client_fn, group, spinner=True, desc=f"Parallel eval group {group_idx}", n=-1, arg_fmt=tag_fmt, return_args=False
            )
            new_througput_results.extend([rec for rec in results if rec is not None])

            # build dataframe from results
            pbar.console.print(f"Saving intermediate results to {output_file}")
            df = pd.DataFrame(new_througput_results)
            if resume and copy_resume_file:
                logger.debug(f"Copying old CSV entries from {resume}")
                df = df.append(pd.read_csv(resume))
            df.to_csv(output_file, index=False)

    logger.info(f"Experiment complete: {experiment_tag}")
    logger.info(f"Results saved to {output_file}")


def latency_grid(
    aws_region_list: List[str] = typer.Option(all_aws_regions, "-aws"),
    azure_region_list: List[str] = typer.Option(all_azure_regions, "-azure"),
    gcp_region_list: List[str] = typer.Option(all_gcp_regions, "-gcp"),
    gcp_standard_region_list: List[str] = typer.Option(all_gcp_regions_standard, "-gcp-standard"),
    enable_aws: bool = typer.Option(True),
    enable_azure: bool = typer.Option(True),
    enable_gcp: bool = typer.Option(True),
    enable_gcp_standard: bool = typer.Option(True),
    # instances to provision
    aws_instance_class: str = typer.Option("m5.large", help="AWS instance class to use"),
    azure_instance_class: str = typer.Option("Standard_D2_v3", help="Azure instance class to use"),
    gcp_instance_class: str = typer.Option("n2-standard-4", help="GCP instance class to use"),
):
    # similar to throughput_grid but start all instances at once and then ping all pairs of instances concurrently

    def check_stderr(tup):
        assert tup[1].strip() == "", f"Command failed, err: {tup[1]}"

    # validate arguments
    aws_region_list = aws_region_list if enable_aws else []
    azure_region_list = azure_region_list if enable_azure else []
    gcp_region_list = gcp_region_list if enable_gcp else []
    if not enable_aws and not enable_azure and not enable_gcp:
        logger.error("At least one of -aws, -azure, -gcp must be enabled.")
        raise typer.Abort()

    # validate AWS regions
    if not enable_aws:
        aws_region_list = []
    elif not all(r in all_aws_regions for r in aws_region_list):
        logger.error(f"Invalid AWS region list: {aws_region_list}")
        raise typer.Abort()

    # validate Azure regions
    azure_region_list = [r for r in azure_region_list if r != "westus2" and r != "eastus2"]  # due to instance class
    if not enable_azure:
        azure_region_list = []
    elif not all(r in all_azure_regions for r in azure_region_list):
        logger.error(f"Invalid Azure region list: {azure_region_list}")
        raise typer.Abort()

    # validate GCP regions
    assert not enable_gcp_standard or enable_gcp, f"GCP is disabled but GCP standard is enabled"
    gcp_region_list = [r for r in gcp_region_list if r != "us-west2-c"]  # due to instance class
    if not enable_gcp:
        gcp_region_list = []
    elif not all(r in all_gcp_regions for r in gcp_region_list):
        logger.error(f"Invalid GCP region list: {gcp_region_list}")
        raise typer.Abort()

    # validate GCP standard instances
    if not enable_gcp_standard:
        gcp_standard_region_list = []
    if not all(r in all_gcp_regions_standard for r in gcp_standard_region_list):
        logger.error(f"Invalid GCP standard region list: {gcp_standard_region_list}")
        raise typer.Abort()

    # provision servers
    aws = AWSCloudProvider()
    azure = AzureCloudProvider()
    gcp = GCPCloudProvider()
    aws_instances, azure_instances, gcp_instances = provision(
        aws=aws,
        azure=azure,
        gcp=gcp,
        aws_regions_to_provision=aws_region_list,
        azure_regions_to_provision=azure_region_list,
        gcp_regions_to_provision=gcp_region_list,
        aws_instance_class=aws_instance_class,
        azure_instance_class=azure_instance_class,
        gcp_instance_class=gcp_instance_class,
        gcp_use_premium_network=True,
    )
    instance_list: List[Server] = [i for ilist in aws_instances.values() for i in ilist]
    instance_list.extend([i for ilist in azure_instances.values() for i in ilist])
    instance_list.extend([i for ilist in gcp_instances.values() for i in ilist])

    # provision standard tier servers
    _, _, gcp_standard_instances = provision(
        aws=aws,
        azure=azure,
        gcp=gcp,
        aws_regions_to_provision=[],
        azure_regions_to_provision=[],
        gcp_regions_to_provision=gcp_standard_region_list,
        aws_instance_class=aws_instance_class,
        azure_instance_class=azure_instance_class,
        gcp_instance_class=gcp_instance_class,
        gcp_use_premium_network=False,
    )
    instance_list.extend([i for ilist in gcp_standard_instances.values() for i in ilist])

    # setup instances
    def setup(server: Server):
        check_stderr(server.run_command(make_sysctl_tcp_tuning_command(cc="cubic")))

    do_parallel(setup, instance_list, spinner=True, n=-1, desc="Setup")

    # build experiment
    instance_pairs_all = [(i1, i2) for i1 in instance_list for i2 in instance_list if i1 != i2]
    instance_pairs = []
    for i1, i2 in instance_pairs_all:
        instance_pairs.append((i1, i2))
    random.shuffle(instance_pairs)

    # confirm experiment
    experiment_tag_words = os.popen("bash scripts/get_random_word_hash.sh").read().strip()
    timestamp = datetime.now(timezone.utc).strftime("%Y.%m.%d_%H.%M")
    experiment_tag = f"{timestamp}_{experiment_tag_words}"
    data_dir = skyplane_root / "data"
    log_dir = data_dir / "logs" / "latency_grid" / f"{experiment_tag}"

    # ask for confirmation
    typer.secho(f"\nExperiment configuration: (total pairs = {len(instance_pairs)})", fg="red", bold=True)
    logger.debug(f"Experiment tag: {experiment_tag}")
    logger.debug(f"Log directory: {log_dir}")
    sys.stdout.flush()
    sys.stderr.flush()
    if not typer.confirm(f"\nRun experiment? (tag: {experiment_tag})", default=True):
        logger.error("Exiting")
        sys.exit(1)

    # define ping command
    def client_fn(instance_pair):
        instance_src, instance_dst = instance_pair
        result_rec = dict(
            src_region=instance_src.region_tag,
            src_tier=instance_src.network_tier(),
            src_instance_class=instance_src.instance_class(),
            dst_region=instance_dst.region_tag,
            dst_tier=instance_dst.network_tier(),
            dst_instance_class=instance_dst.instance_class(),
        )

        ping_cmd = f"ping -c 10 {instance_dst.public_ip()}"
        if isinstance(instance_src, GCPServer):
            ping_cmd = f"docker run --net=host alpine {ping_cmd}"
        ping_result_stdout, ping_result_stderr = instance_src.run_command(ping_cmd)
        values = list(map(float, ping_result_stdout.strip().split("\n")[-1].split(" = ")[-1][:-3].split("/")))
        try:
            if len(values) == 4:
                (min_rtt, avg_rtt, max_rtt, mdev_rtt) = values
            else:
                (min_rtt, avg_rtt, max_rtt) = values
                mdev_rtt = None
        except Exception as e:
            logger.error(f"{instance_src.region_tag} -> {instance_dst.region_tag} ping failed: {e}")
            logger.warning(f"Full ping output: {ping_result_stdout}")
            logger.warning(f"Full ping error: {ping_result_stderr}")
            (min_rtt, avg_rtt, max_rtt, mdev_rtt) = (None, None, None, None)
        result_rec["min_rtt"] = min_rtt
        result_rec["avg_rtt"] = avg_rtt
        result_rec["max_rtt"] = max_rtt
        result_rec["mdev_rtt"] = mdev_rtt
        pbar.console.print(f"{instance_src.region_tag} -> {instance_dst.region_tag}: {avg_rtt} ms")
        pbar.update(task_total, advance=1)
        return result_rec

    # run experiment
    new_througput_results = []
    log_dir.mkdir(parents=True, exist_ok=True)
    output_file = log_dir / "latency.csv"
    with Progress() as pbar:
        task_total = pbar.add_task("Total latency evaluation", total=len(instance_pairs))
        results = do_parallel(client_fn, instance_pairs, n=16, return_args=False)
        new_througput_results.extend([rec for rec in results if rec is not None])

    # build dataframe from results
    print(f"Saving intermediate results to {output_file}")
    df = pd.DataFrame(new_througput_results)
    df.to_csv(output_file, index=False)
