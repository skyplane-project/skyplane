import json
import os
import sys
from datetime import datetime, timezone
from functools import partial
from pathlib import Path
from typing import List, Optional, Tuple

import pandas as pd
import questionary
import typer
from skylark import GB, skylark_root
from skylark.benchmark.utils import provision, split_list
from skylark.cli.cli_helper import load_config
from skylark.compute.aws.aws_cloud_provider import AWSCloudProvider
from skylark.compute.azure.azure_cloud_provider import AzureCloudProvider
from skylark.compute.gcp.gcp_cloud_provider import GCPCloudProvider
from skylark.compute.server import Server
from skylark.utils.utils import do_parallel
from tqdm import tqdm

aws_regions = AWSCloudProvider.region_list()
azure_regions = AzureCloudProvider.region_list()
gcp_regions = GCPCloudProvider.region_list()

# aws_regions = [
#     "eu-south-1",
#     "us-west-2",
#     "us-east-2",
#     "ap-northeast-3",
#     "eu-central-1",
#     "eu-north-1",
#     "us-west-1",
#     "sa-east-1",
#     "eu-west-2",
#     "ap-southeast-3",
# ]

# azure_regions = [
#     "northcentralus",
#     "uksouth",
#     "swedencentral",
#     "canadacentral",
#     "australiaeast",
#     "westeurope",
#     "centralindia",
#     "francecentral",
#     "norwayeast",
#     "switzerlandnorth",
# ]

# gcp_regions = [
#     "australia-southeast2-a",
#     "europe-west6-a",
#     "australia-southeast1-a",
#     "southamerica-west1-a",
#     "southamerica-east1-a",
#     "asia-southeast1-a",
#     "europe-west4-a",
#     "asia-southeast2-a",
#     "northamerica-northeast1-a",
#     "northamerica-northeast2-a",
# ]


log_info = partial(typer.secho, fg="blue")
log_success = partial(typer.secho, fg="green")
log_error = partial(typer.secho, fg="red")


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
        log_error(f"{tag} stderr: {stderr}")

    out_rec = dict(
        tag=tag,
        stdout_path=str(iperf3_log_dir / f"{tag}.stdout"),
        stderr_path=str(iperf3_log_dir / f"{tag}.stderr"),
    )
    try:
        result = json.loads(stdout)
    except json.JSONDecodeError as e:
        log_error(f"({instance_src.region_tag} -> {instance_dst.region_tag}) iperf3 client failed: {stdout} {stderr}")
        out_rec["success"] = False
        out_rec["exception"] = str(e)
        return out_rec

    out_rec["throughput_sent"] = result["end"]["sum_sent"]["bits_per_second"]
    out_rec["throughput_recieved"] = result["end"]["sum_received"]["bits_per_second"]
    out_rec["cpu_utilization"] = result["end"]["cpu_utilization_percent"]["host_total"]
    out_rec["success"] = True

    instance_src.close_server()
    instance_dst.close_server()
    return out_rec


def throughput_grid(
    resume: Optional[Path] = typer.Option(
        None, help="Resume from a past result. Pass the resulting CSV for the past result to resume. Default is None."
    ),
    copy_resume_file: bool = typer.Option(True, help="Copy the resume file to the output CSV. Default is True."),
    # regions
    aws_region_list: List[str] = typer.Option(aws_regions, "-aws"),
    azure_region_list: List[str] = typer.Option(azure_regions, "-azure"),
    gcp_region_list: List[str] = typer.Option(gcp_regions, "-gcp"),
    enable_aws: bool = typer.Option(True),
    enable_azure: bool = typer.Option(True),
    enable_gcp: bool = typer.Option(True),
    # instances to provision
    aws_instance_class: str = typer.Option("m5.8xlarge", help="AWS instance class to use"),
    azure_instance_class: str = typer.Option("Standard_D32_v5", help="Azure instance class to use"),
    gcp_instance_class: str = typer.Option("n2-standard-32", help="GCP instance class to use"),
    # cloud options
    gcp_test_standard_network: bool = typer.Option(False, help="Test GCP standard network in addition to premium (default to false)"),
    azure_test_standard_network: bool = typer.Option(False, help="Test Azure standard network in addition to premium (default to false)"),
    gcp_project: Optional[str] = None,
    azure_subscription: Optional[str] = None,
    # iperf3 options
    iperf3_runtime: int = typer.Option(5, help="Runtime for iperf3 in seconds"),
    iperf3_connections: int = typer.Option(64, help="Number of connections to test"),
):
    config = load_config()
    gcp_project = gcp_project or config.get("gcp_project_id")
    azure_subscription = azure_subscription or config.get("azure_subscription_id")
    log_info(f"Loaded from config file: gcp_project={gcp_project}, azure_subscription={azure_subscription}")

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
        log_error("At least one of -aws, -azure, -gcp must be enabled.")
        typer.Abort()
    if not all(r in aws_regions for r in aws_region_list):
        log_error(f"Invalid AWS region list: {aws_region_list}")
        typer.Abort()
    if not all(r in azure_regions for r in azure_region_list):
        log_error(f"Invalid Azure region list: {azure_region_list}")
        typer.Abort()
    if not all(r in gcp_regions for r in gcp_region_list):
        log_error(f"Invalid GCP region list: {gcp_region_list}")
        typer.Abort()
    assert not gcp_test_standard_network, "GCP standard network is not supported yet"
    assert not azure_test_standard_network, "Azure standard network is not supported yet"

    # provision servers
    aws = AWSCloudProvider()
    azure = AzureCloudProvider(azure_subscription)
    gcp = GCPCloudProvider(gcp_project)
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
        gcp_use_premium_network=not gcp_test_standard_network,
    )
    instance_list: List[Server] = [i for ilist in aws_instances.values() for i in ilist]
    instance_list.extend([i for ilist in azure_instances.values() for i in ilist])
    instance_list.extend([i for ilist in gcp_instances.values() for i in ilist])

    # setup instances
    def setup(server: Server):
        sysctl_updates = {
            "net.core.rmem_max": 2147483647,
            "net.core.wmem_max": 2147483647,
            "net.ipv4.tcp_rmem": "4096 87380 1073741824",
            "net.ipv4.tcp_wmem": "4096 65536 1073741824",
        }
        server.run_command("sudo sysctl -w {}".format(" ".join(f"{k}={v}" for k, v in sysctl_updates.items())))
        server.run_command("(sudo apt-get update && sudo apt-get install -y iperf3); pkill iperf3; iperf3 -s -D -J")

    do_parallel(setup, instance_list, progress_bar=True, n=-1, desc="Setup")

    # build experiment
    instance_pairs_all = [(i1, i2) for i1 in instance_list for i2 in instance_list if i1 != i2]
    instance_pairs = []
    for i1, i2 in instance_pairs_all:
        # ["iperf3_connections", "iperf3_runtime", "src_instance_class", "dst_instance_class", "src_tier", "dst_tier", "src_region",  "dst_region"]
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
            log_success(f"Key already in resume set: {exp_key}")
        else:
            instance_pairs.append((i1, i2))
    groups = split_list(instance_pairs)

    # confirm experiment
    experiment_tag_words = os.popen("bash scripts/utils/get_random_word_hash.sh").read().strip()
    timestamp = datetime.now(timezone.utc).strftime("%Y.%m.%d_%H.%M")
    experiment_tag = f"{timestamp}_{experiment_tag_words}_{iperf3_runtime}s_{iperf3_connections}c"
    typer.secho(f"\nExperiment configuration: (total pairs = {len(instance_pairs)})", fg="red", bold=True)
    for group_idx, group in enumerate(groups):
        typer.secho(f"\tGroup {group_idx}: ({len(group)} items)", fg="green", bold=True)
        for instance_pair in group:
            typer.secho(f"\t{instance_pair[0].region_tag} -> {instance_pair[1].region_tag}")
    gbyte_sent = len(instance_pairs) * 5.0 / 8 * iperf3_runtime
    typer.secho(f"\niperf_runtime={iperf3_runtime}, iperf3_connections={iperf3_connections}", fg="blue")
    typer.secho(f"Approximate runtime: {len(groups) * (10 + iperf3_runtime)}s (assuming 10s startup time)", fg="blue")
    typer.secho(f"Approximate data to send: {gbyte_sent:.2f}GB (assuming 5Gbps)", fg="blue")
    typer.secho(f"Approximate cost: ${gbyte_sent * 0.1:.2f} (assuming $0.10/GB)", fg="red")
    sys.stdout.flush()
    sys.stderr.flush()
    if not questionary.confirm(f"Launch experiment {experiment_tag}?", default=False).ask():
        log_error("Exiting")
        sys.exit(1)

    # make experiment directory
    data_dir = skylark_root / "data"
    log_dir = data_dir / "logs" / "throughput_grid" / f"{experiment_tag}"
    log_dir.mkdir(exist_ok=True, parents=True)
    raw_iperf3_log_dir = log_dir / "raw_iperf3_logs"
    raw_iperf3_log_dir.mkdir(exist_ok=True, parents=True)
    log_info(f"Experiment tag: {experiment_tag}")
    log_info(f"Log directory: {log_dir}")
    log_info(f"Raw iperf3 log directory: {raw_iperf3_log_dir}")

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
        pbar.update(1)
        tqdm.write(f"{result_rec['tag']}: {result_rec.get('throughput_sent', 0.) / GB:.2f}Gbps")
        return result_rec

    # run experiments
    new_througput_results = []
    output_file = log_dir / "throughput.csv"
    with tqdm(total=len(instance_pairs), desc="Total throughput evaluation") as pbar:
        for group_idx, group in enumerate(groups):
            tag_fmt = lambda x: f"{x[0].region_tag}:{x[0].network_tier()} to {x[1].region_tag}:{x[1].network_tier()}"
            results = do_parallel(client_fn, group, progress_bar=True, desc=f"Parallel eval group {group_idx}", n=-1, arg_fmt=tag_fmt)
            new_througput_results.extend([rec for args, rec in results if rec is not None])

            # build dataframe from results
            tqdm.write(f"Saving intermediate results to {output_file}")
            df = pd.DataFrame(new_througput_results)
            if resume and copy_resume_file:
                log_info(f"Copying old CSV entries from {resume}")
                df = df.append(pd.read_csv(resume))
            df.to_csv(output_file, index=False)

    log_success(f"Experiment complete: {experiment_tag}")
    log_success(f"Results saved to {output_file}")
