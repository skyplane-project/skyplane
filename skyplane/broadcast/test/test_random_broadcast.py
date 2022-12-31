import traceback
import skyplane
import typer
import time

from skyplane.obj_store.object_store_interface import ObjectStoreObject
from skyplane.utils.definitions import GB, MB
from skyplane.config_paths import cloud_config, config_path
from skyplane.cli.impl.common import console, print_header
from typing import List
from skyplane.broadcast.bc_client import SkyplaneBroadcastClient

app = typer.Typer(name="broadcast")


@app.command()
def replicate_random(
    src_region: str,
    dst_regions: List[str],
    num_gateways: int = typer.Option(1, "--num-gateways", "-n", help="Number of gateways"),
    num_chunks: int = typer.Option(1, "--num-chunks", "-c", help="Number of chunks"),
    # for ILP
    num_partitions: int = typer.Option(10, "--num-partitions", "-p", help="Number of partitions"),
    filter_node: bool = typer.Option(False, "--filter-nodes", "-fn", help="Filter nodes"),
    filter_edge: bool = typer.Option(False, "--filter-edges", "-fe", help="Filter edges"),
    iterative: bool = typer.Option(False, "--iterative", "-i", help="Chunk iterative solve"),
    aws_only: bool = typer.Option(False, "--aws-only", "-aws", help="Use AWS only nodes and edges"),
    gcp_only: bool = typer.Option(False, "--gcp-only", "-gcp", help="Use GCP only nodes and edges"),
    azure_only: bool = typer.Option(False, "--azure-only", "-azure", help="Use Azure only nodes and edges"),
    # transfer flags
    reuse_gateways: bool = typer.Option(False, help="If true, will leave provisioned instances running to be reused"),
    debug: bool = typer.Option(True, help="If true, will write debug information to debug directory."),
    confirm: bool = typer.Option(cloud_config.get_flag("autoconfirm"), "--confirm", "-y", "-f", help="Confirm all transfer prompts"),
    # solver
    algo: str = typer.Option("Ndirect", "--algo", "-a", help="Algorithm selected from [MDST, HST, ILP]"),
    solver_target_time_budget: float = typer.Option(
        10, "--time-budget", "-s", help="Solver option for ILP: Required time budget in seconds"
    ),
    solver_verbose: bool = False,
):
    s_cloud_provider, s_region = src_region.split(":")
    d_cloud_providers = []
    d_regions = []
    for d in dst_regions:
        dst_cloud_provider, dst_region = d.split(":")
        d_cloud_providers.append(dst_cloud_provider)
        d_regions.append(dst_region)

    # create transfer list
    # 8,  80, 800,  1600,   3200, 4000, 4800    8000(n_chunks)
    # 0.5, 5,  50,   100,   200,   250,  300     500 (transfer siz2e in GB)
    # 0.8, 8,  80,   160,   320,   400,          800 (seconds for ILP)

    # 200 --> 3200 as the # of chunks
    # 100 --> 1600 as the # of chunks

    random_chunk_size_mb = 64
    transfer_size_gbytes = num_chunks * random_chunk_size_mb * MB / GB

    client = SkyplaneBroadcastClient(
        aws_config=skyplane.AWSConfig(),
        # all for transfer configs, might be better way to do this
        multipart_enabled=False,
        generate_random=True,
        num_random_chunks=num_chunks,
        random_chunk_size_mb=random_chunk_size_mb,
        src_region=src_region,
        dst_regions=dst_regions,
    )
    print(f"Log dir: {client.log_dir}/client.log")
    print("Transfer size (in GB): ", transfer_size_gbytes)

    # use existing gw program file 
    #dp = client.broadcast_dataplane_from_gateway_program("/Users/sarahwooders/repos/skyplane/old_gw_programs/gateway_programs_complete.json")

    dp = client.broadcast_dataplane(
        src_cloud_provider=s_cloud_provider,
        src_region=s_region,
        dst_cloud_providers=d_cloud_providers,
        dst_regions=d_regions,
        type=algo,
        n_vms=int(num_gateways),
        num_partitions=int(num_partitions),
        gbyte_to_transfer=transfer_size_gbytes,  # 171.78460 for image net
        target_time=solver_target_time_budget,
        filter_node=filter_node,
        filter_edge=filter_edge,
        solve_iterative=iterative,
        aws_only=aws_only,
        gcp_only=gcp_only,
        azure_only=azure_only,
    )

    with dp.auto_deprovision():
        # not doing anything
        dp.queue_copy(
            "",
            [],
        )

        dp.provision(allow_firewall=False, spinner=True)
        tracker = dp.run_async()

        # monitor the transfer
        print("Waiting for transfer to complete...")
        while True:
            # handle errors
            if tracker.errors:
                for ip, error_list in tracker.errors.items():
                    for error in error_list:
                        print(f"Error on {ip}: {error}")
                break

            bytes_remaining, _ = tracker.query_bytes_remaining()
            timestamp = time.strftime("%H:%M:%S", time.localtime())
            if bytes_remaining is None:
                print(f"{timestamp} Transfer not yet started")
            elif bytes_remaining > 0:
                print(f"{timestamp} {(bytes_remaining / (2 ** 30)):.5f}GB left")
            else:
                break
            time.sleep(10)
        tracker.join()
        print("Transfer complete!")


if __name__ == "__main__":
    app()
