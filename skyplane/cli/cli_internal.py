from pathlib import Path
from typing import Optional

import typer

from skyplane.cli.common import print_header
from skyplane import skyplane_root
from skyplane.cli.cli_impl.cp_replicate import confirm_transfer, launch_replication_job
from skyplane.obj_store.object_store_interface import ObjectStoreObject
from skyplane.replicate.replication_plan import ReplicationTopology, ReplicationJob
from skyplane.utils import logger


def replicate_random(
    src_region: str,
    dst_region: str,
    inter_region: Optional[str] = typer.Argument(None),
    num_gateways: int = typer.Option(1, "--num-gateways", "-n", help="Number of gateways"),
    num_outgoing_connections: int = typer.Option(
        32, "--num-outgoing-connections", "-c", help="Number of outgoing connections between each gateway"
    ),
    total_transfer_size_mb: int = typer.Option(2048, "--size-total-mb", "-s", help="Total transfer size in MB."),
    chunk_size_mb: int = typer.Option(8, "--chunk-size-mb", help="Chunk size in MB."),
    use_bbr: bool = typer.Option(True, help="If true, will use BBR congestion control"),
    reuse_gateways: bool = False,
    debug: bool = False,
):
    """Replicate objects from remote object store to another remote object store."""
    print_header()

    if inter_region:
        assert inter_region not in [src_region, dst_region] and src_region != dst_region
        topo = ReplicationTopology()
        for i in range(num_gateways):
            topo.add_instance_instance_edge(src_region, i, inter_region, i, num_outgoing_connections)
            topo.add_instance_instance_edge(inter_region, i, dst_region, i, num_outgoing_connections)
    elif src_region == dst_region:
        typer.secho(
            "Replicate random doesn't support replicating to the same region as it tests inter-gateway networks.", fg="red", err=True
        )
        raise typer.Exit(code=1)
    else:
        topo = ReplicationTopology()
        for i in range(num_gateways):
            topo.add_instance_instance_edge(src_region, i, dst_region, i, num_outgoing_connections)

    if total_transfer_size_mb % chunk_size_mb != 0:
        logger.warning(f"total_transfer_size_mb ({total_transfer_size_mb}) is not a multiple of chunk_size_mb ({chunk_size_mb})")
    n_chunks = int(total_transfer_size_mb / chunk_size_mb)

    transfer_list = []
    for i in range(n_chunks):
        src_obj = ObjectStoreObject(src_region.split(":")[0], "", str(i))
        dst_obj = ObjectStoreObject(dst_region.split(":")[0], "", str(i))
        transfer_list.append((src_obj, dst_obj))

    job = ReplicationJob(
        source_region=topo.source_region(),
        source_bucket=None,
        dest_region=topo.sink_region(),
        dest_bucket=None,
        transfer_pairs=transfer_list,
        random_chunk_size_mb=total_transfer_size_mb // n_chunks,
    )
    confirm_transfer(topo=topo, job=job, ask_to_confirm_transfer=False)
    stats = launch_replication_job(
        topo=topo, job=job, debug=debug, reuse_gateways=reuse_gateways, use_bbr=use_bbr, use_compression=False, use_e2ee=True
    )
    return 0 if stats.monitor_status == "completed" else 1


def replicate_random_solve(
    src_region: str,
    dst_region: str,
    inter_region: Optional[str] = typer.Argument(None),
    num_gateways: int = typer.Option(1, "--num-gateways", "-n", help="Number of gateways"),
    num_outgoing_connections: int = typer.Option(
        32, "--num-outgoing-connections", "-c", help="Number of outgoing connections between each gateway"
    ),
    total_transfer_size_mb: int = typer.Option(2048, "--size-total-mb", "-s", help="Total transfer size in MB."),
    chunk_size_mb: int = typer.Option(8, "--chunk-size-mb", help="Chunk size in MB."),
    use_bbr: bool = typer.Option(True, help="If true, will use BBR congestion control"),
    reuse_gateways: bool = False,
    solve: bool = typer.Option(False, help="If true, will use solver to optimize transfer, else direct path is chosen"),
    throughput_per_instance_gbits: float = typer.Option(2, help="Solver option: Required throughput in gbps."),
    solver_throughput_grid: Path = typer.Option(
        skyplane_root / "profiles" / "throughput.csv", "--throughput-grid", help="Throughput grid file"
    ),
    solver_verbose: bool = False,
    debug: bool = False,
):
    """Replicate objects from remote object store to another remote object store."""
    print_header()

    if solve:
        logger.error(f"This has been deprecated. Please use the 'skyplane cp' command instead.")
        raise typer.Exit(code=1)
    elif inter_region:
        assert inter_region not in [src_region, dst_region] and src_region != dst_region
        topo = ReplicationTopology()
        for i in range(num_gateways):
            topo.add_instance_instance_edge(src_region, i, inter_region, i, num_outgoing_connections)
            topo.add_instance_instance_edge(inter_region, i, dst_region, i, num_outgoing_connections)
    else:
        assert src_region != dst_region
        topo = ReplicationTopology()
        for i in range(num_gateways):
            topo.add_instance_instance_edge(src_region, i, dst_region, i, num_outgoing_connections)

    if total_transfer_size_mb % chunk_size_mb != 0:
        logger.warning(f"total_transfer_size_mb ({total_transfer_size_mb}) is not a multiple of chunk_size_mb ({chunk_size_mb})")
    n_chunks = int(total_transfer_size_mb / chunk_size_mb)

    transfer_list = []
    for i in range(n_chunks):
        src_obj = ObjectStoreObject(src_region.split(":")[0], "", str(i))
        dst_obj = ObjectStoreObject(dst_region.split(":")[0], "", str(i))
        transfer_list.append((src_obj, dst_obj))

    job = ReplicationJob(
        source_region=topo.source_region(),
        source_bucket=None,
        dest_region=topo.sink_region(),
        dest_bucket=None,
        transfer_pairs=transfer_list,
        random_chunk_size_mb=total_transfer_size_mb // n_chunks,
    )
    confirm_transfer(topo=topo, job=job, ask_to_confirm_transfer=False)
    stats = launch_replication_job(topo=topo, job=job, debug=debug, reuse_gateways=reuse_gateways, use_bbr=use_bbr, use_compression=False)
    return 0 if stats.monitor_status == "completed" else 1
