from distutils.command.config import config
import os
import tempfile
from pathlib import Path
from typing import Optional

import typer

from skyplane.cli.common import print_header
from skyplane import skyplane_root
from skyplane.cli.cli_impl.cp_replicate import replicate_helper
from skyplane.replicate.replication_plan import ReplicationTopology
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
    gateway_docker_image: str = os.environ.get("SKYPLANE_DOCKER_IMAGE", "ghcr.io/skyplane-project/skyplane:main"),
    aws_instance_class: str = "m5.8xlarge",
    azure_instance_class: str = "Standard_D32_v4",
    gcp_instance_class: Optional[str] = "n2-standard-32",
    gcp_use_premium_network: bool = True,
    time_limit_seconds: Optional[int] = None,
    log_interval_s: float = 1.0,
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
        typer.secho("Replicate random doesn't support replicating to the same region as it tests inter-gateway networks.", fg="red")
        raise typer.Exit(code=1)
    else:
        topo = ReplicationTopology()
        for i in range(num_gateways):
            topo.add_instance_instance_edge(src_region, i, dst_region, i, num_outgoing_connections)

    if total_transfer_size_mb % chunk_size_mb != 0:
        logger.warning(f"total_transfer_size_mb ({total_transfer_size_mb}) is not a multiple of chunk_size_mb ({chunk_size_mb})")
    n_chunks = int(total_transfer_size_mb / chunk_size_mb)

    return replicate_helper(
        topo,
        random_size_total_mb=total_transfer_size_mb,
        random_n_chunks=n_chunks,
        random=True,
        reuse_gateways=reuse_gateways,
        gateway_docker_image=gateway_docker_image,
        aws_instance_class=aws_instance_class,
        gcp_instance_class=gcp_instance_class,
        azure_instance_class=azure_instance_class,
        gcp_use_premium_network=gcp_use_premium_network,
        time_limit_seconds=time_limit_seconds,
        log_interval_s=log_interval_s,
        use_bbr=use_bbr,
        ask_to_confirm_transfer=False,
    )


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
    gateway_docker_image: str = os.environ.get("SKYPLANE_DOCKER_IMAGE", "ghcr.io/skyplane-project/skyplane:main"),
    aws_instance_class: str = "m5.8xlarge",
    azure_instance_class: str = "Standard_D32_v4",
    gcp_instance_class: Optional[str] = "n2-standard-32",
    gcp_use_premium_network: bool = True,
    time_limit_seconds: Optional[int] = None,
    log_interval_s: float = 1.0,
    solve: bool = typer.Option(False, help="If true, will use solver to optimize transfer, else direct path is chosen"),
    solver_required_throughput_gbits: float = typer.Option(2, help="Solver option: Required throughput in gbps."),
    solver_throughput_grid: Path = typer.Option(
        skyplane_root / "profiles" / "throughput.csv", "--throughput-grid", help="Throughput grid file"
    ),
    solver_verbose: bool = False,
):
    """Replicate objects from remote object store to another remote object store."""
    print_header()

    if solve:
        from skyplane.cli.cli_solver import solve_throughput  # lazy import due to pip dependencies

        with tempfile.NamedTemporaryFile(mode="w") as f:
            solve_throughput(
                src_region,
                dst_region,
                solver_required_throughput_gbits,
                gbyte_to_transfer=total_transfer_size_mb / 1024.0,
                max_instances=num_gateways,
                throughput_grid=solver_throughput_grid,
                solver_verbose=solver_verbose,
                out=Path(f.name),
            )
            topo = ReplicationTopology.from_json(Path(f.name).read_text())
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

    return replicate_helper(
        topo,
        random_size_total_mb=total_transfer_size_mb,
        random_n_chunks=n_chunks,
        random=True,
        reuse_gateways=reuse_gateways,
        gateway_docker_image=gateway_docker_image,
        aws_instance_class=aws_instance_class,
        gcp_instance_class=gcp_instance_class,
        azure_instance_class=azure_instance_class,
        gcp_use_premium_network=gcp_use_premium_network,
        time_limit_seconds=time_limit_seconds,
        log_interval_s=log_interval_s,
        use_bbr=use_bbr,
        ask_to_confirm_transfer=False,
    )


def replicate_json(
    path: Path = typer.Argument(..., exists=True, file_okay=True, dir_okay=False, help="Path to JSON file describing replication plan"),
    size_total_mb: int = typer.Option(2048, "--size-total-mb", "-s", help="Total transfer size in MB (across n_chunks chunks)"),
    n_chunks: int = typer.Option(512, "--n-chunks", "-n", help="Number of chunks"),
    # bucket options
    use_random_data: bool = False,
    source_bucket: str = typer.Option(None, "--source-bucket", help="Source bucket url"),
    dest_bucket: str = typer.Option(None, "--dest-bucket", help="Destination bucket url"),
    src_key_prefix: str = "/",
    dest_key_prefix: str = "/",
    # gateway provisioning options
    use_bbr: bool = typer.Option(True, help="If true, will use BBR congestion control"),
    reuse_gateways: bool = False,
    gateway_docker_image: str = os.environ.get("SKYPLANE_DOCKER_IMAGE", "ghcr.io/skyplane-project/skyplane:main"),
    # cloud provider specific options
    aws_instance_class: str = "m5.8xlarge",
    azure_instance_class: str = "Standard_D32_v4",
    gcp_instance_class: Optional[str] = "n2-standard-32",
    gcp_use_premium_network: bool = True,
    # logging options
    time_limit_seconds: Optional[int] = None,
    log_interval_s: float = 1.0,
):
    """Replicate objects from remote object store to another remote object store."""
    print_header()

    with path.open("r") as f:
        topo = ReplicationTopology.from_json(f.read())

    return replicate_helper(
        topo,
        random_size_total_mb=size_total_mb,
        random_n_chunks=n_chunks,
        random=use_random_data,
        source_bucket=source_bucket,
        dest_bucket=dest_bucket,
        src_key_prefix=src_key_prefix,
        dest_key_prefix=dest_key_prefix,
        reuse_gateways=reuse_gateways,
        gateway_docker_image=gateway_docker_image,
        aws_instance_class=aws_instance_class,
        gcp_instance_class=gcp_instance_class,
        azure_instance_class=azure_instance_class,
        gcp_use_premium_network=gcp_use_premium_network,
        time_limit_seconds=time_limit_seconds,
        log_interval_s=log_interval_s,
        use_bbr=use_bbr,
        ask_to_confirm_transfer=False,
    )
