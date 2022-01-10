import atexit
from datetime import datetime
import pickle
from pathlib import Path

import typer
from loguru import logger
from skylark import GB, MB, skylark_root
from skylark.replicate.replication_plan import ReplicationJob, ReplicationTopology
from skylark.replicate.replicator_client import ReplicatorClient


def bench_triangle(
    src_region: str,
    dst_region: str,
    inter_region: str = None,
    log_dir: Path = None,
    num_gateways: int = 1,
    num_outgoing_connections: int = 16,
    chunk_size_mb: int = 16,
    n_chunks: int = 512,
    gcp_project: str = "skylark-333700",
    gateway_docker_image: str = "ghcr.io/parasj/skylark:main",
    aws_instance_class: str = "m5.4xlarge",
    gcp_instance_class: str = None,
    gcp_use_premium_network: bool = False,
    key_prefix: str = "/test/benchmark_triangles",
):
    if log_dir is None:
        log_dir = skylark_root / "data" / "experiments" / "benchmark_triangles" / datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_dir.mkdir(exist_ok=True, parents=True)
    result_dir = log_dir / "results"
    result_dir.mkdir(exist_ok=True, parents=True)

    try:
        if inter_region:
            topo = ReplicationTopology(paths=[[src_region, inter_region, dst_region] for _ in range(num_gateways)])
            num_conn = num_outgoing_connections
        else:
            topo = ReplicationTopology(paths=[[src_region, dst_region] for _ in range(num_gateways)])
            num_conn = num_outgoing_connections
        rc = ReplicatorClient(
            topo,
            gcp_project=gcp_project,
            gateway_docker_image=gateway_docker_image,
            aws_instance_class=aws_instance_class,
            gcp_instance_class=gcp_instance_class,
            gcp_use_premium_network=gcp_use_premium_network,
        )

        rc.provision_gateways(
            reuse_instances=False,
            num_outgoing_connections=num_conn,
        )
        atexit.register(rc.deprovision_gateways)
        for path in rc.bound_paths:
            logger.info(f"Provisioned path {' -> '.join(path[i].region_tag for i in range(len(path)))}")
            for gw in path:
                logger.info(f"\t[{gw.region_tag}] {gw.gateway_log_viewer_url}")

        job = ReplicationJob(
            source_region=src_region,
            source_bucket=None,
            dest_region=dst_region,
            dest_bucket=None,
            objs=[f"{key_prefix}/{i}" for i in range(n_chunks)],
            random_chunk_size_mb=chunk_size_mb,
        )

        total_bytes = n_chunks * chunk_size_mb * MB
        crs = rc.run_replication_plan(job)
        logger.info(f"{total_bytes / GB:.2f}GByte replication job launched")
        stats = rc.monitor_transfer(crs, serve_web_dashboard=False, time_limit_seconds=10 * 60)
        logger.info(f"Stats: {stats}")
        stats["success"] = stats["monitor_status"] == "success"
        stats["log"] = rc.get_chunk_status_log_df()

        rc.deprovision_gateways()
    except Exception as e:
        logger.error(f"Failed to benchmark triangle {src_region} -> {dst_region}")
        logger.exception(e)

        stats = {}
        stats["error"] = str(e)
        stats["success"] = False

    stats["src_region"] = src_region
    stats["dst_region"] = dst_region
    stats["inter_region"] = inter_region
    stats["num_gateways"] = num_gateways
    stats["num_outgoing_connections"] = num_outgoing_connections
    stats["chunk_size_mb"] = chunk_size_mb
    stats["n_chunks"] = n_chunks

    logger.info(f"Stats: {stats}")
    with open(result_dir / f"src={src_region}_inter={inter_region}_dst={dst_region}.pkl", "wb") as f:
        pickle.dump(stats, f)


if __name__ == "__main__":
    typer.run(bench_triangle)
