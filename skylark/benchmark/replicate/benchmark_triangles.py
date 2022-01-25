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
    chunk_size_mb: int = 8,
    n_chunks: int = 2048,
    gcp_project: str = "skylark-333700",
    gateway_docker_image: str = "ghcr.io/parasj/skylark:main",
    aws_instance_class: str = "m5.8xlarge",
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
            topo = ReplicationTopology()
            for i in range(num_gateways):
                topo.add_edge(src_region, i, inter_region, i, num_outgoing_connections)
                topo.add_edge(inter_region, i, dst_region, i, num_outgoing_connections)
        else:
            topo = ReplicationTopology()
            for i in range(num_gateways):
                topo.add_edge(src_region, i, dst_region, i, num_outgoing_connections)
        rc = ReplicatorClient(
            topo,
            gcp_project=gcp_project,
            gateway_docker_image=gateway_docker_image,
            aws_instance_class=aws_instance_class,
            gcp_instance_class=gcp_instance_class,
            gcp_use_premium_network=gcp_use_premium_network,
        )

        rc.provision_gateways(reuse_instances=False)
        atexit.register(rc.deprovision_gateways)
        for node, gw in rc.bound_nodes:
            logger.info(f"Provisioned {node}: {gw.gateway_log_viewer_url}")

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
        stats = rc.monitor_transfer(crs, show_pbar=False, time_limit_seconds=600)
        stats["success"] = True
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

    logger.info(f"Stats:")
    for k, v in stats.items():
        if k not in ["log", "completed_chunk_ids"]:
            logger.info(f"\t{k}: {v}")

    # compute hash of src_region, dst_region, inter_region, num_gateways, num_outgoing_connections, chunk_size_mb, n_chunks
    arg_hash = hash((src_region, dst_region, inter_region, num_gateways, num_outgoing_connections, chunk_size_mb, n_chunks))
    with open(result_dir / f"{arg_hash}.pkl", "wb") as f:
        pickle.dump(stats, f)


if __name__ == "__main__":
    typer.run(bench_triangle)
