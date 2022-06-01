import json
import os
import pathlib
import signal
from typing import List, Optional

import typer
from halo import Halo

from skyplane import exceptions, MB, GB, skyplane_root
from skyplane.obj_store.object_store_interface import ObjectStoreInterface, ObjectStoreObject
from skyplane.replicate.replication_plan import ReplicationTopology, ReplicationJob
from skyplane.replicate.replicator_client import ReplicatorClient
from skyplane.utils import logger
from skyplane.utils.timer import Timer


def generate_topology(
    src_region: str,
    dst_region: str,
    solve: bool,
    num_connections: int = 32,
    max_instances: int = 1,
    solver_total_gbyte_to_transfer: Optional[float] = None,
    solver_required_throughput_gbits: float = 4,
    solver_throughput_grid: Optional[pathlib.Path] = skyplane_root / "profiles" / "throughput.csv",
    solver_verbose: Optional[bool] = False,
) -> ReplicationTopology:

    if solve:
        if src_region == dst_region:
            typer.secho("Solver is not supported for intra-region transfers, run without the --solve flag", fg="red")
            raise typer.Exit(1)

        # build problem and solve
        from skyplane.replicate.solver import ThroughputProblem
        from skyplane.replicate.solver_ilp import ThroughputSolverILP

        assert solver_throughput_grid is not None and solver_total_gbyte_to_transfer is not None
        tput = ThroughputSolverILP(solver_throughput_grid)
        problem = ThroughputProblem(
            src=src_region,
            dst=dst_region,
            required_throughput_gbits=solver_required_throughput_gbits,
            gbyte_to_transfer=solver_total_gbyte_to_transfer,
            instance_limit=max_instances,
        )
        with Halo(text="Solving...", spinner="dots"):
            solution = tput.solve_min_cost(
                problem,
                solver=ThroughputSolverILP.choose_solver(),
                solver_verbose=solver_verbose,
                save_lp_path=None,
            )
        topo, _ = tput.to_replication_topology(solution)
        return topo
    else:
        if src_region == dst_region:
            topo = ReplicationTopology()
            for i in range(max_instances):
                topo.add_objstore_instance_edge(src_region, src_region, i)
                topo.add_instance_objstore_edge(src_region, i, src_region)
        else:
            topo = ReplicationTopology()
            for i in range(max_instances):
                topo.add_objstore_instance_edge(src_region, src_region, i)
                topo.add_instance_instance_edge(src_region, i, dst_region, i, num_connections)
                topo.add_instance_objstore_edge(dst_region, i, dst_region)
        return topo


def replicate_helper(
    topo: ReplicationTopology,
    size_total_mb: int = 2048,
    n_chunks: int = 512,
    random: bool = False,
    # bucket options
    source_bucket: Optional[str] = None,
    dest_bucket: Optional[str] = None,
    src_key_prefix: str = "",
    dest_key_prefix: str = "",
    cached_src_objs: Optional[List[ObjectStoreObject]] = None,
    # maximum chunk size to breakup objects into
    max_chunk_size_mb: Optional[int] = None,
    # gateway provisioning options
    reuse_gateways: bool = False,
    gateway_docker_image: str = os.environ.get("SKYPLANE_DOCKER_IMAGE", "ghcr.io/skyplane-project/skyplane:main"),
    debug: bool = False,
    use_bbr: bool = False,
    use_compression: bool = False,
    # cloud provider specific options
    aws_instance_class: str = "m5.8xlarge",
    azure_instance_class: str = "Standard_D32_v4",
    gcp_instance_class: Optional[str] = "n2-standard-32",
    gcp_use_premium_network: bool = True,
    # logging options
    time_limit_seconds: Optional[int] = None,
    log_interval_s: float = 1.0,
):
    if "SKYPLANE_DOCKER_IMAGE" in os.environ:
        logger.debug(f"Using docker image: {gateway_docker_image}")
    if reuse_gateways:
        typer.secho(
            f"Instances will remain up and may result in continued cloud billing. Remember to call `skyplane deprovision` to deprovision gateways.",
            fg="red",
            bold=True,
        )

    # make replicator client
    rc = ReplicatorClient(
        topo,
        gateway_docker_image=gateway_docker_image,
        aws_instance_class=aws_instance_class,
        azure_instance_class=azure_instance_class,
        gcp_instance_class=gcp_instance_class,
        gcp_use_premium_network=gcp_use_premium_network,
    )
    typer.secho(f"Storing debug information for transfer in {rc.transfer_dir / 'client.log'}", fg="yellow")
    (rc.transfer_dir / "topology.json").write_text(topo.to_json())

    if random:
        random_chunk_size_mb = size_total_mb // n_chunks
        if max_chunk_size_mb:
            logger.error("Cannot set chunk size for random data replication, set `random_chunk_size_mb` instead")
            raise ValueError("Cannot set max chunk size")
        job = ReplicationJob(
            source_region=topo.source_region(),
            source_bucket=None,
            dest_region=topo.sink_region(),
            dest_bucket=None,
            src_objs=[str(i) for i in range(n_chunks)],
            dest_objs=[str(i) for i in range(n_chunks)],
            random_chunk_size_mb=random_chunk_size_mb,
        )
    else:
        # make replication job
        logger.fs.debug(f"Creating replication job from {source_bucket} to {dest_bucket}")
        if cached_src_objs:
            src_objs = cached_src_objs
        else:
            source_iface = ObjectStoreInterface.create(topo.source_region(), source_bucket)
            logger.fs.debug(f"Querying objects in {source_bucket}")
            with Timer(f"Query {source_bucket} prefix {src_key_prefix}"):
                with Halo(text=f"Querying objects in {source_bucket}", spinner="dots") as spinner:
                    src_objs = []
                    for obj in source_iface.list_objects(src_key_prefix):
                        src_objs.append(obj)
                        spinner.text = f"Querying objects in {source_bucket} ({len(src_objs)} objects)"

        if not src_objs:
            logger.error("Specified object does not exist.")
            raise exceptions.MissingObjectException()

        # map objects to destination object paths
        # todo isolate this logic and test independently
        logger.fs.debug(f"Mapping objects to destination paths")
        src_objs_job = []
        dest_objs_job = []
        # if only one object exists, replicate it
        if len(src_objs) == 1 and src_objs[0].key == src_key_prefix:
            src_objs_job.append(src_objs[0].key)
            if dest_key_prefix.endswith("/"):
                dest_objs_job.append(dest_key_prefix + src_objs[0].key.split("/")[-1])
            else:
                dest_objs_job.append(dest_key_prefix)
        # multiple objects to replicate
        else:
            for src_obj in src_objs:
                src_objs_job.append(src_obj.key)
                # remove prefix from object key
                src_path_no_prefix = src_obj.key[len(src_key_prefix) :] if src_obj.key.startswith(src_key_prefix) else src_obj.key
                # remove single leading slash if present
                src_path_no_prefix = src_path_no_prefix[1:] if src_path_no_prefix.startswith("/") else src_path_no_prefix
                if len(dest_key_prefix) == 0:
                    dest_objs_job.append(src_path_no_prefix)
                elif dest_key_prefix.endswith("/"):
                    dest_objs_job.append(dest_key_prefix + src_path_no_prefix)
                else:
                    dest_objs_job.append(dest_key_prefix + "/" + src_path_no_prefix)
        job = ReplicationJob(
            source_region=topo.source_region(),
            source_bucket=source_bucket,
            dest_region=topo.sink_region(),
            dest_bucket=dest_bucket,
            src_objs=src_objs_job,
            dest_objs=dest_objs_job,
            obj_sizes={obj.key: obj.size for obj in src_objs},
            max_chunk_size_mb=max_chunk_size_mb,
        )

    stats = {}
    try:
        rc.provision_gateways(reuse_gateways, use_bbr=use_bbr, use_compression=use_compression)
        for node, gw in rc.bound_nodes.items():
            logger.fs.info(f"Log URLs for {gw.uuid()} ({node.region}:{node.instance})")
            logger.fs.info(f"\tLog viewer: {gw.gateway_log_viewer_url}")
            logger.fs.info(f"\tAPI: {gw.gateway_api_url}")
        job = rc.run_replication_plan(job)
        if random:
            total_bytes = n_chunks * random_chunk_size_mb * MB
        else:
            total_bytes = sum([chunk_req.chunk.chunk_length_bytes for chunk_req in job.chunk_requests])
        typer.secho(f"{total_bytes / GB:.2f}GByte replication job launched", fg="green")
        if topo.source_region().split(":")[0] == "azure" or topo.sink_region().split(":")[0] == "azure":
            typer.secho(f"Warning: It can take up to 60s for role assignments to propagate on Azure. See issue #355", fg="yellow")
        stats = rc.monitor_transfer(
            job,
            show_spinner=True,
            log_interval_s=log_interval_s,
            time_limit_seconds=time_limit_seconds,
            multipart=max_chunk_size_mb is not None,
            write_profile=debug,
            write_socket_profile=debug,
            copy_gateway_logs=debug,
        )
    except KeyboardInterrupt:
        if not reuse_gateways:
            logger.fs.warning("Deprovisioning gateways then exiting...")
            # disable sigint to prevent repeated KeyboardInterrupts
            s = signal.signal(signal.SIGINT, signal.SIG_IGN)
            rc.deprovision_gateways()
            signal.signal(signal.SIGINT, s)

        stats["success"] = False
        out_json = {k: v for k, v in stats.items() if k not in ["log", "completed_chunk_ids"]}
        typer.echo(f"\n{json.dumps(out_json)}")
        os._exit(1)  # exit now
    if not reuse_gateways:
        s = signal.signal(signal.SIGINT, signal.SIG_IGN)
        rc.deprovision_gateways()
        signal.signal(signal.SIGINT, s)
    stats = stats if stats else {}
    stats["success"] = stats["monitor_status"] == "completed"

    if stats["monitor_status"] == "error":
        for instance, errors in stats["errors"].items():
            for error in errors:
                typer.secho(f"\n❌ {instance} encountered error:", fg="red", bold=True)
                typer.secho(error, fg="red")
        raise typer.Exit(1)

    out_json = {k: v for k, v in stats.items() if k not in ["log", "completed_chunk_ids"]}
    typer.echo(f"\n{json.dumps(out_json)}")
    return 0 if stats["success"] else 1
