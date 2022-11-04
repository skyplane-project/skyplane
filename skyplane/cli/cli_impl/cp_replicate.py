import os
import pathlib
import signal
import sys
import traceback

import typer
from rich import print as rprint
from typing import List, Optional, Tuple, Dict

from skyplane import compute
from skyplane import exceptions
from skyplane.cli.common import console
from skyplane.cli.usage.client import UsageClient
from skyplane.config_paths import cloud_config
from skyplane.obj_store.azure_blob_interface import AzureBlobObject
from skyplane.obj_store.gcs_interface import GCSObject
from skyplane.obj_store.object_store_interface import ObjectStoreInterface, ObjectStoreObject
from skyplane.obj_store.s3_interface import S3Object
from skyplane.replicate.replication_plan import ReplicationTopology, ReplicationJob
from skyplane.replicate.replicator_client import ReplicatorClient, TransferStats
from skyplane.utils import logger
from skyplane.utils.definitions import GB, format_bytes, gateway_docker_image
from skyplane.utils.timer import Timer


def generate_topology(
    src_region: str,
    dst_region: str,
    solve: bool,
    num_connections: int = 32,
    max_instances: int = 1,
    solver_class: str = "ILP",
    solver_total_gbyte_to_transfer: Optional[float] = None,
    solver_target_tput_per_vm_gbits: Optional[float] = None,
    solver_throughput_grid: Optional[pathlib.Path] = None,
    solver_verbose: Optional[bool] = False,
    args: Optional[Dict] = None,
) -> ReplicationTopology:
    if src_region == dst_region:  # intra-region transfer w/o solver
        topo = ReplicationTopology()
        for i in range(max_instances):
            topo.add_objstore_instance_edge(src_region, src_region, i)
            topo.add_instance_objstore_edge(src_region, i, src_region)
        topo.cost_per_gb = 0
        return topo
    elif solve:
        from skyplane.replicate.solver import ThroughputProblem

        if src_region == dst_region:
            e = "Solver is not supported for intra-region transfers, run without the --solve flag"
            typer.secho(e, fg="red", err=True)
            UsageClient.log_exception("generate_topology", exceptions.SkyplaneException(e), args, src_region, dst_region)
            raise typer.Exit(1)
        assert solver_throughput_grid is not None and solver_total_gbyte_to_transfer is not None
        solver_required_throughput_gbits = (
            solver_target_tput_per_vm_gbits * max_instances if solver_target_tput_per_vm_gbits is not None else None
        )
        problem = ThroughputProblem(
            src=src_region,
            dst=dst_region,
            required_throughput_gbits=solver_required_throughput_gbits,
            gbyte_to_transfer=solver_total_gbyte_to_transfer,
            instance_limit=max_instances,
        )

        if solver_class == "ILP":
            from skyplane.replicate.solver_ilp import ThroughputSolverILP

            tput = ThroughputSolverILP(solver_throughput_grid)
            with Timer() as t:
                with console.status("Solving for the optimal transfer plan"):
                    solution = tput.solve_min_cost(
                        problem, solver=ThroughputSolverILP.choose_solver(), solver_verbose=solver_verbose, save_lp_path=None
                    )
            typer.secho(f"Solving for the optimal transfer plan took {t.elapsed:.2f}s", fg="green")
            topo, scale_factor = tput.to_replication_topology(solution)
            logger.fs.debug(f"Scaled solution by {scale_factor:.2f}x")
            topo.cost_per_gb = solution.cost_egress / solution.problem.gbyte_to_transfer
            return topo
        elif solver_class == "RON":
            from skyplane.replicate.solver_ron import ThroughputSolverRON

            tput = ThroughputSolverRON(solver_throughput_grid)
            solution = tput.solve(problem)
            topo, scale_factor = tput.to_replication_topology(solution)
            topo.cost_per_gb = solution.cost_egress / solution.problem.gbyte_to_transfer
            return topo
        else:
            raise NotImplementedError(f"Solver class {solver_class} not implemented")
    else:  # inter-region transfer w/o solver
        topo = ReplicationTopology()
        for i in range(max_instances):
            topo.add_objstore_instance_edge(src_region, src_region, i)
            topo.add_instance_instance_edge(src_region, i, dst_region, i, num_connections)
            topo.add_instance_objstore_edge(dst_region, i, dst_region)
        topo.cost_per_gb = compute.CloudProvider.get_transfer_cost(src_region, dst_region)
        return topo


def map_object_key_prefix(source_prefix: str, source_key: str, dest_prefix: str, recursive: bool = False):
    """
    map_object_key_prefix computes the mapping of a source key in a bucket prefix to the destination.
    Users invoke a transfer via the CLI; aws s3 cp s3://bucket/source_prefix s3://bucket/dest_prefix.
    The CLI will query the object store for all objects in the source prefix and map them to the
    destination prefix using this function.
    """
    join = lambda prefix, fname: prefix + fname if prefix.endswith("/") else prefix + "/" + fname
    src_fname = source_key.split("/")[-1] if "/" in source_key and not source_key.endswith("/") else source_key
    if not recursive:
        if source_key == source_prefix:
            if dest_prefix == "" or dest_prefix == "/":
                return src_fname
            elif dest_prefix[-1] == "/":
                return dest_prefix + src_fname
            else:
                return dest_prefix
        else:
            rprint(f"\n:x: [bold red]In order to transfer objects using a prefix, you must use the --recursive or -r flag.[/bold red]")
            rprint(f"[yellow]If you meant to transfer a single object, pass the full source object key.[/yellow]")
            rprint(f"[bright_black]Try running: [bold]skyplane {' '.join(sys.argv[1:])} --recursive[/bold][/bright_black]")
            raise exceptions.MissingObjectException("Encountered a recursive transfer without the --recursive flag.")
    else:
        if source_prefix == "" or source_prefix == "/":
            if dest_prefix == "" or dest_prefix == "/":
                return source_key
            else:
                return join(dest_prefix, source_key)
        else:
            # catch special case: map_object_key_prefix("foo", "foobar/baz.txt", "", recursive=True)
            if not source_key.startswith(source_prefix + "/" if not source_prefix.endswith("/") else source_prefix):
                rprint(f"\n:x: [bold red]The source key {source_key} does not start with the source prefix {source_prefix}[/bold red]")
                raise exceptions.MissingObjectException(f"Source key {source_key} does not start with source prefix {source_prefix}")
            if dest_prefix == "" or dest_prefix == "/":
                return source_key[len(source_prefix) :]
            else:
                src_path_after_prefix = source_key[len(source_prefix) :]
                src_path_after_prefix = src_path_after_prefix[1:] if src_path_after_prefix.startswith("/") else src_path_after_prefix
                return join(dest_prefix, src_path_after_prefix)


def generate_full_transferobjlist(
    source_region: str,
    source_bucket: str,
    source_prefix: str,
    dest_region: str,
    dest_bucket: str,
    dest_prefix: str,
    recursive: bool = False,
) -> List[Tuple[ObjectStoreObject, ObjectStoreObject]]:
    """Query source region and return list of objects to transfer."""
    source_iface = ObjectStoreInterface.create(source_region, source_bucket)
    dest_iface = ObjectStoreInterface.create(dest_region, dest_bucket)

    requester_pays = cloud_config.get_flag("requester_pays")
    if requester_pays:
        source_iface.set_requester_bool(True)

    # ensure buckets exist
    if not source_iface.bucket_exists():
        raise exceptions.MissingBucketException(f"Source bucket {source_bucket} does not exist")
    if not dest_iface.bucket_exists():
        raise exceptions.MissingBucketException(f"Destination bucket {dest_bucket} does not exist")
    source_objs, dest_objs = [], []

    # query all source region objects
    logger.fs.debug(f"Querying objects in {source_bucket}")
    with console.status(f"Querying objects in {source_bucket}") as status:
        for obj in source_iface.list_objects(source_prefix):
            source_objs.append(obj)
            status.update(f"Querying objects in {source_bucket} (found {len(source_objs)} objects so far)")
    if not source_objs:
        logger.error("Specified object does not exist.\n")
        raise exceptions.MissingObjectException(f"No objects were found in the specified prefix {source_prefix} in {source_bucket}")

    # map objects to destination object paths
    for source_obj in source_objs:
        try:
            dest_key = map_object_key_prefix(source_prefix, source_obj.key, dest_prefix, recursive=recursive)
        except exceptions.MissingObjectException:
            raise typer.Exit(1)
        if dest_region.startswith("aws"):
            dest_obj = S3Object(dest_region.split(":")[0], dest_bucket, dest_key, mime_type=source_obj.mime_type)
        elif dest_region.startswith("gcp"):
            dest_obj = GCSObject(dest_region.split(":")[0], dest_bucket, dest_key, mime_type=source_obj.mime_type)
        elif dest_region.startswith("azure"):
            dest_obj = AzureBlobObject(dest_region.split(":")[0], dest_bucket, dest_key, mime_type=source_obj.mime_type)
        else:
            raise ValueError(f"Invalid dest_region {dest_region} - could not create corresponding object")
        # dest_obj = ObjectStoreObject(dest_region.split(":")[0], dest_bucket, dest_key)
        dest_objs.append(dest_obj)

    return list(zip(source_objs, dest_objs))


def enrich_dest_objs(dest_region: str, dest_prefix: str, dest_bucket: str, dest_objs: list):
    """
    For skyplane sync, we enrich dest obj metadata with our existing dest obj metadata from the dest bucket following a query.
    """
    dest_iface = ObjectStoreInterface.create(dest_region, dest_bucket)

    # query destination at dest_key
    logger.fs.debug(f"Querying objects in {dest_bucket}")
    dest_objs_keys = {obj.key for obj in dest_objs}
    found_dest_objs = {}
    with console.status(f"Querying objects in {dest_bucket}") as status:
        dst_objs = []
        for obj in dest_iface.list_objects(dest_prefix):
            if obj.key in dest_objs_keys:
                found_dest_objs[obj.key] = obj
            status.update(f"Querying objects in {dest_bucket} (found {len(dst_objs)} objects so far)")

    # enrich dest_objs with found_dest_objs
    for dest_obj in dest_objs:
        if dest_obj.key in found_dest_objs:
            dest_obj.size = found_dest_objs[dest_obj.key].size
            dest_obj.last_modified = found_dest_objs[dest_obj.key].last_modified


def confirm_transfer(topo: ReplicationTopology, job: ReplicationJob, ask_to_confirm_transfer=True):
    console.print(
        f"\n[bold yellow]Will transfer {len(job.transfer_pairs)} objects totaling {format_bytes(job.transfer_size)} from {job.source_region} to {job.dest_region}[/bold yellow]"
    )
    sorted_counts = sorted(topo.per_region_count().items(), key=lambda x: x[0])
    console.print(
        f"    [bold][blue]VMs to provision:[/blue][/bold] [bright_black]{', '.join(f'{c}x {r}' for r, c in sorted_counts)}[/bright_black]"
    )
    if topo.cost_per_gb:
        console.print(
            f"    [bold][blue]Estimated egress cost:[/blue][/bold] [bright_black]${job.transfer_size / GB * topo.cost_per_gb:,.2f} at ${topo.cost_per_gb:,.2f}/GB[/bright_black]"
        )

    # print list of objects to transfer if not a random transfer
    if not job.random_chunk_size_mb:
        for src, dst in job.transfer_pairs[:4]:
            console.print(f"    [bright_black][bold]{src.key}[/bold] => [bold]{dst.key}[/bold][/bright_black]")
        if len(job.transfer_pairs) > 4:
            console.print(f"    [bright_black][bold]...[/bold][/bright_black]")
            for src, dst in job.transfer_pairs[4:][-4:]:
                console.print(f"    [bright_black][bold]{src.key}[/bold] => [bold]{dst.key}[/bold][/bright_black]")

    if ask_to_confirm_transfer:
        if typer.confirm("Continue?", default=True):
            logger.fs.debug("User confirmed transfer")
            console.print(
                "[bold green]Transfer starting[/bold green] (Tip: Enable auto-confirmation with `skyplane config set autoconfirm true`)"
            )
        else:
            logger.fs.error("Transfer cancelled by user.")
            console.print("[bold][red]Transfer cancelled by user.[/red][/bold]")
            raise typer.Abort()
    console.print("")


def launch_replication_job(
    topo: ReplicationTopology,
    job: ReplicationJob,
    gateway_docker_image: str = os.environ.get("SKYPLANE_DOCKER_IMAGE", gateway_docker_image()),
    # transfer flags
    debug: bool = False,
    reuse_gateways: bool = False,
    use_bbr: bool = False,
    use_compression: bool = False,
    use_e2ee: bool = True,
    use_socket_tls: bool = False,
    # multipart
    multipart_enabled: bool = False,
    multipart_min_threshold_mb: int = 128,
    multipart_chunk_size_mb: int = 64,
    multipart_max_chunks: int = 9990,
    # cloud provider specific options
    aws_use_spot_instances: bool = False,
    aws_instance_class: str = "m5.8xlarge",
    azure_use_spot_instances: bool = False,
    azure_instance_class: str = "Standard_D32_v4",
    gcp_use_spot_instances: bool = False,
    gcp_instance_class: str = "n2-standard-32",
    gcp_use_premium_network: bool = True,
    # logging options
    time_limit_seconds: Optional[int] = None,
    log_interval_s: float = 1.0,
    error_reporting_args: Optional[Dict] = None,
    host_uuid: Optional[str] = None,
):
    if "SKYPLANE_DOCKER_IMAGE" in os.environ:
        rprint(f"[bright_black]Using overridden docker image: {gateway_docker_image}[/bright_black]")
    if reuse_gateways:
        typer.secho(
            f"Instances will remain up and may result in continued cloud billing. Remember to call `skyplane deprovision` to deprovision gateways.",
            fg="red",
            err=True,
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
        host_uuid=host_uuid,
    )
    typer.secho(f"Storing debug information for transfer in {rc.transfer_dir / 'client.log'}", fg="yellow", err=True)
    (rc.transfer_dir / "topology.json").write_text(topo.to_json())

    stats = TransferStats.empty()
    try:
        rc.provision_gateways(
            reuse_gateways,
            use_bbr=use_bbr,
            use_compression=use_compression,
            use_e2ee=use_e2ee,
            use_socket_tls=use_socket_tls,
            aws_use_spot_instances=aws_use_spot_instances,
            azure_use_spot_instances=azure_use_spot_instances,
            gcp_use_spot_instances=gcp_use_spot_instances,
        )
        for node, gw in rc.bound_nodes.items():
            logger.fs.info(f"Log URLs for {gw.uuid()} ({node.region}:{node.instance})")
            logger.fs.info(f"\tLog viewer: {gw.gateway_log_viewer_url}")
            logger.fs.info(f"\tAPI: {gw.gateway_api_url}")
        job = rc.run_replication_plan(
            job,
            multipart_enabled=multipart_enabled,
            multipart_min_threshold_mb=multipart_min_threshold_mb,
            multipart_chunk_size_mb=multipart_chunk_size_mb,
            multipart_max_chunks=multipart_max_chunks,
        )
        total_bytes = sum([chunk_req.chunk.chunk_length_bytes for chunk_req in job.chunk_requests])
        console.print(f":rocket: [bold blue]{total_bytes / GB:.2f}GB transfer job launched[/bold blue]")

        stats = rc.monitor_transfer(
            job,
            show_spinner=True,
            log_interval_s=log_interval_s,
            log_to_file=True,
            time_limit_seconds=time_limit_seconds,
            multipart=multipart_enabled,
            debug=debug,
        )
        error_occurred = False
    except KeyboardInterrupt:
        logger.fs.warning("Transfer cancelled by user (KeyboardInterrupt)")
        rprint("\n[bold red]Transfer cancelled by user. Exiting.[/bold red]")
        error_occurred = True
    except exceptions.SkyplaneException as e:
        logger.fs.exception(e)
        console.print(f"[bright_black]{traceback.format_exc()}[/bright_black]")
        console.print(e.pretty_print_str())
        UsageClient.log_exception("launch_replication_job", e, error_reporting_args, job.source_region, job.dest_region)
        error_occurred = True
    except Exception as e:
        logger.fs.exception(e)
        console.print(f"[bright_black]{traceback.format_exc()}[/bright_black]")
        console.print(e)
        UsageClient.log_exception("launch_replication_job", e, error_reporting_args, job.source_region, job.dest_region)
        error_occurred = True

    if not reuse_gateways:
        logger.fs.warning("Deprovisioning gateways then exiting. Please wait...")
        s = signal.signal(signal.SIGINT, signal.SIG_IGN)
        rc.deprovision_gateways()
        signal.signal(signal.SIGINT, s)

    # handle errors
    if error_occurred:  # client error
        logger.fs.error("Exiting as an error occurred")
        os._exit(1)  # exit now
    if stats.monitor_status == "error":  # gateway error
        err = ""
        for instance, errors in stats.errors.items():
            for error in errors:
                typer.secho(f"\n❌ {instance} encountered error:", fg="red", err=True, bold=True)
                typer.secho(error, fg="red", err=True)
                err += error + "\n"
        UsageClient.log_exception(
            "replicate_monitor", exceptions.SkyplaneException(err), error_reporting_args, job.source_region, job.dest_region
        )
        raise typer.Exit(1)
    elif stats.monitor_status == "completed":
        pass  # success message will be handled by the caller
    else:
        rprint(f"\n:x: [bold red]Transfer failed[/bold red]")
        rprint(stats)
        UsageClient.log_exception(
            "replicate_monitor",
            exceptions.SkyplaneException(stats.monitor_status),
            error_reporting_args,
            job.source_region,
            job.dest_region,
        )
    return stats
