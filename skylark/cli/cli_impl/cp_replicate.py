import json
import os
import signal
from typing import Optional

from halo import Halo
import typer

from skylark import exceptions, MB, GB
from skylark.obj_store.object_store_interface import ObjectStoreInterface
from skylark.replicate.replication_plan import ReplicationTopology, ReplicationJob
from skylark.replicate.replicator_client import ReplicatorClient
from skylark.utils import logger
from skylark.utils.utils import Timer


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
    # maximum chunk size to breakup objects into
    max_chunk_size_mb: Optional[int] = None,
    # gateway provisioning options
    reuse_gateways: bool = False,
    gateway_docker_image: str = os.environ.get("SKYLARK_DOCKER_IMAGE", "ghcr.io/skyplane-project/skyplane:main"),
    use_bbr: bool = False,
    # cloud provider specific options
    aws_instance_class: str = "m5.8xlarge",
    azure_instance_class: str = "Standard_D32_v4",
    gcp_instance_class: Optional[str] = "n2-standard-32",
    gcp_use_premium_network: bool = True,
    # logging options
    time_limit_seconds: Optional[int] = None,
    log_interval_s: float = 1.0,
):
    if "SKYLARK_DOCKER_IMAGE" in os.environ:
        logger.debug(f"Using docker image: {gateway_docker_image}")
    if reuse_gateways:
        typer.secho(
            f"Instances will remain up and may result in continued cloud billing. Remember to call `skylark deprovision` to deprovision gateways.",
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
        rc.provision_gateways(reuse_gateways, use_bbr=use_bbr)
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
        stats = rc.monitor_transfer(
            job,
            show_spinner=True,
            log_interval_s=log_interval_s,
            time_limit_seconds=time_limit_seconds,
            multipart=max_chunk_size_mb is not None,
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
