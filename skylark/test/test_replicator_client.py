import argparse
from skylark.obj_store.object_store_interface import ObjectStoreInterface

from skylark.utils import logger
from skylark import GB, MB, print_header

import tempfile
import concurrent
import os
from skylark.obj_store.s3_interface import S3Interface
from skylark.obj_store.gcs_interface import GCSInterface

# from skylark.obj_store.azure_interface import AzureInterface

import tempfile
import concurrent
import os
from shutil import copyfile

from skylark.replicate.replication_plan import ReplicationJob, ReplicationTopology
from skylark.replicate.replicator_client import ReplicatorClient

from skylark.config import load_config


def parse_args():
    parser = argparse.ArgumentParser(description="Run a replication job")

    # gateway path parameters
    parser.add_argument("--src-region", default="aws:us-east-1", help="AWS region of source bucket")
    parser.add_argument("--inter-region", default=None, help="AWS region of intermediate bucket")
    parser.add_argument("--dest-region", default="aws:us-west-1", help="AWS region of destination bucket")
    parser.add_argument("--num-gateways", default=1, type=int, help="Number of gateways to use")

    # object information
    parser.add_argument("--key-prefix", default="/test/direct_replication", help="S3 key prefix for all objects")
    parser.add_argument("--chunk-size-mb", default=128, type=int, help="Chunk size in MB")
    parser.add_argument("--n-chunks", default=512, type=int, help="Number of chunks in bucket")
    parser.add_argument("--skip-upload", action="store_true", help="Skip uploading objects to S3")

    # bucket namespace
    parser.add_argument("--bucket-prefix", default="sarah", help="Prefix for bucket to avoid naming collision")

    # gateway provisioning
    parser.add_argument("--gcp-project", default=None, help="GCP project ID")
    parser.add_argument("--azure-subscription", default=None, help="Azure subscription")
    parser.add_argument("--gateway-docker-image", default="ghcr.io/parasj/skylark:main", help="Docker image for gateway instances")
    parser.add_argument("--aws-instance-class", default="m5.4xlarge", help="AWS instance class")
    parser.add_argument("--azure-instance-class", default="Standard_D2_v5", help="Azure instance class")
    parser.add_argument("--gcp-instance-class", default="n2-standard-16", help="GCP instance class")
    parser.add_argument("--copy-ssh-key", default=None, help="SSH public key to add to gateways")
    parser.add_argument("--log-dir", default=None, help="Directory to write instance SSH logs to")
    parser.add_argument("--gcp-use-premium-network", action="store_true", help="Use GCP premium network")
    args = parser.parse_args()

    # add support for None arguments
    if args.aws_instance_class == "None":
        args.aws_instance_class = None
    if args.azure_instance_class == "None":
        args.azure_instance_class = None
    if args.gcp_instance_class == "None":
        args.gcp_instance_class = None

    return args


def main(args):
    config = load_config()
    gcp_project = args.gcp_project or config.get("gcp_project_id")
    azure_subscription = args.azure_subscription or config.get("azure_subscription_id")
    logger.debug(f"Loaded gcp_project: {gcp_project}, azure_subscription: {azure_subscription}")

    src_bucket = f"{args.bucket_prefix}-skylark-{args.src_region.split(':')[1]}"
    dst_bucket = f"{args.bucket_prefix}-skylark-{args.dest_region.split(':')[1]}"
    obj_store_interface_src = ObjectStoreInterface.create(args.src_region, src_bucket)
    obj_store_interface_src.create_bucket()
    obj_store_interface_dst = ObjectStoreInterface.create(args.dest_region, dst_bucket)
    obj_store_interface_dst.create_bucket()

    # TODO: fix this to get the key instead of S3Object
    if not args.skip_upload:
        logger.info(f"Not skipping upload, source bucket is {src_bucket}, destination bucket is {dst_bucket}")

        # TODO: fix this to get the key instead of S3Object
        matching_src_keys = list([obj.key for obj in obj_store_interface_src.list_objects(prefix=args.key_prefix)])
        if matching_src_keys and not args.skip_upload:
            logger.warning(f"Deleting {len(matching_src_keys)} objects from source bucket")
            obj_store_interface_src.delete_objects(matching_src_keys)

        # create test objects w/ random data
        logger.info("Creating test objects")
        obj_keys = []
        futures = []
        tmp_files = []

        # TODO: for n_chunks > 880, get syscall error
        with tempfile.NamedTemporaryFile() as f:
            f.write(os.urandom(int(MB * args.chunk_size_mb)))
            f.seek(0)
            for i in range(args.n_chunks):
                k = f"{args.key_prefix}/{i}"
                tmp_file = f"{f.name}-{i}"
                # need to copy, since GCP API will open file and cause to delete
                copyfile(f.name, f"{f.name}-{i}")
                futures.append(obj_store_interface_src.upload_object(tmp_file, k))
                obj_keys.append(k)
                tmp_files.append(tmp_file)

        logger.info(f"Uploading {len(obj_keys)} to bucket {src_bucket}")
        concurrent.futures.wait(futures)

        matching_dst_keys = list([obj.key for obj in obj_store_interface_dst.list_objects(prefix=args.key_prefix)])
        if matching_dst_keys:
            logger.warning(f"Deleting {len(matching_dst_keys)} objects from destination bucket")
            obj_store_interface_dst.delete_objects(matching_dst_keys)

        # cleanup temp files once done
        for f in tmp_files:
            os.remove(f)
    else:
        obj_keys = [f"{args.key_prefix}/{i}" for i in range(args.n_chunks)]

    # define the replication job and topology
    if args.inter_region:
        topo = ReplicationTopology()
        for i in range(args.num_gateways):
            topo.add_edge(args.src_region, i, args.inter_region, i, args.num_outgoing_connections)
            topo.add_edge(args.inter_region, i, args.dest_region, i, args.num_outgoing_connections)
    else:
        topo = ReplicationTopology()
        for i in range(args.num_gateways):
            topo.add_edge(args.src_region, i, args.dest_region, i, args.num_outgoing_connections)
    logger.info("Creating replication client")

    # Getting configs
    rc = ReplicatorClient(
        topo,
        gcp_project=gcp_project,
        azure_subscription=azure_subscription,
        gateway_docker_image=args.gateway_docker_image,
        aws_instance_class=args.aws_instance_class,
        azure_instance_class=args.azure_instance_class,
        gcp_instance_class=args.gcp_instance_class,
        gcp_use_premium_network=args.gcp_use_premium_network,
    )

    # provision the gateway instances
    logger.info("Provisioning gateway instances")
    rc.provision_gateways(reuse_instances=True, log_dir=args.log_dir, authorize_ssh_pub_key=args.copy_ssh_key)
    for node, gw in rc.bound_nodes.items():
        logger.info(f"Provisioned {node}: {gw.gateway_log_viewer_url}")

    # run replication, monitor progress
    job = ReplicationJob(
        source_region=args.src_region, source_bucket=src_bucket, dest_region=args.dest_region, dest_bucket=dst_bucket, objs=obj_keys
    )

    total_bytes = args.n_chunks * args.chunk_size_mb * MB
    job = rc.run_replication_plan(job)
    logger.info(f"{total_bytes / GB:.2f}GByte replication job launched")
    stats = rc.monitor_transfer(job, show_pbar=True, cancel_pending=False)
    logger.info(f"Replication completed in {stats['total_runtime_s']:.2f}s ({stats['throughput_gbits']:.2f}Gbit/s)")


if __name__ == "__main__":
    print_header()
    main(parse_args())
