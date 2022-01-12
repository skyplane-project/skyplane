import argparse
import atexit

from loguru import logger
from skylark import GB, MB, print_header
from skylark.gateway.chunk import ChunkState

import tempfile
import concurrent
import os
from skylark.obj_store.s3_interface import S3Interface

from skylark.replicate.replication_plan import ReplicationJob, ReplicationTopology
from skylark.replicate.replicator_client import ReplicatorClient
from skylark.utils.utils import Timer
from skylark import skylark_root


def parse_args():
    parser = argparse.ArgumentParser(description="Run a replication job")

    # gateway path parameters
    parser.add_argument("--src-region", default="aws:us-east-1", help="AWS region of source bucket")
    parser.add_argument("--inter-region", default=None, help="AWS region of intermediate bucket")
    parser.add_argument("--dest-region", default="aws:us-west-1", help="AWS region of destination bucket")
    parser.add_argument("--num-gateways", default=1, type=int, help="Number of gateways to use")
    parser.add_argument("--num-outgoing-connections", default=16, type=int, help="Number of outgoing connections from a gateway")

    # object information
    parser.add_argument("--key-prefix", default="/test/direct_replication", help="S3 key prefix for all objects")
    parser.add_argument("--chunk-size-mb", default=128, type=int, help="Chunk size in MB")
    parser.add_argument("--n-chunks", default=16, type=int, help="Number of chunks in bucket")
    parser.add_argument("--skip-upload", action="store_true", help="Skip uploading objects to S3")

    # bucket namespace
    parser.add_argument("--bucket-prefix", default="sarah", help="Prefix for bucket to avoid naming collision")

    # gateway provisioning
    parser.add_argument("--gcp-project", default="skylark-333700", help="GCP project ID")
    parser.add_argument("--gateway-docker-image", default="ghcr.io/parasj/skylark:main", help="Docker image for gateway instances")
    parser.add_argument("--aws-instance-class", default="m5.4xlarge", help="AWS instance class")
    parser.add_argument("--gcp-instance-class", default="n2-standard-16", help="GCP instance class")
    parser.add_argument("--copy-ssh-key", default=None, help="SSH public key to add to gateways")
    parser.add_argument("--log-dir", default=None, help="Directory to write instance SSH logs to")
    parser.add_argument("--gcp-use-premium-network", action="store_true", help="Use GCP premium network")
    args = parser.parse_args()

    # add support for None arguments
    if args.aws_instance_class == "None":
        args.aws_instance_class = None
    if args.gcp_instance_class == "None":
        args.gcp_instance_class = None

    return args


def main(args):
    src_bucket = f"{args.bucket_prefix}-skylark-{args.src_region.split(':')[1]}"
    dst_bucket = f"{args.bucket_prefix}-skylark-{args.dest_region.split(':')[1]}"
    s3_interface_src = S3Interface(args.src_region.split(":")[1], src_bucket)
    s3_interface_dst = S3Interface(args.dest_region.split(":")[1], dst_bucket)
    s3_interface_src.create_bucket()
    s3_interface_dst.create_bucket()

    if not args.skip_upload:
        # todo implement object store support
        #pass
        print("Not skipping upload...", src_bucket, dst_bucket)

        # TODO: fix this to get the key instead of S3Object
        matching_src_keys = list([obj.key for obj in s3_interface_src.list_objects(prefix=args.key_prefix)])
        matching_dst_keys = list([obj.key for obj in s3_interface_dst.list_objects(prefix=args.key_prefix)])
        if matching_src_keys:
            logger.warning(f"Deleting objects from source bucket: {matching_src_keys}")
            s3_interface_src.delete_objects(matching_src_keys)
        if matching_dst_keys:
            logger.warning(f"Deleting objects from destination bucket: {matching_dst_keys}")
            s3_interface_dst.delete_objects(matching_dst_keys)

        # create test objects w/ random data
        logger.info("Creating test objects")
        obj_keys = []
        futures = []

        # TODO: for n_chunks > 880, get syscall error
        with tempfile.NamedTemporaryFile() as f:
            f.write(os.urandom(int(MB * args.chunk_size_mb)))
            f.seek(0)
            for i in range(args.n_chunks):
                k = f"{args.key_prefix}/{i}"
                futures.append(s3_interface_src.upload_object(f.name, k))
                print("done", f.name, len(futures))
                obj_keys.append(k)
        print("created all futures")
        concurrent.futures.wait(futures)
    else:
        obj_keys = [f"{args.key_prefix}/{i}" for i in range(args.n_chunks)]

    # define the replication job and topology
    if args.inter_region:
        topo = ReplicationTopology(paths=[[args.src_region, args.inter_region, args.dest_region] for _ in range(args.num_gateways)])
    else:
        topo = ReplicationTopology(paths=[[args.src_region, args.dest_region] for _ in range(args.num_gateways)])
    logger.info("Creating replication client")
    rc = ReplicatorClient(
        topo,
        gcp_project=args.gcp_project,
        gateway_docker_image=args.gateway_docker_image,
        aws_instance_class=args.aws_instance_class,
        gcp_instance_class=args.gcp_instance_class,
        gcp_use_premium_network=args.gcp_use_premium_network,
    )

    # provision the gateway instances
    logger.info("Provisioning gateway instances")
    rc.provision_gateways(
        reuse_instances=True,
        log_dir=args.log_dir,
        authorize_ssh_pub_key=args.copy_ssh_key,
        num_outgoing_connections=args.num_outgoing_connections,
    )
    for path in rc.bound_paths:
        logger.info(f"Provisioned path {' -> '.join(path[i].region_tag for i in range(len(path)))}")
        for gw in path:
            logger.info(f"\t[{gw.region_tag}] {gw.gateway_log_viewer_url}")

    # run replication, monitor progress
    job = ReplicationJob(
        source_region=args.src_region,
        source_bucket=src_bucket,
        dest_region=args.dest_region,
        dest_bucket=dst_bucket,
        objs=obj_keys,
        random_chunk_size_mb=args.chunk_size_mb,
    )

    total_bytes = args.n_chunks * args.chunk_size_mb * MB
    crs = rc.run_replication_plan(job)
    logger.info(f"{total_bytes / GB:.2f}GByte replication job launched")
    stats = rc.monitor_transfer(crs)
    logger.info(f"Replication completed in {stats['total_runtime_s']:.2f}s ({stats['throughput_gbits']:.2f}Gbit/s)")


if __name__ == "__main__":
    print_header()
    main(parse_args())
