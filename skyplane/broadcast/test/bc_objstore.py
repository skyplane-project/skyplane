import time
from skyplane.obj_store.s3_interface import S3Interface
from skyplane.obj_store.gcs_interface import GCSInterface

import skyplane
from skyplane.broadcast.bc_client import SkyplaneBroadcastClient
from skyplane.obj_store.object_store_interface import ObjectStoreInterface
from skyplane.utils.path import parse_path
from skyplane.utils.definitions import GB
from skyplane.utils.definitions import gateway_docker_image
import argparse


def start_transfer(args):
    #src_region = "ap-east-1"
    # src_region = "af-south-1"
    # src_region = "us-east-1"
    # dst_regions = ["ap-south-1", "ap-east-1", "ap-southeast-1", "ap-northeast-3", "ap-northeast-1"]
    # dst_regions = ["ap-south-1", "ap-east-1", "ap-southeast-2", "ap-northeast-3", "ap-northeast-1"]

    # dst_regions = ["ap-southeast-2", "ap-south-1"]
    #dst_regions = ["ap-southeast-2", "ap-south-1", "ap-northeast-3", "ap-northeast-2", "ap-northeast-1"]
    # dst_regions = ["us-west-1", "us-west-2"]



    # GCP 

    # gcp:asia-southeast2-a gcp:australia-southeast1-a gcp:southamerica-east1-a gcp:europe-west4-a gcp:europe-west6-a gcp:asia-east1-a gcp:europe-west2-a
    src_cloud_provider = "gcp"
    #src_region = "asia-southeast2-a"
    src_region = "us-east1-b"
    source_file = "gs://skyplane-broadcast-datasets/OPT-66B/"
    #dst_regions = ["europe-west3-a", "europe-west4-a", "us-west4-a", "europe-north1-a", "europe-west2-a"] #, "asia-south1-a"]
    dst_regions = ["australia-southeast1-a", "southamerica-east1-a", "europe-west4-a", "europe-west6-a", "asia-east1-a", "europe-west2-a"]
    dst_cloud_providers = ["gcp"] * len(dst_regions)
    dest_files = [f"gs://skyplane-broadcast-test-{d}/OPT-66B/" for d in dst_regions]

    ## AWS
    #dst_regions = ["ap-east-1", "ap-northeast-1"]
    #dst_cloud_providers = ["aws"] * len(dst_regions)
    #dest_files = [f"s3://skyplane-broadcast-test-{d}/OPT-66B/" for d in dst_regions]
    #src_cloud_provider = "aws"
    #src_region = "us-east-1"
    #source_file = "s3://laion-400m-dataset/"
 
    # OPT model
    # source_file = "s3://skyplane-broadcast/OPT-66B/"
    # source_file = f"s3://broadcast-opt-{src_region}/test_replication/"
    # dest_files = [f"s3://broadcast-opt-{d}/skyplane/" for d in dst_regions]

    # source_file = "s3://broadcast-opt-ap-east-1/test_replication/"
    # dest_files = [f"s3://broadcast-opt-{d}/test_replication/" for d in dst_regions]

    # source_file = "s3://skyplane-broadcast/imagenet-images/"
    #source_file = "s3://broadcast-exp3-ap-east-1/OPT-66B/"
    #source_file = "s3://broadcast-opt-ap-east-1/test_replication/"
    #dest_files = [f"s3://broadcast-exp3-{d}/OPT-66B/" for d in dst_regions]

    # create bucket if it doesn't exist

    actual_dest_regions = []
    for (region, bucket_path) in zip([src_region] + dst_regions, [source_file] + dest_files):
        bucket_name = bucket_path.split("/")[2]
        if "s3://" in bucket_path:
            bucket = S3Interface(bucket_name)
        elif "gs://" in bucket_path:
            bucket = GCSInterface(bucket_name)
        else: 
            print("Unsupported cloud provider", bucket_path)
        print("Create bucket", region, bucket_path)
        bucket.create_bucket(region)

        actual_dest_regions.append(bucket.gcp_region)

    print("acutual dest", actual_dest_regions, dst_regions) 
    dst_regions = actual_dest_regions

    print(source_file)
    print(dest_files)

    # Get transfer size
    if src_cloud_provider in ["aws", "gcp", "azure"] and [d in ["aws", "gcp", "azure"] for d in dst_cloud_providers]:
        try:
            provider_src, bucket_src, path_src = parse_path(source_file)
            src_region_tag = f"{provider_src}:{src_region}"

            src_client = ObjectStoreInterface.create(src_region_tag, bucket_src)

            print(f"Listing objects from the source bucket {src_region}")
            src_objects = []
            for obj in src_client.list_objects(path_src, src_region):
                src_objects.append(obj)
            transfer_size_gbytes = sum([obj.size for obj in src_objects]) / GB

            print("Transfer size gbytes: ", transfer_size_gbytes)
        except Exception as e:
            raise Exception("Cannot list size in the source bucket", e)

    client = SkyplaneBroadcastClient(aws_config=skyplane.AWSConfig(), multipart_enabled=True)
    print(f"Log dir: {client.log_dir}/client.log")

    dp = client.broadcast_dataplane(
        src_cloud_provider=src_cloud_provider,
        src_region=src_region,
        dst_cloud_providers=dst_cloud_providers,
        dst_regions=dst_regions,
        type=args["algo"],
        n_vms=int(args["num_vms"]),
        num_partitions=int(args["num_partitions"]),
        gbyte_to_transfer=transfer_size_gbytes,  # 171.78460 for image net
        target_time=args["runtime_budget"],
        filter_node=args["filter_node"],
        filter_edge=args["filter_edge"],
        solve_iterative=args["iterative"],
        aws_only=args["aws_only"],
        gcp_only=args["gcp_only"],
        azure_only=args["azure_only"],
    )

    with dp.auto_deprovision():
        # NOTE: need to queue copy first, then provision
        # NOTE: otherwise can't upload gateway programs to the gateways, don't know the bucket name and object name

        dp.queue_copy(
            source_file,
            dest_files,
            recursive=True,
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


def main():
    # Set up arguments
    parser = argparse.ArgumentParser(description="Test object store transfer")
    parser.add_argument("-a", "--algo", help="Algorithms: [Ndirect, MDST, HST, ILP]", type=str)
    parser.add_argument("-s", "--runtime-budget", help="Maximum runtime budget", nargs="?", required=False, const=10, type=float)
    parser.add_argument("-n", "--num-vms", help="Maximum number of vms per region", nargs="?", required=True, const=1, type=int)
    parser.add_argument("-p", "--num-partitions", help="Number of partitions of the solver", nargs="?", required=True, const=10, type=int)
    parser.add_argument("-fe", "--filter-edge", help="Filter edge (one-hop)", required=False, action="store_true")
    parser.add_argument("-fn", "--filter-node", help="Filter node (random)", required=False, action="store_true")
    parser.add_argument("-i", "--iterative", help="Chunk iterative solve", required=False, action="store_true")
    parser.add_argument("-aws", "--aws-only", help="Use aws only nodes", required=False, action="store_true")
    parser.add_argument("-gcp", "--gcp-only", help="Use gcp only nodes", required=False, action="store_true")
    parser.add_argument("-azure", "--azure-only", help="Use azure only nodes", required=False, action="store_true")
    args = vars(parser.parse_args())
    start_transfer(args)


if __name__ == "__main__":
    main()
