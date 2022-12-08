import time

import skyplane
from skyplane.broadcast.bc_client import SkyplaneBroadcastClient

if __name__ == "__main__":
    src_region = "us-east-1"
    dst_regions = ["us-west-1", "us-west-2"]
    # dst_regions = ["ap-east-1", "ap-northeast-1"]

    client = SkyplaneBroadcastClient(aws_config=skyplane.AWSConfig(), multipart_enabled=True)
    print(f"Log dir: {client.log_dir}/client.log")
    dp = client.broadcast_dataplane(
        src_cloud_provider="aws",
        src_region=src_region,
        # type="ILP",
        dst_cloud_providers=["aws", "aws"],
        dst_regions=dst_regions,
        n_vms=1,
    )

    with dp.auto_deprovision():
        # NOTE: need to queue copy first, then provision
        # NOTE: otherwise can't upload gateway programs to the gateways, don't know the bucket name and object name

        # source_file = "s3://sarah-skylark-us-east-1/test/direct_replication/"
        # dest1_file = "s3://broadcast-experiment-ap-south-1/chunks"
        # dest2_file = "s3://broadcast-experiment-us-east-2/chunks/"

        source_file = "s3://skyplane-broadcast/OPT-66B/"
        dest_files = ["s3://awsbucketsky2/OPT-66B/", "s3://awsbucketsky3/OPT-66B/"]

        # Able to transfer 2 9.1GB data now
        #source_file = "s3://awsbucketsky/test_multipart/"
        #dest_files = ["s3://awsbucketsky2/test_multipart/", "s3://awsbucketsky3/test_multipart/"]

        # source_file = "s3://skyplane-broadcast/OPT-66B/"
        # dest_files = ["s3://broadcast-ap-east-1/OPT-66B/", "s3://broadcast-ap-northeast-1/OPT-66B/"]

        print(source_file)
        print(dest_files)

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

            bytes_remaining = tracker.query_bytes_remaining()
            timestamp = time.strftime("%H:%M:%S", time.localtime())
            if bytes_remaining is None:
                print(f"{timestamp} Transfer not yet started")
            elif bytes_remaining > 0:
                print(f"{timestamp} {(bytes_remaining / (2 ** 30)):.5f}GB left")
            else:
                break
            time.sleep(1)
        tracker.join()
        print("Transfer complete!")
