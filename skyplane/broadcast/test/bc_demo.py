import time

import skyplane

if __name__ == "__main__":
    client = skyplane.SkyplaneBroadcastClient(aws_config=skyplane.AWSConfig())
    print(f"Log dir: {client.log_dir}/client.log")
    dp = client.broadcast_dataplane(
        src_cloud_provider="aws",
        src_region="us-east-1",
        dst_cloud_providers=["aws", "aws"],
        dst_regions=["us-west-1", "us-west-2"],
        n_vms=1,
        # gbyte_to_transfer=32 NOTE: might need to fix the calculation of topo.cost_per_gb until real data is passed
    )
    # dp = client.broadcast_dataplane(
    #     src_cloud_provider="aws",
    #     src_region="us-east-1",
    #     dst_cloud_providers=["aws"],
    #     dst_regions=["us-west-1"],
    #     n_vms=1,
    #     # gbyte_to_transfer=32 NOTE: might need to fix the calculation of topo.cost_per_gb until real data is passed
    # )

    with dp.auto_deprovision():
        # NOTE: need to queue copy first, then provision
        # NOTE: otherwise can't upload gateway programs to the gateways, don't know the bucket name and object name
        dp.queue_copy(
            "s3://awsbucketsky/chunk_0",
            [
                "s3://awsbucketsky2/chunk_0",
                "s3://awsbucketsky3/chunk_0",
            ],
            recursive=False,
        )
        # dp.queue_copy(
        #     "s3://awsbucketsky/chunk_0",
        #     ["s3://awsbucketsky2/chunk_0"],
        #     recursive=False,
        # )
        dp.provision(spinner=True)
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
                print(f"{timestamp} {(bytes_remaining / (2 ** 30)):.2f}GB left")
            else:
                break
            time.sleep(1)
        tracker.join()
        print("Transfer complete!")
