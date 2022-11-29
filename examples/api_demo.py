import time

import skyplane

if __name__ == "__main__":

    source_file = "s3://sarah-skylark-ap-northeast-2/medium_file.txt"
    dest_file = "s3://broadcast-experiment-ap-south-1/medium_file.txt"

    client = skyplane.SkyplaneClient(aws_config=skyplane.AWSConfig())
    print(f"Log dir: {client.log_dir}/client.log")
    dp = client.dataplane("aws", "us-east-1", "aws", "us-west-1", n_vms=1)
    with dp.auto_deprovision():
        dp.provision(spinner=True)
        dp.queue_copy(source_file, dest_file, recursive=False)
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
