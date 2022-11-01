import time
from pathlib import Path

from skyplane.api.api import SkyplaneClient
from skyplane.api.auth_config import AWSConfig

if __name__ == "__main__":
    client = SkyplaneClient(aws_config=AWSConfig())
    print(f"Log dir: {client.log_dir}/client.log")
    dp = client.direct_dataplane("aws", "us-east-1", "aws", "us-east-2", n_vms=1)
    with dp.auto_deprovision():
        dp.provision(spinner=True)
        dp.start_gateway(gateway_log_dir=Path("~/.skyplane").expanduser() / "gateway_log", spinner=True)

        # queue some copies
        client.queue_copy("s3://skycamp-demo-src", "s3://skycamp-demo-us-east-2/imagenet-bucket", recursive=True)

        # launch the transfer in a background thread
        tracker = client.run_async(dp)
        print("Waiting for transfer to complete...")

        # monitor the transfer
        while True:
            # handle errors
            if tracker.errors:
                for ip, error in tracker.errors.items():
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
