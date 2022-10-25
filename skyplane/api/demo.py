import time

from skyplane.api.api import SkyplaneClient
from skyplane.api.auth_config import AWSConfig

if __name__ == "__main__":
    client = SkyplaneClient(aws_config=AWSConfig())
    dp = client.direct_dataplane("aws", "us-east-1", "aws", "us-east-2", n_vms=1)
    with dp.auto_deprovision():
        dp.provision()

        # queue some copies
        client.queue_copy("s3://src/a", "s3://dst/b", recursive=True)
        client.queue_sync("s3://src/c", "s3://dst/d", recursive=True)

        # launch the transfer in a background thread
        tracker = client.run_async(dp)

        # monitor the transfer
        while True:
            bytes_remaining = tracker.query_bytes_remaining()
            if bytes_remaining > 0:
                print(f"{(bytes_remaining / (2 ** 30)):.2f}GB left")
                time.sleep(1)
            else:
                break
        tracker.join()
