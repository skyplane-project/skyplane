from pathlib import Path
import pickle
from skyplane.api.api import SkyplaneClient
from skyplane.api.auth_config import AWSConfig

if __name__ == "__main__":
    client = SkyplaneClient(aws_config=AWSConfig())
    print(f"Log dir: {client.log_dir}/client.log")
    dp = client.direct_dataplane("aws", "us-east-1", "aws", "us-east-2", n_vms=1)
    dp.provision(spinner=True)
    dp.start_gateway(gateway_log_dir=Path("~/.skyplane").expanduser() / "gateway_log", spinner=True)

    # queue some copies
    client.queue_copy("s3://skycamp-demo-src", "s3://skycamp-demo-us-east-2/imagenet-bucket", recursive=True)

    # launch the transfer in a background thread
    tracker = client.run_async(dp)

    # # monitor the transfer
    # while True:
    #     bytes_remaining = tracker.query_bytes_remaining()
    #     if bytes_remaining > 0:
    #         print(f"{(bytes_remaining / (2 ** 30)):.2f}GB left")
    #         time.sleep(1)
    #     else:
    #         break
    # tracker.join()
