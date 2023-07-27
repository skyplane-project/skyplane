import skyplane
import datetime
import time

from airflow import models
from airflow.models import BaseOperator

from skyplane import SkyplaneClient
from typing import Optional


class SkyplaneOperator(BaseOperator):
    def __init__(
        self,
        src_provider: str,
        src_bucket: str,
        src_region: str,
        dst_provider: str,
        dst_bucket: str,
        dst_region: str,
        aws_config: Optional[skyplane.AWSConfig] = None,
        gcp_config: Optional[skyplane.GCPConfig] = None,
        azure_config: Optional[skyplane.AzureConfig] = None,
        *args,
        **kwargs,
    ) -> None:
        self.src_provider = src_provider
        self.src_bucket = src_bucket
        self.src_region = src_region
        self.dst_provider = dst_provider
        self.dst_bucket = dst_bucket
        self.dst_region = dst_region
        self.aws_config = aws_config
        self.gcp_config = gcp_config
        self.azure_config = azure_config
        super().__init__(task_id="skyplane_dag", *args, **kwargs)

    def execute(self, context):
        client = SkyplaneClient(
            aws_config=self.aws_config,
            gcp_config=self.gcp_config,
            azure_config=self.azure_config,
        )
        dp = client.dataplane(
            self.src_provider,
            self.src_region,
            self.dst_provider,
            self.dst_region,
            n_vms=1,
        )
        with dp.auto_deprovision():
            dp.provision()

            dp.queue_copy(self.src_bucket, self.dst_bucket, recursive=True)

            # launch the transfer in a background thread
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
