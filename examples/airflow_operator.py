from __future__ import annotations

import os
import warnings

import skyplane
from typing import TYPE_CHECKING, Sequence

from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from skyplane import SkyplaneClient, SkyplaneAuth

if TYPE_CHECKING:
    from airflow.utils.context import Context

class SkyplaneOperator(BaseOperator):
    template_fields: Sequence[str] = (
        src_provider,
        src_bucket,
        src_region,
        dst_provider,
        dst_bucket,
        dst_region,
        config_path,
    )
    def __init__(
        self,
        *
        src_provider: str,
        src_bucket: str,
        src_region: str,
        dst_provider: str,
        dst_bucket: str,
        dst_region: str,
        config_path: str,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.src_provider = src_provider
        self.src_bucket = src_bucket
        self.src_region = src_region
        self.dst_provider = dst_provider
        self.dst_bucket = dst_bucket
        self.dst_region = dst_region
        self.config_path = config_path

def execute(self, context: Context):
        aws_config, gcp_config, azure_config = SkyplaneAuth.load_from_config_file(self.config_path)
        client = SkyplaneClient(aws_config=aws_config, gcp_config=gcp_config, azure_config=azure_config)
        dp = client.dataplane(self.src_provider, self.src_region, self.dst_provider, self.dst_region, n_vms=1)
        with dp.auto_deprovision():
            dp.provision()

            dp.queue_copy(self.src_bucket, self.dst_bucket, recursive=True)

            # launch the transfer in a background thread
            tracker = dp.run_async()

            reporter = SimpleReporter(tracker)

        # monitor the transfer
        while reporter.update():
            time.sleep(1)