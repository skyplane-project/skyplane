from __future__ import annotations
import time

import skyplane

from airflow.models import BaseOperator  # type: ignore


class SkyplaneOperator(BaseOperator):
    template_fields = (
        "src_provider",
        "src_bucket",
        "src_region",
        "dst_provider",
        "dst_bucket",
        "dst_region",
        "config_path",
    )

    def __init__(
        self,
        *src_provider: str,
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


def execute(self, context):
    aws_config, gcp_config, azure_config = skyplane.SkyplaneAuth.load_from_config_file(self.config_path)
    client = skyplane.SkyplaneClient(aws_config=aws_config, gcp_config=gcp_config, azure_config=azure_config)
    dp = client.dataplane(self.src_provider, self.src_region, self.dst_provider, self.dst_region, n_vms=1)
    with dp.auto_deprovision():
        dp.provision()
        dp.queue_copy(self.src_bucket, self.dst_bucket, recursive=True)
        tracker = dp.run_async()
        reporter = skyplane.SimpleReporter(tracker)

    # monitor the transfer
    while reporter.update():
        time.sleep(1)
