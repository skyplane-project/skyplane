import json
import os

from skyplane.api.config import TransferConfig
from skyplane.planner.planner import MulticastDirectPlanner


# Quota fall back helper functions and constants
REGIONS = {
    "aws": "us-east-1",
    "azure": "uaenorth",
    "gcp": "us-east1",
}
QUOTA_FILE = "test_quota_file"


def run_quota_tests(tests):
    for test in tests:
        quota_limits, expected_vm_types, expected_n_instances = test

        # Overwrite the test quota file - also creates one
        with open(QUOTA_FILE, "w") as f:
            f.write(json.dumps(quota_limits, indent=2))

        transfer_config = TransferConfig()
        planner = MulticastDirectPlanner(n_instances=8, n_connections=100, transfer_config=transfer_config, quota_limits_file=QUOTA_FILE)

        region_tags = [f"{p}:{REGIONS[p]}" for p in REGIONS.keys()]
        for i, src_region_tag in enumerate(region_tags):
            dst_region_tags = region_tags[:i] + region_tags[i + 1 :]
            vm_types, n_instances = planner._get_vm_type_and_instances(src_region_tag=src_region_tag, dst_region_tags=dst_region_tags)

            assert vm_types == expected_vm_types, f"vm types are calculated wrong - expected: {expected_vm_types}, calculated: {vm_types}"
            assert (
                n_instances == expected_n_instances
            ), f"n_instances are calculated wrong - expected: {expected_n_instances}, calculated: {n_instances}"

    if os.path.exists(QUOTA_FILE):
        # Delete the temporary quota file
        os.remove(QUOTA_FILE)
