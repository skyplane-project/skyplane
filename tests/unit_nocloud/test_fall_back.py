import json
import os

import pytest
from skyplane.api.transfer_config import TransferConfig
from skyplane.planner.planner import MulticastDirectPlanner


REGIONS = {
    "aws": "us-east-1",
    "azure": "uaenorth",
    "gcp": "us-east1",
}
QUOTA_FILE = "test_quota_file"


@pytest.mark.skip(reason="Shared function")
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


def test_fall_back_no_quota_limit_exists():
    # 1. Test for when the quota limit file doesn't exist
    no_quota_tests = [
        # No AWS
        [
            {
                "gcp": {"us-east1": 8},
                "azure": {"uaenorth": 4},
            },
            {
                "azure:uaenorth": "Standard_D2_v5",
                "aws:us-east-1": "m5.8xlarge",
                "gcp:us-east1": "n2-standard-8",
            },
            1,
        ],
        # No GCP
        [
            {
                "aws": [
                    {
                        "on_demand_standard_vcpus": 10,
                        "spot_standard_vcpus": 10,
                        "region_name": "us-east-1",
                    }
                ],
                "azure": {"uaenorth": 8},
            },
            {
                "azure:uaenorth": "Standard_D2_v5",
                "gcp:us-east1": "n2-standard-16",
                "aws:us-east-1": "m5.2xlarge",
            },
            1,
        ],
        # No Azure
        [
            {
                "aws": [
                    {
                        "on_demand_standard_vcpus": 20,
                        "spot_standard_vcpus": 20,
                        "region_name": "us-east-1",
                    }
                ],
                "gcp": {"us-east1": 32},
            },
            {
                "azure:uaenorth": "Standard_D2_v5",
                "gcp:us-east1": "n2-standard-16",
                "aws:us-east-1": "m5.4xlarge",
            },
            1,
        ],
        # Only AWS
        [
            {
                "aws": [
                    {
                        "on_demand_standard_vcpus": 10,
                        "spot_standard_vcpus": 10,
                        "region_name": "us-east-1",
                    }
                ],
            },
            {
                "azure:uaenorth": "Standard_D2_v5",
                "gcp:us-east1": "n2-standard-16",
                "aws:us-east-1": "m5.2xlarge",
            },
            1,
        ],
        # Only GCP
        [
            {
                "gcp": {"us-east1": 32},
            },
            {
                "azure:uaenorth": "Standard_D2_v5",
                "gcp:us-east1": "n2-standard-16",
                "aws:us-east-1": "m5.8xlarge",
            },
            2,
        ],
        # Only Azure
        [
            {
                "azure": {"uaenorth": 8},
            },
            {
                "azure:uaenorth": "Standard_D2_v5",
                "gcp:us-east1": "n2-standard-16",
                "aws:us-east-1": "m5.8xlarge",
            },
            4,
        ],
    ]

    # 2. Test for when the quota limit file exists but doesn't include a region
    no_region_tests = [
        [
            {
                "aws": [
                    {
                        "on_demand_standard_vcpus": 5,
                        "spot_standard_vcpus": 5,
                        "region_name": "us-west-1",  # us-east-1 doesn't exist
                    }
                ],
                "gcp": {"us-east1": 8},
                "azure": {"uaenorth": 4},
            },
            {
                "azure:uaenorth": "Standard_D2_v5",
                "aws:us-east-1": "m5.8xlarge",
                "gcp:us-east1": "n2-standard-8",
            },
            1,
        ],
        [
            {
                "aws": [
                    {
                        "on_demand_standard_vcpus": 10,
                        "spot_standard_vcpus": 10,
                        "region_name": "us-east-1",
                    }
                ],
                "gcp": {"us-west1": 16},  # us-east1 doesn't exist
                "azure": {"uaenorth": 8},
            },
            {
                "azure:uaenorth": "Standard_D2_v5",
                "gcp:us-east1": "n2-standard-16",
                "aws:us-east-1": "m5.2xlarge",
            },
            1,
        ],
        [
            {
                "aws": [
                    {
                        "on_demand_standard_vcpus": 20,
                        "spot_standard_vcpus": 20,
                        "region_name": "us-east-1",
                    }
                ],
                "gcp": {"us-east1": 32},
                "azure": {"westindia": 16},  # uaenorth doesn't exist
            },
            {
                "azure:uaenorth": "Standard_D2_v5",
                "gcp:us-east1": "n2-standard-16",
                "aws:us-east-1": "m5.4xlarge",
            },
            1,
        ],
    ]

    for test in (no_quota_tests, no_region_tests):
        run_quota_tests(test)


def test_fall_back_multiple_limits_and_types():
    # Different quota limits
    tests = [
        [
            {
                "aws": [
                    {
                        "on_demand_standard_vcpus": 5,
                        "spot_standard_vcpus": 5,
                        "region_name": "us-east-1",
                    }
                ],
                "gcp": {"us-east1": 8},
                "azure": {"uaenorth": 4},
            },
            {
                "azure:uaenorth": "Standard_D2_v5",
                "aws:us-east-1": "m5.xlarge",
                "gcp:us-east1": "n2-standard-8",
            },
            1,
        ],
        [
            {
                "aws": [
                    {
                        "on_demand_standard_vcpus": 10,
                        "spot_standard_vcpus": 10,
                        "region_name": "us-east-1",
                    }
                ],
                "gcp": {"us-east1": 16},
                "azure": {"uaenorth": 8},
            },
            {
                "azure:uaenorth": "Standard_D2_v5",
                "gcp:us-east1": "n2-standard-16",
                "aws:us-east-1": "m5.2xlarge",
            },
            1,
        ],
        [
            {
                "aws": [
                    {
                        "on_demand_standard_vcpus": 20,
                        "spot_standard_vcpus": 20,
                        "region_name": "us-east-1",
                    }
                ],
                "gcp": {"us-east1": 32},
                "azure": {"uaenorth": 16},
            },
            {
                "azure:uaenorth": "Standard_D2_v5",
                "gcp:us-east1": "n2-standard-16",
                "aws:us-east-1": "m5.4xlarge",
            },
            1,
        ],
    ]

    run_quota_tests(tests)
