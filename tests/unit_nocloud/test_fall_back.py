from skyplane.api.config import TransferConfig
from skyplane.cli.cli_transfer import SkyplaneCLI
from skyplane.config import SkyplaneConfig
from skyplane.planner.planner import MulticastDirectPlanner


def test_fall_back_multiple_limits_and_types():
    # Regions for each provider
    regions = {
        "aws": "us-east-1",
        "azure": "uaenorth",
        "gcp": "us-east1",
    }

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

    for test in tests:
        quota_limits, expected_vm_types, expected_n_instances = test
        transfer_config = TransferConfig()
        planner = MulticastDirectPlanner(n_instances=8, n_connections=100, transfer_config=transfer_config)
        planner.quota_limits = quota_limits

        region_tags = [f"{p}:{regions[p]}" for p in regions.keys()]
        for i, src_region_tag in enumerate(region_tags):
            dst_region_tags = region_tags[:i] + region_tags[i + 1 :]
            vm_types, n_instances = planner._get_vm_type_and_instances(src_region_tag=src_region_tag, dst_region_tags=dst_region_tags)

            assert vm_types == expected_vm_types, f"vm types are calculated wrong {vm_types}"
            assert n_instances == expected_n_instances, f"n_instances are calculated wrong {n_instances}"
