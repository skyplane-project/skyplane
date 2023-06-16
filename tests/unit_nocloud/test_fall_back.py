from tests.test_utils import run_quota_tests


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
