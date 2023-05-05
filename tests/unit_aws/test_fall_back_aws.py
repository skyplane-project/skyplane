from skyplane import compute


def test_fall_back_aws():
    # Different instances
    instances = (
        "m5.8xlarge",
        "m5.12xlarge",
        "m5.24xlarge",
        "m5.metal",
        "m5.xlarge",
        "m5.4xlarge",
        "m5.large",
        "m5.2xlarge",
        "m5.16xlarge",
    )

    # Default parameters
    auth = compute.AWSAuthentication
    quota_limit = 5

    # Test fall back
    for vm_type in instances:
        smaller_vm = auth.fall_back_to_smaller_vm_if_neccessary(instance_type=vm_type, quota_limit=quota_limit)

        if vm_type == "m5.xlarge" or vm_type == "m5.large":
            assert smaller_vm is None
        else:
            assert smaller_vm is not None
            assert smaller_vm == "m5.xlarge"
