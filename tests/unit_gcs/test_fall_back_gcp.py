from skyplane import compute


def test_fall_back_aws():
    # Different instances
    instances = (
        "n2-standard-128",
        "n2-standard-96",
        "n2-standard-80",
        "n2-standard-64",
        "n2-standard-48",
        "n2-standard-32",
        "n2-standard-16",
        "n2-standard-8",
        "n2-standard-4",
        "n2-standard-2",
    )

    # Default parameters
    auth = compute.GCPAuthentication
    quota_limit = 8

    # Test fall back
    for vm_type in instances:
        smaller_vm = auth.fall_back_to_smaller_vm_if_neccessary(instance_type=vm_type, quota_limit=quota_limit)

        if vm_type == "n2-standard-8" or vm_type == "n2-standard-4" or "n2-standard-2":
            assert smaller_vm is None
        else:
            assert smaller_vm is not None
            assert smaller_vm == "n2-standard-8"
