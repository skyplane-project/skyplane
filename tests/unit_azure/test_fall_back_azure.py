from skyplane import compute


def test_fall_back_aws():
    # Different instances
    instances = (
        "Standard_D2_v5",
        "Standard_D4_v5",
        "Standard_D8_v5",
        "Standard_D16_v5",
        "Standard_D32_v5",
        "Standard_D48_v5",
        "Standard_D64_v5",
        "Standard_D96_v5",
    )

    # Default parameters
    auth = compute.AzureAuthentication
    quota_limit = 4

    # Test fall back
    for vm_type in instances:
        smaller_vm = auth.fall_back_to_smaller_vm_if_neccessary(instance_type=vm_type, quota_limit=quota_limit)

        if vm_type == "Standard_D4_v5" or vm_type == "Standard_D2_v5":
            assert smaller_vm is None
        else:
            assert smaller_vm is not None
            assert smaller_vm == "Standard_D4_v5"
