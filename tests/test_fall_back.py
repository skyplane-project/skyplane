from typing import Dict, Tuple
from skyplane import compute
from skyplane.api.config import TransferConfig
from skyplane.planner.planner import MulticastDirectPlanner, Planner
from skyplane.utils.fn import do_parallel

quota_limits = {
    "aws": [
        {"on_demand_standard_vcpus": 5, "spot_standard_vcpus": 5, "region_name": "us-east-1"},
    ],
    "gcp": {
        "us-east1": 8,
    },
    "azure": {
        "uaenorth": 4,
    },
}


def test_fall_back():
    transfer_config = TransferConfig()
    planner = MulticastDirectPlanner(n_instances=8, n_connections=100, transfer_config=transfer_config)
    planner.quota_limits = quota_limits

    # aws: 5
    # azure: 4
    # gcp: 8
    region_tags = ["aws:us-east-1", "azure:uaenorth", "gcp:us-east1"]
    tests: Dict[Tuple[str, str], Tuple[int, int, str]] = {
        ("aws:us-east-1", "aws:us-east-1"): (8, 4, "m5.xlarge"),
        ("aws:us-east-1", "azure:uaenorth"): (1, 2, "m5.large"),
        ("aws:us-east-1", "gcp:us-east1"): (4, 4, "m5.xlarge"),
        ("azure:uaenorth", "aws:us-east-1"): (1, 2, "Standard_D2_v5"),
        ("azure:uaenorth", "azure:uaenorth"): (1, 2, "Standard_D2_v5"),
        ("azure:uaenorth", "gcp:us-east1"): (1, 2, "Standard_D2_v5"),
        ("gcp:us-east1", "aws:us-east-1"): (4, 4, "n2-standard-4"),
        ("gcp:us-east1", "azure:uaenorth"): (1, 2, "n2-standard-2"),
        ("gcp:us-east1", "gcp:us-east1"): (2, 8, "n2-standard-8"),
    }

    for src_region_tag in region_tags:
        for dst_region_tags in region_tags:
            targets = do_parallel(
                planner._calculate_vm_portions, [(src_region_tag, dst_region_tag) for dst_region_tag in [dst_region_tags]]
            )
            targets = [t[1] for t in targets]
            min_target_vcpus = min(t[0] for t in targets)  # type: ignore
            min_quota_limit = min(t[1] for t in targets if t[1] is not None)  # type: ignore
            n_portions, vcpus = (
                Planner._split_vcpus(num_vcpus=min_target_vcpus, quota_limit=min_quota_limit)
                if min_quota_limit is not None
                else (1, min_target_vcpus)
            )  # type: ignore

            # Get the source vm_type
            src_provider = src_region_tag.split(":")[0]
            src_vm_type = Planner._vcpus_to_vm(cloud_provider=src_provider, vcpus=vcpus)

            assert tests[(src_region_tag, dst_region_tags)] == (n_portions, vcpus, src_vm_type)
