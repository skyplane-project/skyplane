# from typing import Dict, Tuple
# from skyplane import compute
# from skyplane.api.config import TransferConfig
# from skyplane.planner.planner import MulticastDirectPlanner, Planner
# from skyplane.utils.fn import do_parallel

# quota_limits = {
#     "aws": [
#         {"on_demand_standard_vcpus": 5, "spot_standard_vcpus": 5, "region_name": "us-east-1"},
#     ],
#     "gcp": {
#         "us-east1": 8,
#     },
#     "azure": {
#         "uaenorth": 4,
#     },
# }


# def test_fall_back():
#     transfer_config = TransferConfig()
#     planner = MulticastDirectPlanner(n_instances=8, n_connections=100, transfer_config=transfer_config)
#     planner.quota_limits = quota_limits

#     region_tags = ["aws:us-east-1", "azure:uaenorth", "gcp:us-east1"]
#     test_vm_types = {"aws:us-east-1": "m5.xlarge", "azure:uaenorth": "Standard_D4_v5", "gcp:us-east1": "n2-standard-8"}
#     test_n_instances = {
#         "aws:us-east-1": 1,
#         "azure:uaenorth": 1,
#         "gcp:us-east1": 1,
#     }

#     for i, src_region_tag in enumerate(region_tags):
#         dst_region_tags = region_tags[:i] + region_tags[i + 1 :]
#         vm_info = do_parallel(planner._calculate_vm_types, [src_region_tag] + dst_region_tags)  # type: ignore
#         vm_types = {v[0]: Planner._vcpus_to_vm(cloud_provider=v[0].split(":")[0], vcpus=v[1][0]) for v in vm_info}  # type: ignore
#         n_instances = min(v[1][1] for v in vm_info)  # type: ignore

#         assert vm_types == test_vm_types, "vm types are calculated wrong"
#         assert test_n_instances[src_region_tag] == n_instances
