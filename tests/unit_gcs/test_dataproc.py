import uuid
from skyplane.config_paths import load_cloud_config, load_config_path, cloud_config
from skyplane.obj_store.object_store_interface import ObjectStoreInterface
from tests.interface_util import interface_test_framework
from skyplane.utils import imports

# from google.cloud import dataproc_v1 as dataproc
# from google.cloud import compute_v1 as compute
# from google.api_core.extended_operation import ExtendedOperation


def test_dataproc():
    cluster_name = f"skyplane-dataproc-test"
    region = "us-central1"
    # project_id = cloud_config.gcp_project_id

    # # Create a client with the endpoint set to the desired cluster region.
    # cluster_client = dataproc.ClusterControllerClient(client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"})

    # try:
    #     # Create the cluster config.
    #     cluster = {
    #         "cluster_name": cluster_name,
    #         "config": {
    #             "master_config": {"num_instances": 1, "machine_type_uri": "n1-standard-2"},
    #             "worker_config": {"num_instances": 2, "machine_type_uri": "n1-standard-2"},
    #         },
    #     }

    #     # Open up firewall rules
    #     firewall_rule = compute.Firewall()
    #     firewall_rule.name = "allow-ingress-all"
    #     firewall_rule.direction = "INGRESS"

    #     allowed_ports = compute.Allowed()
    #     allowed_ports.I_p_protocol = "tcp"
    #     allowed_ports.ports = ["0-65535"]

    #     firewall_rule.allowed = [allowed_ports]
    #     firewall_rule.source_ranges = ["0.0.0.0/0"]
    #     firewall_rule.network = "global/networks/default"

    #     firewall_client = compute.FirewallsClient()
    #     operation = firewall_client.insert(project=project_id, firewall_resource=firewall_rule)

    #     operation.result(timeout=300)

    #     if operation.error_code:
    #         raise operation.exception() or RuntimeError(operation.error_message)

    #     # Create the cluster.
    #     operation = cluster_client.create_cluster(request={"project_id": project_id, "region": region, "cluster": cluster})
    #     result = operation.result()
    #     print(result)
    # except Exception as e:
    #     raise e

    # print("Cluster created successfully. Testing interface...")

    # try:
    #     master_instance = compute.InstancesClient().get(project=project_id, zone="us-central1-b", instance="skyplane-dataproc-test-m")
    #     ip = master_instance.network_interfaces[0].network_i_p

    #     # assert interface_test_framework(f"hdfs:{region}", ip, False, test_delete_bucket=True)

    # except Exception as e:
    #     print(e)

    # finally:
    #     # Delete the cluster once the job has terminated.
    #     operation = cluster_client.delete_cluster(
    #         request={
    #             "project_id": project_id,
    #             "region": region,
    #             "cluster_name": cluster_name,
    #         }
    #     )
    #     operation.result()

    # print("Cluster {} successfully deleted.".format(cluster_name))
