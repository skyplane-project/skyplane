from skyplane.obj_store.object_store_interface import ObjectStoreInterface
from tests.interface_util import interface_test_framework
from skyplane.utils.definitions import MB
import boto3
import uuid


def test_hdfs():
    client = boto3.client("emr")
    try:
        cluster_name = uuid.uuid4().hex
        response = client.run_job_flow(
            Name=cluster_name,
            ReleaseLabel="emr-5.12.0",
            Instances={
                "MasterInstanceType": "m4.xlarge",
                "SlaveInstanceType": "m4.xlarge",
                "InstanceCount": 3,
                "KeepJobFlowAliveWhenNoSteps": True,
                "TerminationProtected": False,
            },
            VisibleToAllUsers=True,
            JobFlowRole="EMR_EC2_DefaultRole",
            ServiceRole="EMR_DefaultRole",
        )
        job_flow_id = response["JobFlowId"]

        clusters = client.list_clusters()

        clusterID = ""
        for cluster in clusters["Clusters"]:
            if cluster["Name"] == cluster_name:
                clusterID = cluster["Id"]

        waiter = client.get_waiter("cluster_running")
        waiter.wait(
            ClusterId=clusterID,
        )
    except Exception as e:
        raise e

    assert interface_test_framework("hdfs", clusterID, False, test_delete_bucket=True)

    assert interface_test_framework("hdfs", clusterID, False, test_delete_bucket=True, file_size_mb=0)

    try:
        response = client.terminate_job_flows(JobFlowIds=[job_flow_id])
    except Exception as e:
        raise e
