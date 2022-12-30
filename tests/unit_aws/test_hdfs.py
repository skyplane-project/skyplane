from skyplane.obj_store.object_store_interface import ObjectStoreInterface
from tests.interface_util import interface_test_framework
from skyplane.utils.definitions import MB
import boto3
import uuid
import os


def test_hdfs():
    client = boto3.client("emr")
    try:
        # create roles necessary for EMR
        os.system("aws emr create-default-roles")

        waiter = boto3.client("iam").get_waiter("role_exists")
        waiter.wait(
            RoleName="EMR_EC2_DefaultRole",
        )

        # create cluster
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

    print("Cluster created successfully. Testing interface...")

    try:
        description = client.describe_cluster(ClusterId=clusterID)
        cluster_description = description["Cluster"]
        assert interface_test_framework("hdfs:emr", cluster_description["MasterPublicDnsName"], False, test_delete_bucket=True)

        assert interface_test_framework(
            "hdfs:emr", cluster_description["MasterPublicDnsName"], False, test_delete_bucket=True, file_size_mb=0
        )
    except Exception as e:
        raise e
    finally:
        response = client.terminate_job_flows(JobFlowIds=[job_flow_id])
