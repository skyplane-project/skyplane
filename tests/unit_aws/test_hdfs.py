from skyplane.obj_store.object_store_interface import ObjectStoreInterface
from tests.interface_util import interface_test_framework
from skyplane.utils.definitions import MB
import boto3
import uuid
import os


def test_hdfs():

    # TODO: Create HDFS unit test
    client = boto3.client("emr", "us-east-1")

    # # create roles necessary for EMR
    # os.system("aws emr create-default-roles")

    # waiter = boto3.client("iam").get_waiter("role_exists")
    # waiter.wait(
    #     RoleName="EMR_EC2_DefaultRole",
    # )

    # ec2 = boto3.client("ec2")
    # security_groups = ec2.describe_security_groups(GroupNames=["ElasticMapReduce-master", "ElasticMapReduce-slave"])["SecurityGroups"]

    # try:
    #     # create cluster
    #     cluster_name = uuid.uuid4().hex

    #     response = client.run_job_flow(
    #         Name=cluster_name,
    #         ReleaseLabel="emr-5.12.0",
    #         Instances={
    #             "MasterInstanceType": "m4.xlarge",
    #             "SlaveInstanceType": "m4.xlarge",
    #             "InstanceCount": 3,
    #             "KeepJobFlowAliveWhenNoSteps": True,
    #             "TerminationProtected": False,
    #         },
    #         VisibleToAllUsers=True,
    #         JobFlowRole="EMR_EC2_DefaultRole",
    #         ServiceRole="EMR_DefaultRole",
    #     )
    #     job_flow_id = response["JobFlowId"]

    #     clusters = client.list_clusters()

    #     clusterID = ""
    #     for cluster in clusters["Clusters"]:
    #         if cluster["Name"] == cluster_name:
    #             clusterID = cluster["Id"]
    #     waiter = client.get_waiter("cluster_running")
    #     waiter.wait(
    #         ClusterId=clusterID,
    #     )

    #     # open up security groups
    #     for group in security_groups:
    #         security_group_id = group["GroupId"]
    #         ec2.authorize_security_group_ingress(
    #             GroupId=security_group_id,
    #             IpPermissions=[{"IpProtocol": "tcp", "FromPort": 0, "ToPort": 65535, "IpRanges": [{"CidrIp": "0.0.0.0/0"}]}],
    #         )

    # except Exception as e:
    #     raise e

    # print("Cluster created successfully. Testing interface...")

    # try:
    #     master_ip = ""
    #     # get the master IP address
    #     for instance in client.list_instances(ClusterId=clusterID, InstanceGroupTypes=["MASTER"])["Instances"]:
    #         master_ip = instance["PublicIpAddress"]
    #         print(instance)

    #     # Resolving the worker IP address is a bit tricky. The following code is a workaround.
    #     # hostname = open("/tmp/hostname", "w+")
    #     for instance in client.list_instances(ClusterId=clusterID, InstanceGroupTypes=["CORE"])["Instances"]:
    #         # print(f'{instance["PublicIpAddress"]}\t{instance["PrivateIpAddress"]}')
    #         # hostname.write(f'{instance["PublicIpAddress"]}\t{instance["PrivateIpAddress"]} \n')
    #         print(instance)
    #         # os.system(f'sudo iptables -t nat -I PREROUTING -d {instance["PrivateIpAddress"]} -j SNAT --to-destination {instance["PublicIpAddress"]}')

    #     assert interface_test_framework("hdfs:emr", master_ip, False, test_delete_bucket=True)

    #     assert interface_test_framework("hdfs:emr", master_ip, False, test_delete_bucket=True, file_size_mb=0)
    # except Exception as e:
    #     raise e
    # finally:

    #     # # delete the file so it doesn't interfere with other tests
    #     # os.remove("/tmp/hostname")

    #     # close security groups
    #     for group in security_groups:
    #         security_group_id = group["GroupId"]
    #         ec2.revoke_security_group_ingress(
    #             GroupId=security_group_id,
    #             IpPermissions=[{"IpProtocol": "tcp", "FromPort": 0, "ToPort": 65535, "IpRanges": [{"CidrIp": "0.0.0.0/0"}]}],
    #         )

    #     # Delete the cluster once the job has terminated.
    #     response = client.terminate_job_flows(JobFlowIds=[job_flow_id])
