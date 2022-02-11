"""
AWS convenience interface
"""

import atexit
import json
import subprocess
import time
from shlex import split
from typing import Optional

import questionary
import typer
from skylark import GB
from skylark.compute.aws.aws_cloud_provider import AWSCloudProvider
from skylark.compute.aws.aws_server import AWSServer
from skylark.obj_store.s3_interface import S3Interface
from skylark.utils.utils import Timer, do_parallel

app = typer.Typer(name="skylark-aws")


@app.command()
def vcpu_limits(quota_code="L-1216C47A"):
    """List the vCPU limits for each region."""

    def get_service_quota(region):
        service_quotas = AWSServer.get_boto3_client("service-quotas", region)
        response = service_quotas.get_service_quota(ServiceCode="ec2", QuotaCode=quota_code)
        return response["Quota"]["Value"]

    quotas = do_parallel(get_service_quota, AWSCloudProvider.region_list())
    for region, quota in quotas:
        typer.secho(f"{region}: {int(quota)}", fg="green")


@app.command()
def ssh(region: Optional[str] = None):
    aws = AWSCloudProvider()
    typer.secho("Querying AWS for instances", fg="green")
    instances = aws.get_matching_instances(region=region)
    if len(instances) == 0:
        typer.secho(f"No instancess found", fg="red")
        typer.Abort()

    instance_map = {f"{i.region()}, {i.public_ip()} ({i.instance_state()})": i for i in instances}
    choices = list(sorted(instance_map.keys()))
    instance_name: AWSServer = questionary.select("Select an instance", choices=choices).ask()
    if instance_name is not None and instance_name in instance_map:
        instance = instance_map[instance_name]
        proc = subprocess.Popen(split(f"ssh -i {str(instance.local_keyfile)} ubuntu@{instance.public_ip()}"))
        proc.wait()
    else:
        typer.secho(f"No instance selected", fg="red")


@app.command()
def cp_datasync(src_bucket: str, dst_bucket: str, path: str):
    src_region = S3Interface.infer_s3_region(src_bucket)
    dst_region = S3Interface.infer_s3_region(dst_bucket)

    iam_client = AWSServer.get_boto3_client("iam", "us-east-1")
    try:
        response = iam_client.get_role(RoleName="datasync-role")
        typer.secho("IAM role exists datasync-role", fg="green")
    except iam_client.exceptions.NoSuchEntityException:
        typer.secho("Creating datasync-role", fg="green")
        policy = {
            "Version": "2012-10-17",
            "Statement": [{"Effect": "Allow", "Principal": {"Service": "datasync.amazonaws.com"}, "Action": "sts:AssumeRole"}],
        }
        response = iam_client.create_role(RoleName="datasync-role", AssumeRolePolicyDocument=json.dumps(policy))
    iam_client.attach_role_policy(
        RoleName="datasync-role",
        PolicyArn="arn:aws:iam::aws:policy/AWSDataSyncFullAccess",
    )
    # attach s3:ListBucket to datasync-role
    iam_client.attach_role_policy(RoleName="datasync-role", PolicyArn="arn:aws:iam::aws:policy/AmazonS3FullAccess")

    iam_arn = response["Role"]["Arn"]
    typer.secho(f"IAM role ARN: {iam_arn}", fg="green")

    ds_client_src = AWSServer.get_boto3_client("datasync", src_region)
    src_response = ds_client_src.create_location_s3(
        S3BucketArn=f"arn:aws:s3:::{src_bucket}",
        Subdirectory=path,
        S3Config={"BucketAccessRoleArn": iam_arn},
    )
    src_s3_arn = src_response["LocationArn"]
    ds_client_dst = AWSServer.get_boto3_client("datasync", dst_region)
    dst_response = ds_client_dst.create_location_s3(
        S3BucketArn=f"arn:aws:s3:::{dst_bucket}",
        Subdirectory=path,
        S3Config={"BucketAccessRoleArn": iam_arn},
    )
    dst_s3_arn = dst_response["LocationArn"]

    try:
        create_task_response = ds_client_dst.create_task(
            SourceLocationArn=src_s3_arn,
            DestinationLocationArn=dst_s3_arn,
            Name=f"{src_bucket}-{dst_bucket}-{path}",
            Options={"BytesPerSecond": -1, "OverwriteMode": "ALWAYS", "TransferMode": "ALL", "VerifyMode": "NONE"},
        )
        task_arn = create_task_response["TaskArn"]
    except ds_client_dst.exceptions.InvalidRequestException:
        typer.secho(f"Region not supported: {src_region} to {dst_region}", fg="red")
        raise typer.Abort()

    with Timer() as t:
        exec_response = ds_client_dst.start_task_execution(TaskArn=task_arn)
        task_execution_arn = exec_response["TaskExecutionArn"]

        def exit():
            task_execution_response = ds_client_dst.describe_task_execution(TaskExecutionArn=task_execution_arn)
            if task_execution_response["Status"] != "SUCCESS":
                ds_client_dst.cancel_task_execution(TaskExecutionArn=task_execution_arn)
                typer.secho("Cancelling task", fg="red")

        atexit.register(exit)

        last_status = None
        while last_status != "SUCCESS":
            task_execution_response = ds_client_dst.describe_task_execution(TaskExecutionArn=task_execution_arn)
            last_status = task_execution_response["Status"]
            metadata = {
                k: v
                for k, v in task_execution_response.items()
                if k
                in [
                    "EstimatedBytesToTransfer",
                    "BytesWritten",
                    "Result",
                ]
            }
            typer.secho(f"{int(t.elapsed)}s\tStatus: {last_status}, {metadata}", fg="green")
            time.sleep(5)

    task_execution_response = ds_client_dst.describe_task_execution(TaskExecutionArn=task_execution_arn)
    transfer_size = task_execution_response["BytesTransferred"]
    transfer_duration_ms = task_execution_response["Result"]["TransferDuration"]
    gbps = transfer_size * 8 / transfer_duration_ms / 1000 / GB
    typer.secho(f"DataSync response: {task_execution_response}", fg="green")
    typer.secho(json.dumps(dict(transfer_size=transfer_size, transfer_duration_ms=transfer_duration_ms, gbps=gbps)), fg="white")


if __name__ == "__main__":
    app()
