"""
AWS convenience interface
"""

import json
import time

import typer

from skyplane import GB
from skyplane.compute.aws.aws_auth import AWSAuthentication
from skyplane.compute.aws.aws_cloud_provider import AWSCloudProvider
from skyplane.obj_store.s3_interface import S3Interface
from skyplane.utils import logger
from skyplane.utils.fn import do_parallel
from skyplane.utils.timer import Timer

app = typer.Typer(name="skyplane-aws")


@app.command()
def vcpu_limits(quota_code="L-1216C47A"):
    """List the vCPU limits for each region."""
    aws_auth = AWSAuthentication()

    def get_service_quota(region):
        service_quotas = aws_auth.get_boto3_client("service-quotas", region)
        try:
            response = service_quotas.get_service_quota(ServiceCode="ec2", QuotaCode=quota_code)
        except Exception as e:
            logger.exception(e, print_traceback=False)
            logger.error(f"Failed to get service quota for {quota_code} in {region}")
            return -1
        return response["Quota"]["Value"]

    quotas = do_parallel(get_service_quota, AWSCloudProvider.region_list())
    for region, quota in quotas:
        typer.secho(f"{region}: {int(quota)}", fg="green")


@app.command()
def cp_datasync(src_bucket: str, dst_bucket: str, path: str):
    aws_auth = AWSAuthentication()
    src_region = S3Interface(src_bucket).aws_region
    dst_region = S3Interface(dst_bucket).aws_region

    iam_client = aws_auth.get_boto3_client("iam", "us-east-1")
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
    iam_client.attach_role_policy(RoleName="datasync-role", PolicyArn="arn:aws:iam::aws:policy/AWSDataSyncFullAccess")
    # attach s3:ListBucket to datasync-role
    iam_client.attach_role_policy(RoleName="datasync-role", PolicyArn="arn:aws:iam::aws:policy/AmazonS3FullAccess")

    iam_arn = response["Role"]["Arn"]
    typer.secho(f"IAM role ARN: {iam_arn}", fg="green")

    # wait for role to be ready
    typer.secho("Waiting for IAM role to be ready", fg="green")
    iam_client.get_waiter("role_exists").wait(RoleName="datasync-role")

    ds_client_src = aws_auth.get_boto3_client("datasync", src_region)
    src_response = ds_client_src.create_location_s3(
        S3BucketArn=f"arn:aws:s3:::{src_bucket}", Subdirectory=path, S3Config={"BucketAccessRoleArn": iam_arn}
    )
    src_s3_arn = src_response["LocationArn"]
    ds_client_dst = aws_auth.get_boto3_client("datasync", dst_region)
    dst_response = ds_client_dst.create_location_s3(
        S3BucketArn=f"arn:aws:s3:::{dst_bucket}", Subdirectory=path, S3Config={"BucketAccessRoleArn": iam_arn}
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
        typer.secho(f"Region not supported: {src_region} to {dst_region}", fg="red", err=True)
        raise typer.Abort()

    with Timer() as t:
        exec_response = ds_client_dst.start_task_execution(TaskArn=task_arn)
        task_execution_arn = exec_response["TaskExecutionArn"]

        def exit():
            task_execution_response = ds_client_dst.describe_task_execution(TaskExecutionArn=task_execution_arn)
            if task_execution_response["Status"] != "SUCCESS":
                ds_client_dst.cancel_task_execution(TaskExecutionArn=task_execution_arn)
                typer.secho("Cancelling task", fg="red", err=True)

        last_status = None
        try:
            while last_status != "SUCCESS":
                task_execution_response = ds_client_dst.describe_task_execution(TaskExecutionArn=task_execution_arn)
                last_status = task_execution_response["Status"]
                metadata_fields = ["EstimatedBytesToTransfer", "BytesWritten", "Result"]
                metadata = {k: v for k, v in task_execution_response.items() if k in metadata_fields}
                typer.secho(f"{int(t.elapsed)}s\tStatus: {last_status}, {metadata}", fg="green")
                time.sleep(5)
                if (int(t.elapsed) > 300) and last_status == "LAUNCHING":
                    typer.secho(
                        "The process might have errored out. Try deleting the objects if they exist already and restart the transfer.",
                        fg="red",
                        err=True,
                    )
        except KeyboardInterrupt:
            if last_status != "SUCCESS":
                exit()

    task_execution_response = ds_client_dst.describe_task_execution(TaskExecutionArn=task_execution_arn)
    transfer_size_gb = task_execution_response["BytesTransferred"] / GB
    transfer_duration_s = task_execution_response["Result"]["TransferDuration"] / 1000
    gbps = transfer_size_gb * 8 / transfer_duration_s
    typer.secho(f"DataSync response: {task_execution_response}", fg="green")
    typer.secho(
        json.dumps(dict(transfer_size_gb=transfer_size_gb, transfer_duration_s=transfer_duration_s, gbps=gbps, total_runtime_s=t.elapsed)),
        fg="white",
    )
