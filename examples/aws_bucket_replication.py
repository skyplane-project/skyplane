import boto3
import skyplane
import os
import time
import json

from skyplane.compute.aws.aws_auth import AWSAuthentication
from skyplane.obj_store.s3_interface import S3Interface

from absl import app
from absl import flags

FLAGS = flags.FLAGS
flags.DEFINE_string("src_region", None, "Source region")
flags.DEFINE_spaceseplist("dst_regions", None, "Destination regions")
flags.DEFINE_string("target_data", None, "Target data directory specified by S3 URI")

def bucket_handle(region): 
    return f"broadcast-experiment-{region}"

def delete_policy(policy_arn):
    client = boto3.client("iam")
    response = client.delete_policy(PolicyArn=policy_arn)

def delete_role(role_name, policy_arn, batch_policy_arn):
    client = boto3.client("iam")
    response = client.detach_role_policy(RoleName=role_name, PolicyArn=policy_arn)
    print("Successfully detached policy", policy_arn)
    response = client.detach_role_policy(RoleName=role_name, PolicyArn=batch_policy_arn)
    print("Successfully detached policy", batch_policy_arn)
    response = client.delete_role(RoleName=role_name)
    print("Deleted role", role_name)

def create_iam_role(src_region, dst_regions):
    client = boto3.client("iam")
    bucket_names = [bucket_handle(region.split(":")[1]) for region in [src_region] + dst_regions]

    batch_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "s3:InitiateReplication"
                ],
                "Resource": f"arn:aws:s3:::{bucket_names[0]}/*"
            },
            {
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject",
                    "s3:GetObjectVersion"
                ],
                "Resource": [
                    "arn:aws:s3:::{{ManifestDestination}}/*"
                ]
            },
            {
                "Effect": "Allow",
                "Action": [
                    "s3:PutObject"
                ],
                "Resource": [
                    "arn:aws:s3:::laion-400m-dataset/*"
                ]
            },
            {
                "Effect": "Allow",
                "Action": [
                    "s3:GetReplicationConfiguration",
                    "s3:PutInventoryConfiguration"
                ],
                "Resource": "arn:aws:s3:::broadcast-experiment-us-east-1"
            },
            {
                "Effect": "Allow",
                "Action": [
                    "s3:PutObject"
                ],
                "Resource": "arn:aws:s3:::{{ManifestDestination}}/*"
            }
        ]
    }
    policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Action": [
                    "s3:ListBucket",
                    "s3:GetReplicationConfiguration",
                    "s3:GetObjectVersionForReplication",
                    "s3:GetObjectVersionAcl",
                    "s3:GetObjectVersionTagging",
                    "s3:GetObjectRetention",
                    "s3:GetObjectLegalHold"
                ],
                "Effect": "Allow",
                "Resource": [f"arn:aws:s3:::{bucket_name}" for bucket_name in bucket_names] + [f"arn:aws:s3:::{bucket_name}/*" for bucket_name in bucket_names]
            },
            {
                "Action": [
                    "s3:ReplicateObject",
                    "s3:ReplicateDelete",
                    "s3:ReplicateTags",
                    "s3:ObjectOwnerOverrideToBucketOwner"
                ],
                "Effect": "Allow",
                "Resource": [f"arn:aws:s3:::{bucket_name}/*" for bucket_name in bucket_names]
            }
        ]
    }

    role_name = f"skyplane-bucket-replication-role-{int(time.time())}"
    policy_name = f"skyplane-bucket-replication-policy-{int(time.time())}"

    response = client.create_policy(
        PolicyName=policy_name, 
        PolicyDocument=json.dumps(policy)
    )
    policy_arn = response["Policy"]["Arn"]
    print("Created policy ARN", policy_arn)

    response = client.create_policy(
        PolicyName="batch"+policy_name, 
        PolicyDocument=json.dumps(batch_policy)
    )
    batch_policy_arn = response["Policy"]["Arn"]
    print("Created batch policy ARN", batch_policy_arn)
 

    assume_role_policy_document = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {
                    "Service": "s3.amazonaws.com"
                },
                "Action": "sts:AssumeRole"
            }, 
            {
                "Effect": "Allow",
                "Principal": {
                    "Service": "batchoperations.s3.amazonaws.com"
                },
                "Action": "sts:AssumeRole"
            }
        ]
    }

    resp = client.create_role(
        RoleName=role_name, 
        AssumeRolePolicyDocument=json.dumps(assume_role_policy_document)
    )
    role_arn = resp["Role"]["Arn"]
    print("Created role", role_name, role_arn)

    time.sleep(5)
    response = client.attach_role_policy(RoleName=role_name, PolicyArn=policy_arn)
    response = client.attach_role_policy(RoleName=role_name, PolicyArn=batch_policy_arn)

    response = client.get_role(
        RoleName=role_name
    )

    return role_name, role_arn, policy_arn, batch_policy_arn

def write_source_data(src_region, target_data, directory): 
    bucket_name = bucket_handle(src_region.split(":")[1])
    sync_command = f"aws s3 sync {target_data} s3://{bucket_name}/{directory}/"
    print("Syncing data to source bucket", sync_command)
    os.system(sync_command)


def main(argv):

    src_region = FLAGS.src_region
    dst_regions = list(FLAGS.dst_regions)
    experiment_name = f"aws_replication_{int(time.time())}"
    print("Destinations:", dst_regions)
    
    buckets = {}

    # create temporary bucket for each region 
    for region in [src_region] + dst_regions: 
        region = region.split(":")[1]
        bucket_name = bucket_handle(region)
        bucket = S3Interface(bucket_name)

        if not bucket.bucket_exists():
            print(f"Bucket {bucket_name} does not exist, creating it")
            bucket.create_bucket()
            print(f"Created bucket {bucket_name} in {region}")

        buckets[region] = bucket
        response = bucket._s3_client(region).put_bucket_versioning(
            Bucket=bucket_name,
            VersioningConfiguration={
                'Status': 'Enabled'
            }
        )
        print(f"Enabled bucket versioning for {bucket_name}")


    # put replication policy 
    src_name = bucket_handle(src_region.split(":")[1])
    # creat iam roles
    role_name, role_arn, policy_arn, batch_policy_arn = create_iam_role(src_region, dst_regions)
    time.sleep(5)

    client = buckets[src_region.split(":")[1]]._s3_client(src_region.split(":")[1])
    rules = []
    for dst_region in dst_regions:
        dest_name = bucket_handle(dst_region.split(":")[1])
        print("destination:", f"arn:aws:s3:::{dest_name}")
        rules.append(
            {
                'Priority': dst_regions.index(dst_region),
                'Filter': {'Prefix': f'{experiment_name}/'},
                'Status': 'Enabled',
                #'ExistingObjectReplication': {
                #    'Status': 'Enabled'
                #},
                'Destination': {
                    'Bucket': f"arn:aws:s3:::{dest_name}",
                    'StorageClass': 'STANDARD',
                    'ReplicationTime': {
                        'Status': 'Enabled',
                        'Time': {
                            'Minutes': 15
                        }
                    },
                    'Metrics': {
                        'Status': 'Enabled',
                        'EventThreshold': {
                            'Minutes': 15
                        }
                    }
                },
                'DeleteMarkerReplication': {
                    'Status': 'Disabled'
                }
            }
        )
        # create replication config 
    try:
        resp = client.put_bucket_replication(
            Bucket=src_name, 
            ReplicationConfiguration={
                'Role': role_arn,
                'Rules': rules
            }
        )
    except Exception as e:
        delete_role(role_name, policy_arn)
        delete_policy(policy_arn)
        delete_policy(batch_policy_arn)
        print("Error creating replication rule", e) 
        return 

    # write data to source - use skyplane to quickly copy some target data into the source bucket 
    # NOTE: only data copied after the replication rules are created will get copied
    src_region = src_region.split(":")[1]
    target_data_bucket = FLAGS.target_data.split("/")[2]
    target_data_region = S3Interface(target_data_bucket).aws_region
    src_bucket = bucket_handle(src_region)
    client = skyplane.SkyplaneClient(aws_config=skyplane.AWSConfig())
    print(f"Log dir: {client.log_dir}/client.log")

    dp = client.dataplane("aws", target_data_region, "aws", src_region, n_vms=4) # TODO: pass in target_throughput that we also pass to broadcast?
    with dp.auto_deprovision():

        # copy data with skyplane to source bucket
        dp.provision(spinner=True)
        dp.queue_copy(
            FLAGS.target_data, f"s3://{src_bucket}/{experiment_name}", recursive=True
        )
        print("Waiting for data to copy to source bucket...")
        dp.run()

        # wait for copy at destinations
        target_objects = list(buckets[src_region].list_objects(prefix=experiment_name))
        print(f"Waiting for len(target_objects) to replicate", experiment_name)
        num_src = len(target_objects)
        # TODO: replace with better monitoring
        while True: 
            completed = 0
            for region, bucket in buckets.items(): 
                if region == src_region: continue 
                objs = list(bucket.list_objects(prefix=experiment_name))
                num_dest = len(objs)
                print(f"{region}: Object replicated = {len(objs)} / {len(target_objects)}")
                if num_dest == num_src: 
                    completed += 1

            if completed == len(list(buckets.keys())) - 1:
                print("All replication completed!")
                break 


            time.sleep(1)

    # cleanup IAM roles and policies 
    delete_role(role_name, policy_arn, batch_policy_arn)
    delete_policy(policy_arn)
    delete_policy(batch_policy_arn)




if __name__ == '__main__':
  app.run(main)