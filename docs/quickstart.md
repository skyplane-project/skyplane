# Quickstart 

## CLI 
The simplest way to run transfers on Skyplane is to use the CLI. To transfer files from a AWS to GCP, you can run: 
```
skyplane cp -r s3://... gs://...
```
You can also sync directories to avoid copying data that is already in the destination location: 
```
skyplane sync s3://... gs://...
```


## Python API 
You can also run skyplane from a Python API client. To copy a single object or folder, you can run: 
```
import skyplane

client = skyplane.SkyplaneClient()
client.copy(src="s3://bucket-src/key", dst="s3://bucket-dst/key", recursive=False)
```
This will create a skyplane dataplane (i.e. cluster), execute the transfer, and tear down the cluster upon completion. 

You can also execute multiple transfers on the same dataplane to reduce overhead from VM startup time. To do this, you can define a dataplane object and provision it: 
```
dp = client.dataplane("aws", "us-east-1", "aws", "us-east-2", n_vms=8)
dp.provision()
```
This will create a dataplane for transfers between `us-east-1` and `us-east-2` with 8 VMs per region. Now, we can queue transfer jobs in this dataplane: 
```
# queue transfer 
dp.queue_copy("s3://bucket1/key1", "s3://bucket2/key1")
dp.queue_copy("s3://bucket1/key2", "s3://bucket2/key2")

# execute transfer
tracker = dp.run_async()

# monitor transfer status
remaining_bytes = tracker.query_bytes_remaining()
```
The queued transfer won't run until you call `dp.run()` or `dp.run_async()`. Once you run the transfer, you can moniter the transfer with the returned `tracker` object. Once the transfer is completed, make sure the deprovision the dataplane to avoid cloud costs: 
```
# tear down the dataplane 
dp.deprovision() 
```
You can have Skyplane automatically deprovision `dp.auto_deprovision()`:  
```
with dp.auto_deprovision():
    dp.provision()
    dp.queue_copy(...)
    tracker = dp.run_async()
```
Now you can programmatically transfer terabytes of data across clouds! To see some examples of applications you can build with the API, you can check out our tutorials on how to [load training data from another region](tutorial_dataloader.md) and [build an Airflow operator](tutorial_airflow.md).



