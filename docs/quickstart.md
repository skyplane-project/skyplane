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

# single destination 
client.copy(src="s3://bucket-src/key", dst="s3://bucket-dst/key", recursive=False)

# multiple destinations
client.copy(
    src="s3://bucket-src/key", 
    dst=["s3://bucket-dst1/key", "s3://bucket-dst2/key"], 
    recursive=False
)
```
This will create a skyplane dataplane (i.e. cluster), execute the transfer, and tear down the cluster upon completion. 

You can also pipeline multiple transfers using the same provisioned gateways to reduce overhead from VM startup time. To do this, you can define a pipeline object and queue transfer jobs: 
```
pipeline = client.pipeline()
pipeline.queue_copy(src="s3://bucket-src/key", dst="s3://bucket-dst/key", recursive=False)
pipeline.queue_sync(src="s3://bucket-src/secondkey", dst="s3://bucket-dst/secondkey")
```
This will create a pipeline running one copy job and one sync job. Now, we can start the pipeline: 
```
# execute transfer
tracker = pipeline.start_async()

# monitor transfer status
remaining_bytes = tracker.query_bytes_remaining()
```
The queued transfer won't run until you call `pipeline.start()` or `pipeline.start_async()`. Once you run the transfer, you can monitor the transfer with the returned `tracker` object. Once the transfer is completed, the provisioned resources will be automatically deprovisioned to avoid cloud costs.

Now you can programmatically transfer terabytes of data across clouds! To see some examples of applications you can build with the API, you can check out our tutorials on how to [load training data from another region](tutorial_dataloader.md) and [build an Airflow operator](tutorial_airflow.md).



