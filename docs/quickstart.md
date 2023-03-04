# Quickstart 

## CLI 
To transfer files from a AWS to GCP, you can run: 
```
skyplane cp -r s3://... gs://...
```
You can also sync directories to avoid copying data that is already in the destination location: 
```
skyplane sync s3://... gs://...
```


## Python API 
You can also run skyplane from a Python API client. 
```
import skyplane

client = skyplane.SkyplaneClient(aws_config=skyplane.AWSConfig())
client.copy(src="s3://skycamp-demo-src/synset_labels.txt", dst="s3://skycamp-demo-us-east-2/imagenet-bucket/synset_labels.txt", recursive=False)
```