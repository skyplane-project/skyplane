from skyplane.api.api_class import *

client = SkyplaneClient()
client.copy(src="s3://jason-us-east-1/fake_imagenet/", 
            dst="s3://jason-us-west-2/fake_imagenet_1/", 
            recursive=True, future=1)