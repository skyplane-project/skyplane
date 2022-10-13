from skyplane.api.api_class import *

client1 = new_client(None)
client1.copy(src="s3://ap-se-2/fake_imagenet/", dst="s3://eu-west-3/fake_imagenet_1/", recursive=True)
