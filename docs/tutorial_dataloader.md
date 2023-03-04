# Loading Data from S3 for Model Training

This tutorial explains how you can use the Skyplane API to quickly download data from an object store located in a different region or cloud than your training instance. See full workflow [here](https://github.com/skyplane-project/skyplane/tree/main/examples).

Large-scale machine learning (ML) training typically includes a step for acquiring training data. The following example illustrates an ML workflow where the original ImageNet data is stored in an S3 bucket in the US-East-1 region.

In many cases, datasets and virtual machines (VMs) are located in different regions. This can lead to slow data transfer speeds and high costs for data egress fees when using cloud provider tools, such as aws s3 cp, to download data to the VM's local disk. Skyplane offers a solution by allowing a fast and more cost-effective transfer of the dataset to an S3 bucket in the same region as the VM (e.g. US-West-2), with direct streaming of the data to the model without the need for downloading it to the local folder.

![imagenet_training](_static/api/imagenet.png)
This process is as simple as adding just two lines of code, similar to the demonstration of the Skyplane simple copy.

## Remote vs. Local Regions
Say that you have a VM for running training jobs in an AWS region, `us-west-2`. Reading data from a same-region S3 bucket will be very fast and free. However, if your data is in another region or cloud provider, read the data will be much slower and also charge you per-GB egress fees. In this tutorial, we assume that our data is in a bucket in `us-east-1` (the remote region), but we are running training from another region `us-west-2` (the local region).


## Reading data from S3 
Directly reading data from S3 can be convinient to avoid having to download your entire dataset before starting to train. In this tutorial, we create an `ImageNetS3` dataset that extends AWS's `S3IterableDataset` object.

```
import skyplane
import torch  
import torchvision.transforms as transforms  
from torch.utils.data import IterableDataset, DataLoader  
from awsio.python.lib.io.s3.s3dataset import S3IterableDataset  

class ImageNetS3(IterableDataset):
    def __init__(self, url_list, shuffle_urls=True):
        self.s3_iter_dataset = S3IterableDataset(url_list, shuffle_urls)
        self.transform = transforms.Compose(
            [
                transforms.RandomResizedCrop(224),
                transforms.RandomHorizontalFlip(),
                transforms.ToTensor(),
                transforms.Normalize((0.485, 0.456, 0.406), (0.229, 0.224, 0.225)),
            ]
        )

    def data_generator(self):
        try:
            while True:
                # Based on aplhabetical order of files sequence of label and image will change.
                # e.g. for files 0186304.cls 0186304.jpg, 0186304.cls will be fetched first
                _, label_fobj = next(self.s3_iter_dataset_iterator)
                _, image_fobj = next(self.s3_iter_dataset_iterator)
                label = int(label_fobj)
                image_np = Image.open(io.BytesIO(image_fobj)).convert("RGB")

                # Apply torch visioin transforms if provided
                if self.transform is not None:
                    image_np = self.transform(image_np)
                yield image_np, label

        except StopIteration:
            return
```
We can create a data loader with the data located in our remote bucket: 
```
    remote_bucket_url = "s3://us-east-1-bucket" 
    data_urls = [
        (remote_bucket_url + "/" if not remote_bucket_url.endswith("/") else remote_bucket_url) + f"imagenet-train-{i:06d}.tar"
        for i in range(100)
    ]
    dataset = ImageNetS3(data_urls)
    train_loader = DataLoader(dataset, batch_size=256, num_workers=2)
```
However, the latency of this dataloader will be very high and likely degrade training performance.  

## Tranferring Data with Skyplane 
We can improve our data loader's performance by transferring data to a local region first. We can do this by running: 
```
    local_bucket_url = "s3://us-west-2-bucket" 

    # Step 1:  Create a Skyplane API client. It will read your AWS credentials from the AWS CLI by default
    client = skyplane.SkyplaneClient(aws_config=skyplane.AWSConfig())

    # Step 2:  Copy the data from the remote bucket to the local bucket.
    client.copy(src=remote_bucket_url, dst=local_bucket_url, recursive=True)
```
Once the copy completes, the following code will be able to read the training data from the bucket with low latency, and no egress cost: 
```
    data_urls = [
        (local_bucket_url + "/" if not local_bucket_url.endswith("/") else local_bucket_url) + f"imagenet-train-{i:06d}.tar"
        for i in range(100)
    ]
    dataset = ImageNetS3(data_urls)
    train_loader = DataLoader(dataset, batch_size=256, num_workers=2)
```


