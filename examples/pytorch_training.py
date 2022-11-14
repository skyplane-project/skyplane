import argparse
import io
import warnings

warnings.filterwarnings("ignore")

import skyplane
import torch  # type: ignore
import torch.nn as nn  # type: ignore
import torch.nn.parallel  # type: ignore
import torch.optim  # type: ignore
import torch.utils.data  # type: ignore
import torch.utils.data.distributed  # type: ignore
import torchvision.transforms as transforms  # type: ignore
from torch.utils.data import IterableDataset, DataLoader  # type: ignore
from awsio.python.lib.io.s3.s3dataset import S3IterableDataset  # type: ignore

import torchvision.models as models  # type: ignore
from PIL import Image  # type: ignore


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

    def __iter__(self):
        self.s3_iter_dataset_iterator = iter(self.s3_iter_dataset)
        return self.data_generator()


if __name__ == "__main__":
    # Example:  Skyplane Pytorch training example:
    # Problem:  I want to train ResNet18 but my dataset is in a remote S3 bucket. It would be too slow
    #           to download the dataset to my local machine. AWS s3 cp runs at 20MB/s :(
    # Solution: Use the Skyplane API to copy the dataset to an S3 bucket in the same region as my GPUs.
    #           Then we can use the S3IterableDataset to read the data directly from S3 at high speeds.
    #           This example uses the ImageNet dataset, but it can be used with any dataset stored in S3. 
    
    # Step 0: My data is in a remote S3 bucket and directly reading it is too slow :(
    parser = argparse.ArgumentParser()
    parser.add_argument("--remote-s3-path", type=str, default="s3://jason-us-east-1/data/")
    parser.add_argument("--local-s3-path", type=str, default="s3://jason-us-west-2/data/")
    args = parser.parse_args()
    
    # Step 1:  Create a Skyplane API client. It will read your AWS credentials from the AWS CLI by default
    client = skyplane.SkyplaneClient(aws_config=skyplane.AWSConfig())

    # Step 2:  Copy the data from the remote bucket to the local bucket.
    client.copy(src=args.remote_s3_path, dst=args.local_s3_path, recursive=True)

    # Step 3:  Create a dataloader from the local bucket. This will read the data directly from S3 at high speeds!
    data_urls = [(args.local_s3_path + "/" if not args.local_s3_path.endswith("/") else args.local_s3_path) + f"imagenet-train-{i:06d}.tar" for i in range(100)]
    dataset = ImageNetS3(data_urls)
    train_loader = DataLoader(dataset, batch_size=256, num_workers=2)
    
    # Step 4:  Train a model!
    model = models.__dict__["resnet18"](pretrained=True)
    criterion = nn.CrossEntropyLoss()
    optimizer = torch.optim.SGD(model.parameters(), 0.1, momentum=0.9, weight_decay=1e-4)
    
    model.train()
    for ep in range(5):
        print(f"Start training epoch {ep}")
        for i, (images, target) in enumerate(train_loader):
            output = model(images)
            loss = criterion(output, target)
            print(f"Current loss for batch#{i} is: {loss.item()}")
            optimizer.zero_grad()
            loss.backward()
            optimizer.step()
    print("Training done")
