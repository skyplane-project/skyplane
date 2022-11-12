import warnings

warnings.filterwarnings("ignore")

import torch
import torch.nn as nn
import torch.nn.parallel
import torch.optim
import torch.utils.data
import torch.utils.data.distributed
import torchvision.transforms as transforms
from torch.utils.data import IterableDataset, DataLoader
from awsio.python.lib.io.s3.s3dataset import S3IterableDataset

import torchvision.models as models
from PIL import Image
import io


class ImageNetS3(IterableDataset):
    def __init__(self, url_list, shuffle_urls=True, transform=None):
        self.s3_iter_dataset = S3IterableDataset(url_list, shuffle_urls)
        self.transform = transform

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


def get_urls(prefix, length=100):
    if not prefix.endswith("/"):
        prefix += "/"
    lst = []
    for i in range(length):
        lst.append(prefix + f"imagenet-train-{i:06d}.tar")
    return lst


def train(train_loader, model, criterion, optimizer, epoch):
    # switch to train mode
    model.train()

    for ep in range(epoch):
        print(f"Start training epoch {ep}")
        for i, (images, target) in enumerate(train_loader):
            # compute output
            output = model(images)
            loss = criterion(output, target)
            print(f"Current loss for batch#{i} is: {loss.item()}")

            # compute gradient and do SGD step
            optimizer.zero_grad()
            loss.backward()
            optimizer.step()
    print("Training done")


def prepare_training(data_url):
    batch_size = 256

    # Torchvision transforms to apply on data
    preproc = transforms.Compose(
        [
            transforms.RandomResizedCrop(224),
            transforms.RandomHorizontalFlip(),
            transforms.ToTensor(),
            transforms.Normalize((0.485, 0.456, 0.406), (0.229, 0.224, 0.225)),
        ]
    )

    dataset = ImageNetS3(data_url, transform=preproc)

    train_loader = DataLoader(dataset, batch_size=batch_size, num_workers=2)

    model = models.__dict__["resnet18"](pretrained=True)
    criterion = nn.CrossEntropyLoss()
    optimizer = torch.optim.SGD(model.parameters(), 0.1, momentum=0.9, weight_decay=1e-4)

    return train_loader, model, criterion, optimizer
