# Example applications using Skyplane's API

## Training an ML model with Skyplane

This example will train ResNet18 on the ImageNet dataset using Skyplane's API.

```bash
# install torch and torchvision via https://pytorch.org/get-started/locally
# install awsio via https://github.com/aws/amazon-s3-plugin-for-pytorch

# install skyplane
$ pip install skyplane

# clone the example repo
$ git clone https://github.com/skyplane-project/skyplane.git
$ cd examples

# run the example
$ python pytorch_training.py --remote-s3-path s3://src-bucket/imagenet --local-s3-path s3://bucket-in-gpu-region/imagenet
```