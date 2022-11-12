import skyplane
from helper import get_urls, prepare_training, train

if __name__ == "__main__":
    client = skyplane.SkyplaneClient(aws_config=skyplane.AWSConfig())
    print(f"Log dir: {client.log_dir}/client.log")
    # Original location of the training data
    data_urls = get_urls("s3://jason-us-east-1/data")
    # We can use Skyplane!!!
    client.copy(src="s3://jason-us-east-1/data/", 
                dst="s3://jason-us-west-2/data/", recursive=True)
    # Now skyplane copies it to a new region
    data_urls = get_urls("s3://jason-us-west-2/data")
    
    train_loader, model, criterion, optimizer = prepare_training(data_urls)
    train(train_loader, model, criterion, optimizer, epoch=5)