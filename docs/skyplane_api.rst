API documentation
=================

.. code-block:: python
    :caption: example of how to use API simple copy that automatically deprovisions the VMs

    import skyplane

    if __name__ == "__main__":
        client = skyplane.SkyplaneClient(aws_config=skyplane.AWSConfig())
        print(f"Log dir: {client.log_dir}/client.log")
        client.copy(src="s3://skycamp-demo-src/synset_labels.txt", dst="s3://skycamp-demo-us-east-2/imagenet-bucket/synset_labels.txt", recursive=False)


.. code-block:: python
    :caption: example of how to use API dataplane session

    # from https://github.com/skyplane-project/skyplane/blob/main/examples/api_demo.py
    import time
    import skyplane

    if __name__ == "__main__":
        client = skyplane.SkyplaneClient(aws_config=skyplane.AWSConfig())
        print(f"Log dir: {client.log_dir}/client.log")
        dp = client.dataplane("aws", "us-east-1", "aws", "us-east-2", n_vms=1)
        with dp.auto_deprovision():
            dp.provision(spinner=True)
            dp.queue_copy(
                "s3://skycamp-demo-src/synset_labels.txt", "s3://skycamp-demo-us-east-2/imagenet-bucket/synset_labels.txt", recursive=False
            )
            tracker = dp.run_async()

            # monitor the transfer
            print("Waiting for transfer to complete...")
            while True:
                # handle errors
                if tracker.errors:
                    for ip, error_list in tracker.errors.items():
                        for error in error_list:
                            print(f"Error on {ip}: {error}")
                    break

                bytes_remaining = tracker.query_bytes_remaining()
                timestamp = time.strftime("%H:%M:%S", time.localtime())
                if bytes_remaining is None:
                    print(f"{timestamp} Transfer not yet started")
                elif bytes_remaining > 0:
                    print(f"{timestamp} {(bytes_remaining / (2 ** 30)):.2f}GB left")
                else:
                    break
                time.sleep(1)
            tracker.join()
            print("Transfer complete!")

Contents

.. toctree::
    _api/skyplane.api
