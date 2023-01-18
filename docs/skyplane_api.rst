=================
API documentation
=================

Welcome to Skyplane API!
=======================

Data transfer across the cloud is often a critical component of most project pipelines, and Skyplane's simplicity and efficiency is an attractive tool for developers to handle this stage of the process. For easy and fast integration, Skyplane offers an API which enables users the same functionality as the CLI (e.g. copy, sync, etc.) along with some API-specific features. This has exciting implications for the growth of Skyplane and applications leveraging it going forward.

This API is still experimental, and your feedback is much appreciated in improving it!

Installing Skyplane
===================

We have provided a detailed tutorial `here <https://skyplane.org/en/latest/quickstart.html>`_.

How to import Skyplane
======================

To access Skyplane and its functions, you can import it in your Python code like this:
.. code-block:: python
    import skyplane
Really easy!

How to launch a Skyplane transfer job
======================================

To start, you need to create a Skyplane client that reads the cloud keys. Every transfer job is managed by this client or the dataplane configured by the client.

Then, Skyplane provides two different ways to transfer: 1. simply copy that takes care of the VM provision and deprovision so you can launch a transfer with one line; 2. dataplane session that gives you more freedom to launch multiple transfer jobs together and asynchronously.

Dataplane calculates the optimal transfer topology between the source and the destination regions and can be reused to launch jobs later.

Below are the two examples using the methods mentioned above.

.. code-block:: python
    :caption: Example of how to use API simple copy that automatically deprovisions the VMs

    import skyplane

    client = skyplane.SkyplaneClient(aws_config=skyplane.AWSConfig())
    print(f"Log dir: {client.log_dir}/client.log")
    client.copy(src="s3://skycamp-demo-src/synset_labels.txt", dst="s3://skycamp-demo-us-east-2/imagenet-bucket/synset_labels.txt", recursive=False)


.. code-block:: python
    :caption: Example of how to use API dataplane session

    # from https://github.com/skyplane-project/skyplane/blob/main/examples/api_demo.py
    import skyplane

    client = skyplane.SkyplaneClient(aws_config=skyplane.AWSConfig())
    print(f"Log dir: {client.log_dir}/client.log")
    dp = client.dataplane("aws", "us-east-1", "aws", "us-east-2", n_vms=1)
    with dp.auto_deprovision():
        dp.provision()
        dp.queue_copy(
            "s3://skycamp-demo-src/synset_labels.txt", "s3://skycamp-demo-us-east-2/imagenet-bucket/synset_labels.txt", recursive=False
        )
        tracker = dp.run_async()
        # You can monitor the transfer by querying tracker.query_bytes_remaining()

Integrations with other applications
====================================



Contents

.. toctree::
    _api/skyplane.api
