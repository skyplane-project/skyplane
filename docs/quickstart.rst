***************
Getting Started
***************

Installation
-----------------------
We're ready to install Skyplane. It's as easy as:

.. code-block:: bash

   ---> Install skyplane from PyPI:
   $ pip install "skyplane[aws]"

   # install support for other clouds as needed:
   #   $ pip install "skyplane[azure]"
   #   $ pip install "skyplane[gcp]"
   #   $ pip install "skyplane[all]"

.. dropdown for M1 Macbook users
.. note::

   If you are using an M1 Macbook with the arm64 architecture, you will need to install skyplane as follows:

   .. code-block:: bash

      $ GRPC_PYTHON_BUILD_SYSTEM_OPENSSL=1 GRPC_PYTHON_BUILD_SYSTEM_ZLIB=1 pip install skyplane[all]

Cloud Credentials
-----------------------
Skyplane needs access to cloud credentials to perform transfers. To get started with setting up credentials, make sure you have cloud provider CLI tools installed: 

.. code-block:: bash

   ---> For AWS:
   $ pip install awscli

   ---> For Google Cloud:
   $ pip install gcloud

   ---> For Azure:
   $ pip install azure

Once you have the CLI tools setup, log into each cloud provider's CLI: 

.. code-block:: bash

   ---> For AWS:
   $ aws configure

   ---> For Google Cloud:
   $ gcloud auth application-default login

   ---> For Azure:
   $ az login

Now, you can initialize Skyplane with your desired cloud providers. Skyplane autodetects cloud credentials and valid regions from your CLI environment.

.. code-block:: bash
   
   ---> Setup cloud provider connectors:
   $ skyplane init


Transferring Data via Skyplane CLI
------------------------------------

We're ready to use the Skyplane CLI! Let's use `skyplane cp` to copy files from AWS to GCP:

.. code-block:: bash

   ---> 🎸 Ready to rock and roll! Copy some files:
   $ skyplane cp -r s3://... gs://...

To transfer only new objects, you can instead use `skyplane sync`: 

.. code-block:: bash

   ---> Copy only diff
   $ skyplane sync s3://... gs://...

Transferring Data via Skyplane API
------------------------------------

We can also leverage the power of the Skyplane API! To access Skyplane and its functions, you can import it in your Python code like this:

.. code-block:: python

    import skyplane

To start a simple copy job using the Skyplane API, we simply create a SkyplaneClient and call `copy`:

.. code-block:: python
    :caption: Example of how to use API simple copy that automatically deprovisions the VMs

    import skyplane

    client = skyplane.SkyplaneClient(aws_config=skyplane.AWSConfig())
    client.copy(src="s3://skycamp-demo-src/synset_labels.txt", dst="s3://skycamp-demo-us-east-2/imagenet-bucket/synset_labels.txt", recursive=False)

.. note::
   In this example, we use a defauly AWSConfig which infers AWS credentials from the environment.