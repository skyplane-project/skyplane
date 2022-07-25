***************
Getting Started
***************

Installation
-----------------------
We're ready to install Skyplane. It's as easy as:

.. code-block:: bash

   ---> Install skyplane from PyPI:
   $ pip install skyplane-nightly

.. dropdown for M1 Macbook users
.. note::

   If you are using an M1 Macbook with the arm64 architecture, you will need to install skyplane as follows:

   .. code-block:: bash

      $ GRPC_PYTHON_BUILD_SYSTEM_OPENSSL=1 GRPC_PYTHON_BUILD_SYSTEM_ZLIB=1 pip install skyplane-nightly

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


Transferring Data
-------------------

We're ready to use Skyplane! Let's use `skyplane cp` to copy files from AWS to GCP:

.. code-block:: bash

   ---> 🎸 Ready to rock and roll! Copy some files:
   $ skyplane cp s3://... gs://...

To transfer only new objects, you can instead use `skyplane sync`: 

.. code-block:: bash

   ---> Copy only diff
   $ skyplane sync s3://... gs://...
