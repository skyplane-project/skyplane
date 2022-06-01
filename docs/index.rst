Welcome to Skyplane!
====================================

**Skyplane** efficiently transports data between cloud regions and providers.

.. note::

   This project is under active development.

How to install Skyplane
-----------------------

Before using Skyplane, you should log into each cloud provider's CLI:

.. code-block:: bash

   ---> For AWS:
   $ aws configure

   ---> For Google Cloud:
   $ gcloud auth application-default login

   ---> For Azure:
   $ az login


We're ready to install Skyplane. It's as easy as:

.. code-block:: bash

   ---> Install skyplane from PyPI:
   $ pip install skyplane-nightly

.. dropdown for M1 Macbook users
.. note::

   If you are using an M1 Macbook with the arm64 architecture, you will need to install skyplane as follows:

   .. code-block:: bash

      $ GRPC_PYTHON_BUILD_SYSTEM_OPENSSL=1 GRPC_PYTHON_BUILD_SYSTEM_ZLIB=1 pip install skyplane-nightly

Skyplane autodetects cloud credentials and valid regions from your CLI environment.

.. code-block:: bash
   
   ---> Setup cloud provider connectors:
   $ skyplane init

We're ready to use Skyplane! Let's use `skyplane cp` to copy files from AWS to GCP:

.. code-block:: bash

   ---> 🎸 Ready to rock and roll! Copy some files:
   $ skyplane cp s3://... gs://...


What can you do with Skyplane?
------------------------------

**skyplane cp copies files from one cloud provider to another**

Skyplane's cp command supports copying large datasets between cloud regions within a single provider:

.. code-block:: bash

   $ skyplane cp s3://mybucket/big_dataset s3://mybucket2/

Skyplane also supports copying between providers:

.. code-block:: bash

   $ skyplane cp s3://mybucket/big_dataset gs://mybucket2/

For convenience, Skyplane also supports copying to and from your local filesystem:

.. code-block:: bash

   $ skyplane cp /path/to/dir s3://mybucket/big_dataset
   $ skyplane cp s3://mybucket/big_dataset /path/to/dir

For cloud-to-cloud transfers, once ``skyplane`` provisions all gateway VMs, transfers can complete at up to 50Gb/s.

.. todo add gif of transfer bar here

**skyplane sync only copies new objects**

Skyplane's sync command will diff the contents of two buckets and only copy new objects:

.. code-block:: bash

   $ skyplane sync s3://mybucket/big_dataset gs://mybucket2/

It determines if an object is new by comparing the last modified time as well as the size of the object (similar to `AWS S3 sync <https://docs.aws.amazon.com/cli/latest/reference/s3/sync.html#examples>`_). Skyplane sync is safe to run as it will *NOT* delete objects that are no longer present in the source bucket.

Contents
--------

.. toctree::
   :maxdepth: 2
   :caption: Developer documentation

   contributing
   build_guide

.. toctree::
   :maxdepth: 2
   :caption: Package documentation

   skyplane_cli
   skyplane_internal