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
   $ pip install skyplane

Skyplane autodetects cloud credentials and valid regions from your CLI environment.

.. code-block:: bash
   
   ---> Setup cloud provider connectors:
   $ skylark init

We're ready to use Skyplane! Let's use `skylark cp` to copy files from AWS to GCP:

.. code-block:: bash

   ---> 🎸 Ready to rock and roll! Copy some files:
   $ skylark cp s3://... gs://...


What can you do with Skyplane?
------------------------------

Skyplane's cp command supports copying large datasets between cloud regions within a single provider:

.. code-block:: bash

   $ skylark cp s3://mybucket/big_dataset s3://mybucket2/

Skyplane also supports copying between providers:

.. code-block:: bash

   $ skylark cp s3://mybucket/big_dataset gs://mybucket2/

For convenience, Skyplane also supports copying to and from your local filesystem:

.. code-block:: bash

   $ skylark cp /path/to/dir s3://mybucket/big_dataset
   $ skylark cp s3://mybucket/big_dataset /path/to/dir

For cloud-to-cloud transfers, once ``skylark`` provisions all gateway VMs, transfers can complete at up to 50Gb/s.

.. todo add gif of transfer bar here


Contents
--------

.. toctree::
   :maxdepth: 2
   :caption: Developer documentation

   contributing
   build_guide

.. toctree::
   :maxdepth: 2
   :caption: API documentation

   skylark_cli