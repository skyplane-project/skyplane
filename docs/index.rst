Welcome to Skyplane!
====================================

**Skyplane** efficiently transports data between cloud regions and providers.

.. note::

   This project is under active development.




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
   :caption: Overview

   quickstart

.. toctree::
   :maxdepth: 2
   :caption: Tutorials
   
   vm_to_vm


.. toctree::
   :maxdepth: 2
   :caption: Advanced Configuration

   configure



.. toctree::
   :maxdepth: 4
   :caption: Developer documentation

   contributing
   build_guide
   development_guide
   debugging

.. toctree::
   :maxdepth: 2
   :caption: Package documentation

   skyplane_cli
   skyplane_internal