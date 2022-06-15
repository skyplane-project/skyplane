Welcome to Skyplane!
====================================

**Skyplane** is an secure, open-source tool for blazingly fast data transfers cloud object stores.

.. note::

   This project is under active development.



**skyplane cp copies files from one cloud provider to another**

Skyplane's cp command supports copying large datasets between cloud regions within a single provider:

.. code-block:: bash

   $ skyplane [sync/cp] [local/s3/gcp/azure]://mybucket/big_dataset [local/s3/gcp/azure]://mybucket2/

TODO: Insert performance table here


Contents
--------


.. toctree::
   :maxdepth: 2
   :caption: Overview

   quickstart
   configure
   overview
   faq


.. toctree::
   :maxdepth: 4
   :caption: Developer documentation

   contributing
   development_guide
   debugging

.. toctree::
   :maxdepth: 2
   :caption: Package documentation

   skyplane_cli
   skyplane_internal
