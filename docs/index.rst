Welcome to Skyplane!
====================================

.. raw:: html

   <iframe src="https://ghbtns.com/github-btn.html?user=skyplane-project&repo=skyplane&type=star&count=true&size=large" frameborder="0" scrolling="0" width="170" height="30" title="GitHub"></iframe>

**Skyplane** is an secure, open-source tool for blazingly fast data transfers cloud object stores.

.. note::

   This project is under active development.



**skyplane cp copies files from one cloud provider to another**

Skyplane's cp command supports copying large datasets between cloud regions within a single provider:

.. code-block:: bash

   $ skyplane [sync/cp] [local/s3/gcp/azure]://mybucket/big_dataset [local/s3/gcp/azure]://mybucket2/


Contents
--------


.. toctree::
   :maxdepth: 2
   :caption: Overview

   quickstart
   configure
   architecture
   faq


.. toctree::
   :maxdepth: 4
   :caption: Developer documentation

   contributing
   development_guide
   roadmap
   debugging

.. toctree::
   :maxdepth: 2
   :caption: Package documentation

   skyplane_cli
   skyplane_cli_internal
   skyplane_internal
