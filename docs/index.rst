Welcome to Skyplane!
====================================

.. raw:: html

   <a href="https://join.slack.com/t/skyplaneworkspace/shared_invite/zt-1cxmedcuc-GwIXLGyHTyOYELq7KoOl6Q"><img src="https://img.shields.io/badge/-Join%20Skyplane%20Slack-blue?logo=slack" style="height: 30px;" /></a>
   <iframe src="https://ghbtns.com/github-btn.html?user=skyplane-project&repo=skyplane&type=star&count=true&size=large" frameborder="0" scrolling="0" width="170" height="30" title="GitHub"></iframe>

.. note::

   This project is under active development.

**🔥 Blazing fast bulk data transfers between any cloud 🔥**

Skyplane is a tool for blazingly fast bulk data transfers in the cloud. Skyplane manages parallelism, data partitioning, and network paths to optimize data transfers, and can also spin up VM instances to increase transfer throughput. 

You can use Skyplane to transfer data: 
* Between buckets within a cloud provider
* Between object stores across multiple cloud providers
* (experimental) Between local storage and cloud object stores

Copy a large dataset in the cloud in a minute, not hours:

.. code-block:: bash

   $ pip install skyplane
   $ skyplane init
   $ skyplane [sync/cp] [local/s3/gcp/azure]://mybucket/big_dataset [local/s3/gcp/azure]://mybucket2/


Contents
--------


.. toctree::
   :maxdepth: 2
   :caption: Overview

   quickstart
   benchmark
   configure
   architecture
   usage_stats_collection
   faq


.. toctree::
   :maxdepth: 4
   :caption: Developer documentation

   build_from_source
   contributing
   roadmap
   debugging

.. toctree::
   :maxdepth: 2
   :caption: Package documentation

   skyplane_cli
   skyplane_cli_internal
   skyplane_internal

.. toctree::
   :caption: Community 

    Slack <https://join.slack.com/t/skyplaneworkspace/shared_invite/zt-1cxmedcuc-GwIXLGyHTyOYELq7KoOl6Q>
