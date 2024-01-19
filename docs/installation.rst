***************
Installation
***************

We're ready to install Skyplane. It's as easy as:

.. code-block:: bash

   ---> Install skyplane from PyPI:
   $ pip install "skyplane[aws]"

   # install support for other clouds as needed:
   #   $ pip install "skyplane[azure]"
   #   $ pip install "skyplane[gcp]"
   #   $ pip install "skyplane[scp]"
   #   $ pip install "skyplane[all]"

.. dropdown for M1 Macbook users
.. note::

   If you are using an M1 Macbook with the arm64 architecture, you will need to install skyplane as follows:

   .. code-block:: bash

      $ GRPC_PYTHON_BUILD_SYSTEM_OPENSSL=1 GRPC_PYTHON_BUILD_SYSTEM_ZLIB=1 pip install skyplane[all]

Setting up Cloud Credentials
-----------------------------
Skyplane needs access to cloud credentials to perform transfers. To get started with setting up credentials, make sure you have cloud provider CLI tools installed: 

.. code-block:: bash

   ---> For AWS:
   $ pip install awscli

   ---> For Google Cloud:
   $ pip install gcloud

   ---> For Azure:
   $ pip install azure-cli

Once you have the CLI tools setup, log into each cloud provider's CLI: 

.. code-block:: bash

   ---> For AWS:
   $ aws configure

   ---> For Google Cloud:
   $ gcloud auth application-default login

   ---> For Azure:
   $ az login

   ---> For SCP:
   $ # Create directory if required
   $ mkdir -p ~/.scp
   $ # Add the lines for "scp_access_key", "scp_secret_key", and "scp_project_id" to scp_credential file
   $ echo "scp_access_key = <your_access_key>" >> ~/.scp/scp_credential
   $ echo "scp_secret_key = <your_secret_key>" >> ~/.scp/scp_credential
   $ echo "scp_project_id = <your_project_id>" >> ~/.scp/scp_credential
   
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

To transfer from local disk or HDFS cluster, you can use `skyplane cp` as well:

(Note: On-Prem require additional setup. Please navigate to the `On-Prem` section for more details)

.. code-block:: bash

   ---> Copy from local disk
   $ skyplane cp -r /path/to/local/file gs://...

   ---> Copy from HDFS
   $ skyplane cp -r hdfs://... gs://...
