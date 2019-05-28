.. _quick_start:

=================
Quick Start Guide
=================


Installation
------------

Installing FireXApp is quick and straightforward. Simply use pip to install the latest packages.

.. code-block:: text

        pip install firexapp

To install from the latest source files:

.. code-block:: text

    pip install git+https://github.com/FireXStuff/firexapp.git

You can confirm that FireXApp is now installed by running basic FireXApp commands:

.. code-block:: text

    firexapp list --microservices
    firexapp info sleep


Broker
~~~~~~

FireX is built on top of Celery_, and as such, requires a broker. Currently, the only supported broker is Redis_. By
default, FireXApp will look for redis binaries in included in PATH. If you have redis install, you can verify this by
running the following command.

.. _Celery: http://www.celeryproject.org/
.. _Redis: https://redis.io/

.. code-block:: bash

    which redis-server

If redis is installed but not in your path, it can be provided to FireXApp explicitly by setting the following
environment variable:

.. code-block:: bash

    export redis_bin_base=<path to redis directory containing the redis binaries>

For help installing Redis, please visit the `Redis documentation <https://redis.io/documentation>`_.

Tasks
-----

Basic Usage
-----------

Example
-------

Bundles
-------

Logs
----

Flame
-----
