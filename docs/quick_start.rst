.. _quick_start:

=================
Quick Start Guide
=================

.. contents::
 :depth: 1
 :local:

Installation
------------

Installing Dependencies
~~~~~~~~~~~~~~~~~~~~~~~

FireX is built on top of Celery_, which requires a broker for message exchange. Currently, the only FireX-supported broker is Redis_.
While most FireX dependencies are installed automatically via ``pip`` when FireX itself is installed, it is necessary to have Redis installed before you
can run FireX. By default, FireX will look for redis binaries included in ``PATH``. You can verify that redis is installed by
running the following command.

.. _Celery: http://www.celeryproject.org/
.. _Redis: https://redis.io/

.. code-block:: bash

    > which redis-server

If redis is installed but not in your ``PATH``, it can be provided to FireX explicitly by setting the following
environment variable:

.. code-block:: bash

    > export redis_bin_dir=<path to redis directory containing the redis binaries>

For help installing Redis, please visit the `Redis documentation <https://redis.io/documentation>`_.


Installing FireX
~~~~~~~~~~~~~~~~

Installing FireX is quick and straightforward. Simply use pip in a Python 3.7 environment to install
the latest packages.

.. code-block:: bash

        > pip install firexapp[flame]

You can confirm that FireX is now installed by running basic FireX commands:

.. code-block:: text

    firexapp list --microservices
    firexapp info sleep


Execution
---------

Executing a Trivial Service
~~~~~~~~~~~~~~~~~~~~~~~~~~~

FireX internally defines a do-nothing service called ``nop``. The service can be executed by running:

.. code-block:: text

    > $ firexapp submit --chain nop
    [11:42:57][HOSTNAME] FireX ID: FireX-username-210122-114257-22938
    [11:42:57][HOSTNAME] Logs: /tmp/FireX-username-210122-114257-22938
    [11:43:00][HOSTNAME] Flame: http://HOSTNAME:59535

Follow the Flame link to visualize the FireX run and see that the ``nop`` service was executed. Of course,
since the service is trivial (i.e. receives no arguments, produces no results, and has no side-effects), there
isn't much to be seen, but running a ``nop`` verifies your FireX install is functioning.

.. _execute_own_service:

Executing Your Own Service
~~~~~~~~~~~~~~~~~~~~~~~~~~

The previous section showed how to run the built-in, trivial ``nop`` service. You can easily run your own service
by creating a file called ``hello.py`` and writing the following simple service definition:

.. code-block:: python

    from firexapp.engine.celery import app

    @app.task
    def hello_world():
        return 'Hello World!'

FireX can be told to load this file via the ``--plugins`` argument. The full command to invoke our new ``hello_world`` service
is therefore:

.. code-block:: text

    > firexapp submit --chain hello_world --plugins hello.py
    [11:33:42][HOSTNAME] FireX ID: FireX-username-210122-113342-5274
    [11:33:42][HOSTNAME] Logs: /tmp/FireX-username-210122-113342-5274
    External module hello imported
    [11:33:45][HOSTNAME] Flame: http://HOSTNAME:63749


Next Steps
----------
This page describes the bare minimum to get started with FireX -- there is still much more to discover!
`Take a look at the programming guide to learn about the most common FireX APIs. <programming_guide>`_

