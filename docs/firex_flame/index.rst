FireX Flame
===========

Flame is a lightweight server that collects, serves, and presents data from a
FireX execution. It receives events from the FireX backend and aggregates them
into the Flame data model, which is available through REST and Socket.IO APIs.
Flame also serves the Flame UI for viewing a single execution.

The Flame server is typically ephemeral. A FireX execution launches its own
server, which remains available after the workflow completes and terminates
automatically after its configured timeout.

The implementation is distributed as part of ``firexapp`` while retaining the
``firex_flame`` import namespace, ``firex_flame`` command, and Flame
tracking-service entry point. The separately released
`FireX Flame UI <https://github.com/FireXStuff/firex-flame-ui>`_ remains a
runtime dependency during this migration stage.

API Reference
-------------

.. automodule:: firex_flame.api
   :members:

.. automodule:: firex_flame.controller
   :members:

.. automodule:: firex_flame.event_broker_processor
   :members:

.. automodule:: firex_flame.event_file_processor
   :members:

.. automodule:: firex_flame.flame_helper
   :members:

.. automodule:: firex_flame.flame_task_graph
   :members:

.. automodule:: firex_flame.launcher
   :members:

.. automodule:: firex_flame.main_app
   :members:

.. automodule:: firex_flame.model_dumper
   :members:

.. automodule:: firex_flame.web_app
   :members:
