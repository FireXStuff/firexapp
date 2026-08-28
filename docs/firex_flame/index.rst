FireX Flame
===========

Flame is a lightweight server that collects, serves, and presents data from a
FireX execution. It receives events from the FireX backend and aggregates them
into the Flame data model, which is available through REST and Socket.IO APIs.
Flame also serves the Flame UI for viewing a single execution.

The Flame server is typically ephemeral. A FireX execution launches its own
server, which remains available after the workflow completes and terminates
automatically after its configured timeout.

The server and UI are distributed as part of ``firexapp`` while retaining the
``firex_flame`` and ``firex_flame_ui`` import namespaces, ``firex_flame``
command, and Flame tracking-service entry point. Frontend source and tests
live in ``firex_flame_ui_frontend``; its production build populates the
``firex_flame_ui`` resource package included in the FireXApp artifact.

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
