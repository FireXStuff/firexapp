FireX Keeper
============

FireX Keeper is a lightweight process that collects and persists task data
from a FireX execution in an SQLite database. The database can be queried for
task results, run states, arguments, and execution relationships.

The implementation is distributed as part of ``firexapp`` while retaining the
``firex_keeper`` import namespace, ``firex_keeper`` command, and Keeper
tracking-service entry point.

API Reference
-------------

.. automodule:: firex_keeper.db_model
   :members:

.. automodule:: firex_keeper.keeper_event_consumer
   :members:

.. automodule:: firex_keeper.keeper_helper
   :members:

.. automodule:: firex_keeper.keeper_launcher
   :members:

.. automodule:: firex_keeper.persist
   :members:

.. automodule:: firex_keeper.task_query
   :members:
