.. _firex_prog_guide_enqueuing:

====================================
Enqueuing (Invoking) Services Chains
====================================

This page describes the different ways a service can schedule other chains of services for execution.

Like any other Python function, FireX services can call other Python functions, nothing really special or different from native Python there. But where things get interesting, is the ability for FireX services to schedule other chains of services.

FireX provides a wide range of APIs and primitives to handle various scheduling scenarios, like synchronous, asynchronous and parallel execution.

Some basic scenarios and usage are already described here:
:ref:`Blocking/synchronous enqueue. <blocking_enqueue>`
and
:ref:`Non-blocking/asynchronous enqueue. <nonblocking_enqueue>`


Enqueueing APIs
---------------

Executing a chain involves several steps, which may be be performed separately (for asynchronous invocations) or together (for synchronous invocations):

 - Enqueue/schedule the chain.
 - Wait for the chain to complete.
 - Extract the chain results.

APIs are available to perform each of these steps individually, but APIs that perform some/all of them together in a single call are also available to handle common use cases with a single call.

Some of the enqueueing APIs are also available in two declinations:

 - Methods of the FireX class accessed via ``self`` from within a service definition (preferred)
 - Standalone functions

Whenever available, **always use the FireX class methods to schedule services**, as it is simpler and has more context to perform proper services linkage and properties inheritance. Using the standalone functions should only only be required for extremely rare scenarios, which you are likely to never encounter.


Extracting the results
----------------------

Let's begin by discussing the chain results extraction. Although it's the last step of the process, it can be bundled in a single invocation with the scheduling, so it's important to understand this notion at the time of scheduling for such cases.

Conceptually, the results of a chain are essentially the ``bog`` of that chain after the last service has executed.
:ref:`Read about the bog here. <advanced_dataflow>`

This means a that:

 - The results are only available if the chain successfully executed to completion. If an exception was thrown by a service and the chain aborted, only the exception data will be available and results cannot be extracted.
 - Input values, which are part of the ``bog``, can also be extracted if desired.

Extracting the results of a chain provides you with more options and flexibility than a normal Python function call would.

All results extraction APIs provide the same :meth:`three knobs <microservices.firex_base.FireX.enqueue_child_and_get_results>`
to control the results extraction:

 - ``return_keys``: Is a Python list of the fields names you want the extract from the chain results. When specified, a list of values corresponding to the specified fields is returned. If not specified (default), all results will be returned as a standard Python dictionary.
 - ``merge_children_results``: When set to `True`, the result extraction will look for results in the child services (i.e. other services scheduled by the services in your chain). When set to `False` (default), only the results of the top level chain (i.e. the services explicitly part of your chain) are considered.
 - ``return_keys_only``: When set to `False`, the entire content of the ``bog`` will be considered, meaning input values will also be part of the returned set. When set to `True` (default), only the return values of services will be considered.


Enqueueing APIs of the FireX class
----------------------------------

To access the enqueuing APIs of the FireX class, make sure your services has access to ``self`` by using ``@app.task(bind=True,...)`` as described
:ref:`here. <blocking_enqueue>`

Synchronous scheduling and results extraction
=============================================
:meth:`self.enqueue_child_and_get_results <microservices.firex_base.FireX.enqueue_child_and_get_results>`
is the most commonly used API and will synchronously schedule a services chain, wait for it to complete, and return the results in a single invocation.

Use this when you want to execute services chain sequentially in a run-to-completion fashion.

Asynchronous scheduling and results extraction
==============================================
:meth:`self.enqueue_child <microservices.firex_base.FireX.enqueue_child>`
will schedule the specified chain and return immediately, without waiting for the chain to complete.

This API returns a Celery `AsyncResult <https://docs.celeryproject.org/en/stable/reference/celery.result.html#celery.result.AsyncResult>`_
object, which can be used later in the code to wait for completion and then extract the results.

Once a chain has been asynchronously scheduled, the following APIs are available to wait for completion:

 - wait_on_async_results(async_res): To wait for completion of the specified chain.
 - self.wait_for_children(): To wait for all async chains scheduled by this service to complete.
 - wait_for_any_results([async_res_list]) iterator: To wait for any of the specified ``AsyncResult`` objects in the list to complete.
 - self.wait_for_any_children() iterator: To wait for any async chains scheduled by this service to complete.

To avoid an error exception while waiting for the chain completion, you can specify:

.. code-block:: python

  raise_exception_on_failure=False

when waiting for the chain completion. By default, an exception will be raised during the "wait" if one of the services in the chains fails during the execution.

Once a chain has completed, use the `get_results <http://www.firexkit.com/api_reference.html#firexkit.result.get_results>`_
API to extract the results from the ``AsyncResult``.

Parallel scheduling and results extraction
==========================================

Although parallel scheduling can be achieved by using the native asynchronous APIs described above, FireX provides an API especially for that purpose, which has the added benefit of allowing you to control and cap the maximum number of chains which are allowed to execute in parallel. This is very useful when a large amount of chains need to be scheduled in parallel but you want to avoid exceed the Celery worker slots that are available to perform the work.


:meth:`self.enqueue_in_parallel() <microservices.firex_base.FireX.enqueue_in_parallel>` handles that scenario.

The method receives a list of chains as an input and will schedule up to the specified ``max_parallel_chains`` in parallel until they all have completed. The API returns a list of ``AsyncResult`` objects in which entries correspond the list of chains provided as input. So ``results[0]`` contains the async results of ``chain[0]``, ``results[1]`` the sync results of ``chain[1]``, etc. etc.

..
    TODO: port examples





