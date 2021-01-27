.. _firex_prog_guide:

=================
Programming Guide
=================

.. warning::
    It's best to follow this guide while executing the examples in a FireX installation. Take a look at the
    :ref:`Quick Start Guide <quick_start>` to create and validate your own FireX installation. In particular,
    see how to :ref:`run your own code <execute_own_service>`  to follow the guide interactively.


This guide serves programmers new to writing FireX services by discussing and demonstrating the most common parts of the FireX
API to incrementally implement a simple set of
services. Knowledge of the Python programming language is assumed.
Understanding everything in this guide will leave readers in a strong position to comprehend
and author FireX workflows.

If you are interested in immediately viewing and executing the code from this guide, take a look at
`the final result in example.py <https://github.com/FireXStuff/firexapp/blob/master/firexapp/tasks/example.py>`_.

The commits at the top of the `programming-guide branch <https://github.com/FireXStuff/firexapp/commits/programming-guide>`_
include the code and tests for all sections of the guide.

.. contents::
 :depth: 2
 :local:

.. _trivial_service:

A Trivial Service
=================
`View Example Code <https://github.com/FireXStuff/firexapp/blob/d53ae754abfd6bc9e496ada2b01e891a6f8d8094/firexapp/tasks/example.py>`__

The simplest FireX service is a python function with a single, FireX-specific decorator.

.. code-block:: python

    from getpass import getuser
    from firexapp.engine.celery import app

    @app.task()
    def greet(name=getuser()):
        return 'Hello %s!' % name

FireX exposes all services via the CLI, making it already possible to invoke the new ``greet`` service and view the
resulting graph in Flame:

.. code-block:: text

    firexapp submit --chain greet


`View greet in Flame <http://www.firexflame.com/#/FireX-username-210127-125133-35215/>`_

With only this straightforward service definition, developers can leverage:

    - a CLI entry point
    - telemetry data exported via Kafka, including service inputs, outputs and passed/failed/running statuses for monitoring and alerting
    - workflow visualization via Flame, including call-tree hierarchy and timing breakdown
    - a multi-process and multi-host environment
    - a unique identifier (the FireX ID) per FireX invocation
    - log aggregation at various granularity, including per-service, per-host and run-wide logging

FireX strives to provide a non-intrusive API in the simplest case, expecting workflows will mostly be written in standard
Python code then integrated via FireX. Read on to learn about using FireX to create more involved workflows.


.. _composing_services:

Composing Services
==================

Visualizing and reasoning about large, complex workflows is where FireX shines. This section introduces the
APIs available to programatically invoke one service from another -- the first step towards building
involved workflows.

.. _blocking_enqueue:

Blocking/synchronous Enqueue (Invoke)
-------------------------------------
`View Example Code <https://github.com/FireXStuff/firexapp/blob/987761054ae2f167c8dc3fc3756b437237a8a7d1/firexapp/tasks/example.py>`__

A ``greet_guests`` service will be created to invoke ``greet`` multiple times and aggregate the results.


First, it is necessary to modify ``greet`` so that its result can be referred to by name from invoking services.
The initial implementation was kept purposefully minimal; ordinarily all service definitions that return values give them
names. Add ``returns=['greeting']`` to give the return value the name ``greeting``.
:ref:`Read more about returns. <firex_prog_guide_returns>`

.. code-block:: python

    @app.task(returns=['greeting'])
    def greet(name=getuser()):
        return 'Hello %s!' % name


The ``greet_guests`` service can now be defined as:

.. code-block:: python

    @app.task(bind=True, returns=['guests_greeting'])
    def greet_guests(self, guests):
        greetings = []
        for guest in guests:
            greet_signature = greet.s(name=guest)
            greet_results = self.enqueue_child_and_get_results(greet_signature)
            greetings.append(greet_results['greeting'])
        return ' '.join(greetings)


`See diff with the previous code here. <https://github.com/FireXStuff/firexapp/commit/987761054ae2f167c8dc3fc3756b437237a8a7d1>`__

Let's go over the FireX APIs introduced by the new ``greet_guests`` service that aggregates results from its
child ``greet`` services:

- ``bind=True``

 - Supplying ``bind=True`` to ``app.task`` makes the FireX Task instance ``self`` the first argument to the function
   definition. ``self`` provides access to data and functions made available by FireX.
   In this example, ``self`` is only used to enqueue (i.e. invoke) child services.

 - :ref:`Read more about the uses of 'self' here <firex_prog_guide_firex_base_self>`.

- ``greet.s(name=guest)`` (or more generally, ``<service_name>.s(<service arguments>)``)

 - Creates a Celery Signature,
   `details can be read here. <https://docs.celeryproject.org/en/latest/userguide/canvas.html#signatures/>`_

 - Celery Signatures bind arguments to a service, and can then be enqueued to eventually be executed. Note that
   depending on how it is enqueued, the service might run immediately or need to wait for resources. It's important to
   keep in mind creating the signature does not
   execute the service, but rather enqueuing the signature schedules the service for that signature to be executed.

- ``self.enqueue_child_and_get_results(<celery signature>)``

 - Schedules the supplied Celery Signature for immediate execution, waiting on and returning the results. The return
   value is a Python ``dict`` that contains the ``returns`` keys from the invoked service. In this example, the
   ``greet`` service defines its return value's name to be ``greeting``.

 - :ref:`There are several ways to enqueue child services; read more here. <firex_prog_guide_enqueuing>`

However, there is a detail to address before invoking ``greet_guests`` from the command-line.  Since ``greet_guests``
takes a list of names and the command line receives a string, it is necessary to transform a string argument from the CLI
value of ``guests`` in to a python list by using ``@InputConverter.register`` and ``@SingleArgDecorator``.

.. code-block:: python


    @InputConverter.register
    @SingleArgDecorator('guests')
    def to_list(guests):
        return guests.split(',')


:ref:`Read more about @InputConverter and @SingleArgDecorator <firex_prog_guide_arg_convert>`.

The new ``greet_guests`` service can now be executed:

.. code-block:: text

    firexapp submit --chain greet_guests --guests Li,Mohamed

`View greet_guests in Flame. <http://www.firexflame.com/#/FireX-username-210127-125232-2349>`_

Observe that ``--guests`` was automatically made available as a command-line argument since it is an argument to the ``greet_guests``
service. FireX also automatically generates a help for your service:

.. code-block:: text

    firexapp info greet_guests

You can augment the ``info`` with more details, like a description of the argument, by adding a docstring to your service.

While this example chose to schedule and block on child services, it's also possible to schedule services asynchronously.
Continue on to the next section for details.

.. _nonblocking_enqueue:

Non-Blocking/Asynchronous Enqueue (Invoke)
------------------------------------------
`View Example Code <https://github.com/FireXStuff/firexapp/blob/8a284aeccca7b24694b5b3f0dd5ca23dcea5b636/firexapp/tasks/example.py>`__


In the previous example, the result from each ``greet`` was received before the next call to ``greet`` was performed;
child services were executed sequentially. If ``greet`` were a more expensive service, it will be preferable to leverage FireX as
a multi-process environment by invoking all child services in parallel and then waiting for all results to become available.

.. code-block:: python


    @app.task(bind=True, returns=['guests_greeting'])
    def greet_guests(self, guests):
        child_promises = []
        for guest in guests:
            greet_signature = greet.s(name=guest)
            child_promise = self.enqueue_child(greet_signature)
            child_promises.append(child_promise)

        self.wait_for_children()
        greetings = [promise.result['greeting'] for promise in child_promises]
        return ' '.join(greetings)

`See diff with the previous code here. <https://github.com/FireXStuff/firexapp/commit/8a284aeccca7b24694b5b3f0dd5ca23dcea5b636>`__


Take note of the FireX APIs used to achieve parallel execution of child services:

- ``self.enqueue_child(<celery signature>)``

 - Unlike ``enqueue_child_and_get_results``, the ``enqueue_child`` method schedules the supplied signature for execution
   asynchronously and immediately returns the newly created child result promise. It is the caller's responsibility to
   extract the return value from the promise **after the caller knows the result is available.**

 - :ref:`There are several ways to enqueue child services; read more here. <firex_prog_guide_enqueuing>`

- ``self.wait_for_children``

 - Blocks on the completion of all child services. Once this method has returned, it's safe to inspect the ``result``
   attribute of all child result promises to retrieve the return values of the executed service.

.. _chaining_dataflow:

Dataflow via Chaining (the '|' operator)
----------------------------------------
`View Example Code <https://github.com/FireXStuff/firexapp/blob/02de1664bdedbccfb5c2a81770ed57eb0c9094a5/firexapp/tasks/example.py>`__

The preceding examples enqueue signatures from single services. It's also possible to build a signature that
composes multiple services and executes them as a unit.

Two new services will be created to demonstrate chaining: the outer service ``amplified_greet_guests`` will chain the
existing ``greet_guests`` service with a new, trivial ``amplify`` service.


.. code-block:: python

    @app.task(returns=['amplified_message'])
    def amplify(guests_greeting):
        return guests_greeting.upper()


A chain will be created to send the ``guests_greeting`` result of the ``greet_guests`` service along to the argument named ``guests_greeting``
of the ``amplify`` service, then return the result as ``amplified_greeting``:

.. code-block:: python

    @app.task(bind=True, returns=['amplified_greeting'])
    def amplified_greet_guests(self, guests):

        amplified_greet_guests_chain = greet_guests.s(guests=guests) | amplify.s()

        chain_results = self.enqueue_child_and_get_results(amplified_greet_guests_chain)
        return chain_results['amplified_message']


`View diff with previous section. <https://github.com/FireXStuff/firexapp/commit/02de1664bdedbccfb5c2a81770ed57eb0c9094a5>`_

.. warning:: Chains are built from **signatures**, not service names, so don't forget the ``.s(...)``!

The chain operator ``|`` is used to combine two signatures in to a single chain. The ``greet_guests`` service will produce
a result named ``guests_greeting``, which is consumed as input by ``amplify``.
Notice that binding by names can lead to coupling; ``amplify``
doesn't know about ``greet_guests``, so why should ``amplify`` have an input argument named ``guests_greeting``? The mapping
from names present in the chain to argument names expected by a service can be reassigned, allowing ``amplify`` to have
a more general input argument name, such as ``to_amplify``.

.. code-block:: python

    @app.task(returns=['amplified_message'])
    def amplify(to_amplify):
        return to_amplify.upper()

We can reassign the name received by ``amplify`` by changing its signature construction to
``amplify.s(to_amplify='@guests_greeting')``, so that the chain becomes:

.. code-block:: python

        amplified_greet_guests_chain = greet_guests.s(guests=guests) | amplify.s(to_amplify='@guests_greeting')


The ``amplified_greet_guests`` service can be executed:

.. code-block:: text

    firexapp submit --chain amplified_greet_guests --guests Li,Mohamed

`View amplified_greet_guests in Flame. <http://www.firexflame.com/#/FireX-username-210127-125341-5024>`_

With this example in mind, chaining can be discussed in more general terms. The input arguments flow from the first service to the next, with
services later in the chain receiving inputs that may have been created or updated by return values produced by earlier services.

So, if you a have a chain:

   A | B | C

then:

 - A input can use: all explicit arguments values specified for A.
 - B input can use: all arguments values A can use + the return values of A + all explicit arguments values of B.
 - C input can use: all arguments values B can use + the return values of B + all explicit arguments values of C.

A data context can be created to make many arguments available to all services in a chain via the ``InjectArgs`` construct:

    InjectArgs | A | B | C

``InjectArgs`` is a pseudo-service that can be used to inject a dictionary of arguments/values at the head of a chain.
It can be used only once and only at the head of the chain, not between services.

..
    TODO: create example that shows using InjectArgs that is open-source applicable.
    :ref:`Read more about this here. <advanced_dataflow>`

.. note::
    Remember: if a service updates a value with an existing name, it will override the previous value for downstream
    services in the chain.

`The Celery mechanics of chaining are described here. <https://docs.celeryproject.org/en/latest/userguide/canvas.html#chains>`_

Chaining is fundamentally a convenience for assembling involved workflows to have results available to downstream
services. If any service in the chain fails, subsequent services will not be executed.
The same outcome can be achieved by calling ``enqueue`` methods, extracting results, and making those
results (as well as other required inputs) available to the next service in the would-be
chain, then calling ``enqueue`` again.
Complex workflows often have significant logic while constructing chains by conditionally assembling lower-level services.


Error Propagation
=================
`View Example Code <https://github.com/FireXStuff/firexapp/blob/44b32209b69de673b4dc610a56d0e872426ca687/firexapp/tasks/example.py>`__

Thus far, no services have failed while executing; results from complete service executions have always been available. The
``greet`` leaf-node service will be modified so that it may fail, then calling services can observe failures and decide how to
handle the them.

.. code-block:: python

    @app.task(returns=['greeting'], flame=['greeting'])
    def greet(name=getpass.getuser()):
        assert len(name) > 1, "Cannot greet a name with 1 or fewer characters."
        return 'Hello %s!' % name

Now, when ``greet_guests`` calls ``self.wait_for_children()`` to wait for its ``greet`` children,
``wait_for_children`` may raise a ``ChainInterruptedException`` that is caused by the ``AssertionError`` from ``greet``.
Be aware that the ``ChainInterruptedException`` will be raised **after all children have completed** (i.e. either produced results or
raised exceptions).

``greet_guests`` can inspect the child task failures instead of automatically raising a ``ChainInterruptedException`` by
invoking ``self.wait_for_children(raise_exception_on_failure=False)``:

.. code-block:: python

    @app.task(...)
    def greet_guests(self, guests):
        ...

        self.wait_for_children(raise_exception_on_failure=False)
        greetings = [promise.result['greeting'] for promise in child_promises if promise.successful()]

        if any(promise.failed() for promise in child_promises):
            greetings.append("And apologies to those not mentioned.")

        return ' '.join(greetings)

`See diff with the previous code here. <https://github.com/FireXStuff/firexapp/commit/44b32209b69de673b4dc610a56d0e872426ca687>`__

After setting ``raise_exception_on_failure=False``, it's no longer safe to immediately inspect the ``promise.result`` value
as a dict. Instead, it's necessary to distinguish between successful children that have a ``result`` dict and failed
children that have ``result`` set to the exception that was raised for that child service.

The ``greet_guests`` service can be executed to purposefully make a ``greet`` service fail:

.. code-block:: text

    firexapp submit --chain greet_guests --guests Li,A

`View greet_guests with a greet failure in Flame. <http://www.firexflame.com/#/FireX-username-210127-125454-25577>`_

In general, calls that block on child results, such as ``self.enqueue_child_and_get_results`` and ``self.wait_for_children``,
will by default raise a ``ChainInterruptedException`` when the enqueued chain fails. Conversely, if a parent service enqueues a child
asyncronously and that child fails, the parent service is not affected. It's only when a parent is waiting on a child's
result that errors propagate and fail the parent (unless error handling, such as exception catching or
``raise_exception_on_failure=False``, is used by the parent).


Subject Specific Guides
=======================
The simple services described in this guide should give readers an understanding of the most common FireX APIs. For more
details on the topics touched upon here, refer to the subject specific guides:

.. toctree::
  :maxdepth: 1
  :glob:

  ./*

If you have feedback on this guide or questions not addressed in any of the topic-specific guides,
`please open an issue. <https://github.com/FireXStuff/firexapp/issues/new>`_
