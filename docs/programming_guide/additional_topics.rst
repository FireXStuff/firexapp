.. _firex_prog_guide_additional:

===================================
Additional Topics Programming Guide
===================================

The :ref:`main FireX programming guide <firex_prog_guide>`
covers most day-to-day FireX APIs. The topics addressed in this guide delve deeper in to FireX.
Note that the examples here build on the examples created throughout
the :ref:`main FireX programming guide <firex_prog_guide>`.
It's therefore worth familiarizing yourself with the main guide before continuing here.

.. contents::
 :depth: 2
 :local:


Controlling the Flame UI
========================
FireX services can send data to the Flame UI in order to influence how runs are displayed.

Service-Specific UI Customization in Flame
------------------------------------------
`View Example Code <https://github.com/FireXStuff/firexapp/blob/2f74bb41728150cdef4db2d5664e9b7e482134d3/firexapp/tasks/example.py>`__

The greetings produced by ``greet``, ``greet_guests`` and ``amplified_greet_guests`` are only available
as service results. It's possible to show these greetings within the Flame UI by using the ``flame``
argument of ``@app.task`` to list the argument and returns names to show in Flame

.. code-block:: python

    @app.task(returns='greeting', flame=['greeting'])
    def greet(name=getuser()):
        ...

    @app.task(bind=True, returns='guests_greeting', flame=['guests_greeting'])
    def greet_guests(self: FireXTask, guests):
        ...

For ``amplified_greet_guests``, it's preferable to show the greeting in a large, abrasive font rather than default text.
We can use the ``@flame`` decorator to transform the ``amplified_greeting`` in to HTML that will be rendered by the
Flame UI:

.. code-block:: python

    from firexkit.task import flame

    @app.task(bind=True, returns=['amplified_greeting'])
    @flame('amplified_greeting',
           lambda amplified_greeting: f'<h1 style="font-family: cursive;">{amplified_greeting}</h1>')
    def amplified_greet_guests(self: FireXTask, guests):
        ...

`See diff with the previous code here. <https://github.com/FireXStuff/firexapp/commit/2a50c5d6d0bb013079b0976b3635f345a2073309>`__


The second argument of ``@flame`` is a function that receives the value named by the first argument
(e.g. ``amplified_greeting``) and produces HTML that will be shown within the service box in the UI.

The ``amplified_greet_guests`` service can be executed identically to before:

.. code-block:: text

    firexapp submit --chain amplified_greet_guests --guests Li,Mohamed

`View amplified_greet_guests with custom HTML in Flame. <http://www.firexflame.com/#/FireX-username-210201-181343-7523/>`_


It's also possible to produce HTML based on all service input arguments and, after successful service completion,
the service's return values, by using the special ``*`` key name:

.. code-block:: python

    def _amplified_greeting_formatter(args_and_maybe_results):
        # Since 'amplified_greeting' is the return value name, it isn't available to the formatter when the task is first
        # started. It will be available if the task produces a return value by completing successfully.
        if 'amplified_greeting' in args_and_maybe_results:
            return f'<h1 style="font-family: cursive;">{args_and_maybe_results["amplified_greeting"]}</h1>'

        # Since 'guests' is an input argument, it will always be available to the formatter, even before the service
        # has completed (i.e. succeeded or failed).
        return f'Planning to greet: {",".join(args_and_maybe_results["guests"])}'


    @app.task(bind=True, returns=['amplified_greeting'])
    @flame('*', _amplified_greeting_formatter)
    def amplified_greet_guests(self: FireXTask, guests):
        assert len(guests) > 1, "Only willing to amplify greeting for more than one guest."
        ...


`See diff with the previous code here. <https://github.com/FireXStuff/firexapp/commit/2f74bb41728150cdef4db2d5664e9b7e482134d3>`_

Unlike the previous examples where the ``@flame`` formatter function received a single value (e.g. a single return value),
when ``*`` is supplied to ``@flame``, a python ``dict`` containing all arguments and results (if results are produced) is available from
the formatter function. Invoking ``amplified_greet_guests`` with a single guest causes the service to fail
and only produce the Flame HTML 'Planning to greet...' since the ``amplified_greeting`` result is never produced.

.. code-block:: text

    firexapp submit --chain amplified_greet_guests --guests Li

`View Failed amplified_greet_guests with custom HTML in Flame. <http://www.firexflame.com/#/FireX-username-210201-181444-56719/>`_

Consider using the ``@flame('*', <formatter_function>)`` form when summarizing many inputs and outputs in a single
HTML entry.


Collapse Service Tree Nodes in Flame
------------------------------------
`View Example Code <https://github.com/FireXStuff/firexapp/blob/68c6c263f4ea1c30063f1ff21817fdf40a02d590/firexapp/tasks/example.py>`__

Since the ``amplified_greet_guests`` service includes the greeting from ``greet_guests``, and ``greet_guests`` already
aggregates data from ``greet`` services, it's worthwhile hiding some lower levels in the Flame graph. We can have
``amplified_greet_guests`` specify to collapse the descendants of ``greet_guests`` by using the
``@flame_collapse`` decorator to reduce clutter.

.. code-block:: python

    from firexkit.task import flame_collapse

    @app.task(...)
    @flame(...)
    @flame_collapse({'greet_guests': 'descendants'})
    def amplified_greet_guests(...):
        ...

`See diff with the previous code here. <https://github.com/FireXStuff/firexapp/commit/68c6c263f4ea1c30063f1ff21817fdf40a02d590>`__

Note that when services other than ``amplified_greet_guests`` enqueue ``greet_guests``, the collapse rule specified
above will not be applied.

The ``amplified_greet_guests`` service can be executed identically to before:

.. code-block:: text

    firexapp submit --chain amplified_greet_guests --guests Li,Mohamed

`View amplified_greet_guests with collapsed tasks in Flame. <http://www.firexflame.com/#/FireX-username-210201-184333-62229>`__

..
    TODO: create advanced_dataflow example without referenceing internal-cisco services.
    .. _advanced_dataflow:
