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

    @app.task(returns=['greeting'], flame=['greeting'])
    def greet(name=getuser()):
        ...

    @app.task(bind=True, returns=['guests_greeting'], flame=['guests_greeting'])
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

`View amplified_greet_guests with custom HTML in Flame. <http://www.firexstuff.com/flame/#/FireX-username-210201-181343-7523/>`_


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

`View Failed amplified_greet_guests with custom HTML in Flame. <http://www.firexstuff.com/flame/#/FireX-username-210201-181444-56719/>`_

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

`View amplified_greet_guests with collapsed tasks in Flame. <http://www.firexstuff.com/flame/#/FireX-username-210201-184333-62229>`__


.. _advanced_dataflow:

Advanced Dataflow: Pass-Through Data From the Invoking Context
==============================================================
`View Example Code <https://github.com/FireXStuff/firexapp/blob/e8ed5b2ba50878377b34481c38c0d5c6fbcb54bb/firexapp/tasks/example.py>`__

In the :ref:`Dataflow via Chaining example <chaining_dataflow>`, we saw how arguments and return values of earlier services in a chain are available to
services later (i.e. downstream) in the chain. In that example, the service that created the chain (``amplified_greet_guests``)
was aware of all arguments needed by the chain, and ensured all required arguments would be available. A common use-case when assembling
complex workflows is that downstream services can receive many arguments, and passing everything downstream explicitly across
multiple layers of services can become
an error-prone maintenance burden. To illustrate how FireX addresses this problem, we'll add some arguments to ``amplify``,
which is downstream from ``amplified_greet_guests``.

.. code-block:: python

    @app.task(returns=['amplified_message'])
    def amplify(to_amplify, upper=True, surround_str=None, underline_char=None, overline_char=None):
        result = to_amplify
        if upper:
            result = to_amplify.upper()
        if surround_str:
            result = surround_str + result + surround_str
        centerline_len = len(result)
        if underline_char:
            result = result + '\n' + (underline_char * centerline_len)

        if overline_char:
            result = (overline_char * centerline_len) + '\n' + result

        return result


Since ``amplified_greet_guests`` wants to make all of ``amplify``'s arguments (such as ``upper``, ``surround_str``,
and so on) available to its callers, it could add every single one to its own definition and pass them along to ``amplify``, like this:

.. code-block:: python

    @app.task(bind=True, returns=['amplified_greeting'])
    def amplified_greet_guests(self: FireXTask, guests, upper=True, surround_str=None, underline_char=None,
                               overline_char=None):
        ...
        amplified_greet_guests_chain = (
            greet_guests.s(guests=guests)
            | amplify.s(
                to_amplify='@guests_greeting'
                upper=upper,
                surround_str=surround_str,
                underline_char=underline_char,
                overline_char=overline_char)
        )
        ...


Note that explicit data passing like this is generally preferable, as it clearly represents where arguments come from
and where they go to. However, even in this purposefully simple situation, it's clear that this can turn in to a maintenance
burden. Consider how much worse things would get if ``amplify`` was instead a service that scheduled other services, and wanted
to make its own downstream parameters available to callers! The ``amplified_greet_guests_chain`` can achieve the same result
by making all of the data it has access to at call-time available to all services in the ``amplified_greet_guests_chain``:

.. code-block:: python

    from firexkit.chain import InjectArgs

    @app.task(bind=True, returns=['amplified_greeting'])
    def amplified_greet_guests(self: FireXTask, guests):
        ...
        amplified_greet_guests_chain = InjectArgs(**self.abog) | greet_guests.s() | amplify.s(to_amplify='@guests_greeting')
        ...

`See diff with the previous code here. <https://github.com/FireXStuff/firexapp/commit/e8ed5b2ba50878377b34481c38c0d5c6fbcb54bb>`__

Note that ``amplified_greet_guests`` has not added any arguments to its ``def``, and no additional arguments are explicitly supplied to ``amplify``.
Instead, the ``InjectArgs`` pseudo-service is used to make data available to the rest of the chain (i.e. both ``greet_guests`` and ``amplify``).
The exact data made
available is from the Bag of Goodies (BoG), accessed via ``self.abog``, which is a Python ``dict`` full of **all data made available to amplified_greet_guests by the calling
context**, even arguments not named in ``def amplified_greet_guests``. If we now execute ``amplified_greet_guests`` with arguments
consumed by ``amplify``, they'll make their way down the chain:

.. code-block:: text

    firexapp submit --chain amplified_greet_guests --guests Li,Dash --underline_char '=' --overline_char '-' --surround_str '***'

`View amplified_greet_guests in Flame. <http://www.firexstuff.com/flame/#/FireX-username-210202-143222-25522>`__

The invoking context in this example is the CLI, so every argument from the CLI is included in the ``self.abog`` of ``amplified_greet_guests``.
Specifically, the BoG enables arguments like ``underline_char`` to be received by ``amplify`` despite not being an explicit argument of
``amplified_greet_guests``.

Be very aware of the trade-offs present when using ``self.abog``. The ``amplified_greet_guests`` service is giving up
explicit data passing and simplicity for flexibility. The service now indicates 'I want the ``amplified_greet_guests_chain``
to have access to all of
the data that I had access to when I was invoked'. This enables callers to influence the ``amplified_greet_guests_chain``,
but makes ``amplified_greet_guests`` more complex and variable.

.. _plugins_example:

Customizing Existing Workflows via Plugins
==========================================
`View Example Code <https://github.com/FireXStuff/firexapp/commit/382d2defe04781223f730feb9c4a55eb47fcacff>`__

When many teams are benefiting from a complex workflow, it's sometimes a single team wants a customization that
is really specific to them. In this case, the workflow owners might be unwilling or unable to provide the desired customization
to the official, public version of the workflow.
This example will show how FireX Plugins can be used to re-use the majority of an existing workflow, but override a specific
service in order to afford arbitrary customization at exclusively a single point in the workflow. As we'll see, this customization
is as simple as writing FireX services to begin with.

Before we demonstrate using a plugin to override existing services, we'll make the running ``greet``/``amplify`` workflow a
bit more involved by creating a new top-level service specifically designed for greeting the employees
of the Springfield Power Plant. This service will reuse the existing ``amplified_greet_guests`` service after looking up
employee titles via the new ``get_springfield_power_plant_job_title`` service:

.. code-block:: python

    @app.task()
    @returns('job_title')
    def get_springfield_power_plant_job_title(name):
        username_to_title = {'Charles Montgomery Burns': 'OWNER',
                             'Waylon Smithers': 'EXECUTIVE ASSISTANT',
                             'Lenny Leonard': 'DIRECTOR',
                             'Homer Simpson': 'SUPERVISOR'}
        return username_to_title.get(name, 'UNKNOWN')


    @app.task(bind=True)
    @returns('amplified_greeting')
    def greet_springfield_power_plant_employees(self, employee_names):
        names_with_titles = []
        for name in employee_names:
            job_title = self.enqueue_child_and_get_results(get_springfield_power_plant_job_title.s(name=name))['job_title']
            names_with_titles.append(f"{job_title} {name}")

        results = self.enqueue_child_and_get_results(amplified_greet_guests.s(guests=names_with_titles))
        return results['amplified_greeting']


Similar to previous examples, we can greet Homer and Smithers by executing:

.. code-block:: text

    firexapp submit --chain greet_springfield_power_plant_employees --employee_names "Homer Simpson,Waylon Smithers"

`View greet_springfield_power_plant_employees in Flame. <http://www.firexstuff.com/flame/#/FireX-username-210202-161714-29033/>`__

Let us say a team called Monarchists comes along and loves the existing ``greet_springfield_power_plant_employees``, but they believe corporate
titles are vastly inferior to titles in a monarchy. The team that owns the original service dislikes monarchies,
and refuses to cooperate. The Monarchists are sensible software engineers and don't want to re-implement the entire workflow,
since they can clearly see they only need to change the results of a service, ``get_springfield_power_plant_job_title``.
Not only will overriding the single service prevent them from needing to maintain the whole workflow, they'll also benefit
from enhancements made to the original workflow.

In a new file, the plugin service can be defined as:

.. code-block:: python
    :caption: springfield_monarchy.py

    @app.task(bind=True, returns=['job_title', FireXTask.DYNAMIC_RETURN])
    def get_springfield_power_plant_job_title(self: FireXTask):
        title_to_monarch = {'OWNER': 'KING',
                            'EXECUTIVE ASSISTANT': 'PRINCE',
                            'DIRECTOR': 'DUKE',
                            'SUPERVISOR': 'CHANCELLOR'}

        # Invoke the original version of the service with all arguments available to to this service: self.abog
        chain = InjectArgs(**self.abog) | self.orig.s()
        orig_ret = self.enqueue_child_and_get_results(chain)

        # Extract the job title from the original results, removing it from the orig_ret dict.
        orig_job_title = orig_ret.pop('job_title')
        # Map the traditional job title to its monarchy equivalent.
        monarchy_job_title = title_to_monarch.get(orig_job_title, 'PEASANT')

        # Return the monarchy title + anything else returned by the original service.
        return monarchy_job_title, orig_ret


In order to be portable and robust in faces of potential addition of arguments and/or return values to the original service it overrides,
this plugin does two things:

 - It passes its entire ``self.abog`` content to the original service ``self.orig``, without even knowing what the input arguments of the original services are. By passing all the arguments down, things will work as-is even if the original services adds a new argument down the road for example.

 - On top of ``job_title``, which is what this service modifies and returns, it also returns ``FireX.DYNAMIC_RETURN``. By doing this, the plugin can also return whatever values the original service is returning, even if that original service adds more return values down the road which the plugin isn't aware of.

You will note that the plugin service is protected against the addition of input arguments and return values, but not the modification or removal of such
values. But by design, services signatures are considered to be public APIs, and as such cannot be changed in a non-backward compatible manner (i.e.: you
can add new arguments with default values and/or add return values, but nor remove/change existing ones) without fixing all dependencies first, so this
only rarely happen.

By specifying the same name as the existing service, ``get_springfield_power_plant_job_title``, this plugin file's service definition
will be called when it is loaded by a ``firexapp`` invocation via the ``--plugins`` argument.

.. code-block:: text

    firexapp submit \
        --chain greet_springfield_power_plant_employees \
        --employee_names "Homer Simpson,Waylon Smithers" \
        --plugins path/to/springfield_monarchy.py

`View greet_springfield_power_plant_employees with overridden get_springfield_power_plant_job_title in Flame. <http://www.firexstuff.com/flame/#/FireX-username-210202-171421-40936/>`__

Take a particularly close look at a overridden ``get_springfield_power_plant_job_title``:

http://www.firexstuff.com/flame/#/FireX-username-210202-171421-40936/tasks/41c3ab03-e4e8-48a6-a582-221ef499e719

Observe that Flame indicates that the service is from a plugin both in the service's name and by the dashed-outline.

With this plugin, the Monarchists team have successfully reused the ``greet_springfield_power_plant_employees`` workflow in its
entirety, while overriding the single service they needed to. You can imagine that some workflows are designed with this reuse/overriding
in mind, when many teams share the vast majority of a workflow, but every team is required to do something specialized
in a specific step (i.e. service) within the workflow.

Keep in mind the plugin is in full control of its relationship with the original service. It could prevent some of its arguments
from being received by the original service, or even not call the original service at all. Further, this example only included
a single overridden service in the plugin file, but it could have also defined another service used by the ``greet_springfield_power_plant_employees``
service, such as ``amplify``, so that a custom version of ``amplify`` would also be used when the plugin is provided.
Since plugins are just alternative definitions of services, they enable extremely flexible alteration of workflows.
