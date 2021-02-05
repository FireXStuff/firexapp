.. _firex_prog_guide_returns:

=============================
Returns
=============================

This page describes the the role played by ``@returns`` and ``@app.task(returns=...)`` within FireX service definitions.

In the FireX world, service return values need to be named. This is required so that they can be added in the ``bog``
of argument/values that flows down a chain.

:ref:`Read about the chain dataflow here. <chaining_dataflow>`

:ref:`See a more involved dataflow example that uses the bog explicitly here. <advanced_dataflow>`

FireX supports two functionally equivalent primitives to specify return value names:
- The ``@returns`` decorator, where you provides a comma separated lost of the names of your return values.
- The ``returns`` argument of the ``@app.task`` decorator, where you provide a Python list with the names of your return value.

When a service performs a `return value1, value2, etc`, the specified return values will be associated with the names
provided using the primitives above, matching the order in which they are defined.

For example:

.. code-block:: python

   @app.task()
   @returns('first_name', 'last_name')
   def first_and_last_name():
     return 'John', 'Doe'


would return and add the following name/value pairs to the ``bog``:

.. code-block:: text

   'first_name` : 'John'
   'last_name`  : 'Doe'


Here's another example taken from an existing service:

.. code-block:: python

    @app.task(returns=['username'])
    def getusername():
        return getuser()

If you run this service by itself from the CLI, you can see it returns "username:youruserid":

.. code-block:: text

   $ firexapp submit --chain getusername --sync
    [13:44:49][HOST-Q54A3] FireX ID: FireX-jdoe-210127-134449-23397
    ...
    Returned values:
      username    jdoe


Returning a dynamic number of values
------------------------------------

FireX supports the notion of returning a variable number of results by providing the ``FireX.DYNAMIC_RETURN`` keyword
in place of an actual return value name. You then have to return a dictionary of name/values in that position of the return values.

This is especially useful in the context of plugins,
where the plugin wants to invoke the original service and return all the same name/values as the original, but without having
to know and hardcode all of these return values.

:ref:`See this detailed example using plugins with FireX.DYNAMIC_RETURN. <plugins_example>`

Handling return value name mismatch
-----------------------------------

When chaining services together, there are cases where a name returned by a service doesn't match the expected input name of downstream services.
To handle a mismatch between the name produced by an upstream service and the name expected by a downstream service,
you can use the special ``@`` prefix to rename a value before supplying it to a service. This is shown in
:ref:`a full example here in the programming guide <chaining_dataflow>`, but we'll take a look at the pertinent code:

.. code-block:: python

    @app.task(bind=True, returns=['guests_greeting'])
    def greet_guests(self, guests):
        ...

    @app.task(returns=['amplified_message'])
    def amplify(to_amplify):
        ...

    @app.task(bind=True, returns=['amplified_greeting'])
    def amplified_greet_guests(self, guests):
        amplified_greet_guests_chain = greet_guests.s(guests=guests) | amplify.s(to_amplify='@guests_greeting')
        ...

The ``amplified_greet_guests`` is creating a chain from two services, ``greet_guests`` and ``amplify``, with the intention
of amplifying the result of ``greet_guests``. However, ``amplify`` expects an input argument named ``to_amplify``,
while ``greet_guests`` produces a return value named ``guests_greeting``. The chain construction uses the special ``@``
prefix to perform this mapping when defining the ``amplify.s(to_amplify='@guests_greeting')`` signature. This ensures
``amplify`` receives the ``guests_greeting`` value as its required ``to_amplify`` argument.


Handling return value name clashes:
-----------------------------------
There are also cases where you would like to take a copy of one of the returned values in the middle of a chain
to avoid it being trampled by one of the downstream service which is using the same return name.

To handle such cases, you can use the ``CopyBogKeys`` service.

Consider the following contrived example where we chain two ``greet`` services one right after another:

.. code-block:: python

    @app.task(returns=['greeting'])
    def greet(name):
        return 'Hello %s!' % name

    @app.task(returns=['lee_greeting', 'tom_greeting'])
    def greet_lee_and_tom():
        chain = greet.s("Lee") | greet.s("Tom")
        results = self.enqueue_child_and_get_results(chain)
        ...

Can we get the greeting for Lee from the ``results``? Unfortunately, no, because ``greet`` always names its return values
``greeting``, causing Lee's ``greeting`` to be trampled by Tom's ``greeting``. We can use
``CopyBogKeys.s({'greeting': 'lee_greeting'})`` between the services to copy the ``greeting`` for Lee into the name ``lee_greeting``:


.. code-block:: python

    @app.task(returns=['lee_greeting', 'tom_greeting'])
    def greet_lee_and_tom():
        chain = greet.s("Lee") | CopyBogKeys.s({'greeting': 'lee_greeting'}) | greet.s("Tom")
        results = self.enqueue_child_and_get_results(chain)
        lee_greeting = results['lee_greeting']
        tom_greeting = results['greeting']
        return lee_greeting, tom_greeting

The ``CopyBogKeys`` service receives a ``dict`` that tells the service to copy values of existing names (e.g. ``greeting``) to
new names (e.g. ``lee_greeting``). This prevents Lee's greeting from being trampled so that the service can return both
greetings. Note that the return value names of ``CopyBogKeys`` are necessarily dynamic (i.e. determined by its input
argument), and ``CopyBogKeys`` therefore must use ``FireX.DYNAMIC_RETURN`` in its service definition.

``CopyBogKeys`` also accepts a `strict=True|False` argument which will specify the behavior if some of the fields
specified in the mapping dictionary are not present in the ``bog``. If set to False (default), the missing fields are
simply skipped over. If set to True, ``CopyBogKeys`` will abort and fail when it encounters a missing field.
