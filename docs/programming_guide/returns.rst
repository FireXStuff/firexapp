.. _firex_prog_guide_returns:

=============================
Returns
=============================

This page describes the the role played by ``@returns`` and ``@app.task(returns=...)`` within FireX service definitions.

In the FireX world, service return values need to be named. This is required so that they can be added in the ``bog`` of argument/values that flows down a chain.

:ref:`Read about the chain dataflow here. <chaining_dataflow>`

:ref:`See a more involved dataflow example that uses the bog here. <advanced_dataflow>`


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

..
  TODO: Port  Handling return values name mismatch or clashes:
