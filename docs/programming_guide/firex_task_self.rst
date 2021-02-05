.. _firex_prog_guide_firex_base_self:

======================================================
FireX Task Class (``self`` within a ``@app.task`` def)
======================================================

.. warning:: This page is a work in progress.

This page describes the functionality exposed via the ``self`` argument within a service definition when
``bind=True`` is set in ``@app.task``.

The most common use of ``self`` is to enqueue (i.e. invoke or schedule) child tasks.
:ref:`Read about enqueuing here. <firex_prog_guide_enqueuing>`

From within a plugin, the original (i.e. the version of the service being overridden) is available via
``self.orig.s(...)``. :ref:`Read about using plugins to override existing services here. <plugins_example>`

