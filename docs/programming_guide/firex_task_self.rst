.. _firex_prog_guide_firex_base_self:

========================================
FireX Task Class (``self`` within a ``@app.task`` def)
========================================

.. warning:: This page is a work in progress.

This page describes the functionality exposed via the ``self`` argument within a microservice definition when
``bind=True`` is set in ``@app.task``.

The most common use of ``self`` is to enqueue (i.e. invoke or schedule) child tasks.
:ref:`Read about enqueuing here. <firex_prog_guide_enqueuing>`
