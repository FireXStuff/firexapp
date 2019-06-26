.. _development:

===========
Development
===========


Creating new tasks
------------------

Creating Automated Tests
------------------------

InputConverters and TaskConverters
----------------------------------

Tracking Services
-----------------

The tracking services mechanism is a means of starting processes in parallel to a FireXApp. This can be useful for
services such as a UI, or separate telemetry processing. Whatever the motivation, the Tracking Services API provides
the tools necessary to launch the services at the stat of the FireX App run.

TrackingService base class
~~~~~~~~~~~~~~~~~~~~~~~~~~

The first steps to integrating an outside process with FireXApp is to provide a concrete implementation of the
TrackingService base class. It consists of two simple parts; A start() method that launches the process, and
an optional extra_cli_arguments() method that allows your service add extra arguments to the FireXApp submit subparser.


setup.py entry point
~~~~~~~~~~~~~~~~~~~~


Report Services
---------------

ReportGenerator base class

