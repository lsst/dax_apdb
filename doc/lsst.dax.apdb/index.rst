.. py:currentmodule:: lsst.dax.apdb

.. _lsst.dax.apdb:

#############
lsst.dax.apdb
#############

.. Paragraph that describes what this Python module does and links to related modules and frameworks.

.. .. _lsst.dax.apdb-using:

.. Using lsst.dax.apdb
.. ===================

.. toctree linking to topics related to using the module's APIs.

.. .. toctree::
..    :maxdepth: 1

.. _lsst.dax.apdb-contributing:

Contributing
============

``lsst.dax.apdb`` is developed at https://github.com/lsst/dax_apdb.
You can find Jira issues for this module under the `dax_apdb <https://jira.lsstcorp.org/issues/?jql=project%20%3D%20DM%20AND%20component%20%3D%20dax_apdb>`_ component.

.. If there are topics related to developing this module (rather than using it), link to this from a toctree placed here.

.. _lsst.dax.apdb-pyapi:

Python API reference
====================

.. automodapi:: lsst.dax.apdb
   :no-main-docstr:
   :no-inheritance-diagram:

.. automodapi:: lsst.dax.apdb.schema_model
   :no-main-docstr:
   :no-inheritance-diagram:


Command line tools
==================

This package provides a command line utility that can be used for some management operations.
The name of the CLI script is ``apdb-cli`` and it has a number of subcommands:

  - ``create-sql`` is used to create new APDB instances based on SQL relational database technology.

  - ``create-cassandra`` is used to create new APDB instances based on Cassandra storage technology.

  - ``list-index`` dumps the contents of the APDB index file.

Each sub-command provides command line help describing its arguments.
