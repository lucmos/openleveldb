============
Installation
============

It is possible to install ``openleveldb`` with poetry_::

    poetry add openleveldb

or with pip::

    pip install openleveldb

.. _poetry: https://python-poetry.org/

Verify installation
===================

Verify that the installation has been successful and that ``plyvel`` correctly installed leveldb,
if it is not already installed on the system::

    python -c 'import openleveldb'

Verify that ``openleveldb`` using the tests

.. code:: bash

    git clone git@github.com:lucmos/openleveldb.git
    cd openleveldb
    poetry run pytest .
