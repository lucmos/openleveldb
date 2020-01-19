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

To verify that the installation as been successful and ``plyvel`` correctly installed leveldb it is possible to run::

    python -c 'import openleveldb'

To verify that ``openleveldb`` behaves as expected it is possible to run the tests

.. code:: bash

    git clone git@github.com:lucmos/openleveldb.git
    cd openleveldb
    poetry run pytest .
