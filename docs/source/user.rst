===============
Getting started
===============

Open db connection
==================

The connection to the db can be direct or pass through a REST server.
The only change required in the code is how the ``LevelDB`` object is instantiated

Direct connection
-----------------

The first thing to do is to instantiate a ``LevelDB`` object to open
a connection to leveldb database::

    from openleveldb import LevelDB
    db = LevelDB(db_path="path_to_db")


REST connection
---------------

If it's required to have multiprocessing support,
that is not provided by leveldb, it is possible to start a
server and connect to the database through REST API.
In order to start the server is enough to do:

.. code:: bash

    cd openleveldb
    make server

Then it's possible to instantiate a ``LevelDB`` object specifying the server::

    from openleveldb import LevelDB
    db = LevelDB(db_path="path_to_db", server_address="http://127.0.0.1:5000")


Basic access
============

Storing, reading and deleting an element follows the dict protocol:

    >>> db["prefix", "key"] = np.array([1, 2, 3], dtype=np.int8)
    >>> db["prefix", "key"]
    array([1, 2, 3], dtype=int8)
    >>> del db["prefix", "key"]


It is possible to use an arbitrary number of prefixes:

    >>> db["prefix1", "prefix2", "key"] = np.array([1, 2, 3], dtype=np.int8)
    >>> db["prefix1", "prefix2", "key"]
    array([1, 2, 3], dtype=int8)
    >>> del db["prefix1", "prefix2", "key"]


Iteration
=========

Iteration over ``(key, value)`` pairs behaves accordingly:

    >>> list(db)
    [('a1', 'value1'), ('b1', 'value2'), ('b2', 'value3'), ('c1', 'value4')]

It's possible to perform advanced form of iteration using
the ``LevelDB.prefixed_iter`` function:

    >>> list(db)
    [('a1', 'value1'), ('b1', 'value2'), ('b2', 'value3'), ('c1', 'value4')]
    >>> list(db.prefixed_iter(prefixes=["b"]))
    [('1', 'value2'), ('2', 'value3')]
    >>> list(db.prefixed_iter(prefixes=["b", "1"]))
    [('', 'value2')]
    >>> list(db.prefixed_iter(starting_by="b"))
    [('b1', 'value2'), ('b2', 'value3')]
    >>> list(db.prefixed_iter(starting_by=["b", "1"]))
    [('b1', 'value2')]


Fancy indexing
==============

When a local connection is available,
it is possible to use fancy indexing to obtain a stateful ``LevelDB``
that remembers the prefixes:

    >>> list(db)
    [('a1', 'value1'), ('b1', 'value2'), ('b2', 'value3'), ('c1', 'value4')]
    >>> db_b = db['b', ...]
    >>> db_b["1"]
    'value2'
    >>> list(db_b)
    [('1', 'value2'), ('2', 'value3')]
    >>> list(db["c", ...])
    [('1', 'value4')]


