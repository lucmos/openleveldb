============================
A pythonic leveldb wrapper
============================
|pypi| |docs| |license| |black|

.. inclusion-marker-do-not-remove


`Openleveldb <https://openleveldb.readthedocs.io/en/latest/index.html#>`_ is a small pythonic wrapper around Plyvel_


Features
========


Transparent object store
------------------------

It works with python objects:

- Automatically **encodes objects** into bytes when saving to leveldb
- Automatically **decodes bytes** into their original type when retrieving objects from leveldb

Supported types include:

- ``int``
- ``str``
- ``numpy.ndarray``
- Anything that is serializable by orjson_

>>> db['key'] = {'key': [1, 2, 3]}
>>> db['key']
{'key': [1, 2, 3]}


Python dict-like protocol
-------------------------

It offers dict-like interface to LevelDB_


>>> db["prefix", "key"] = np.array([1, 2, 3], dtype=np.int8)
>>> db["prefix", "key"]
array([1, 2, 3], dtype=int8)

>>> db = db["prefix", ...]
>>> db["key"]
array([1, 2, 3], dtype=int8)



String-only keys
----------------

The only possible type for the keys is ``str``.
It avoids several problems when working with prefixes.



Multiprocessing support
-----------------------

Experimental **multiprocessing** support using a background flask server,
exposing the same API of a direct connection::

    db = LevelDB(db_path="path_to_db", server_address="http://127.0.0.1:5000")


.. _Plyvel: https://github.com/wbolster/plyvel
.. _LevelDB: http://code.google.com/p/leveldb/
.. _orjson: https://github.com/ijl/orjson


.. |docs| image:: https://readthedocs.org/projects/openleveldb/badge/?version=latest
    :target: https://openleveldb.readthedocs.io/en/latest/?badge=latest
    :alt: Documentation Status

.. |license| image:: https://img.shields.io/github/license/lucmos/openleveldb
    :target: https://github.com/lucmos/openleveldb/blob/master/LICENSE
    :alt: Openleveldb license
    
.. |pypi| image:: https://img.shields.io/pypi/v/openleveldb
    :target: https://pypi.org/project/openleveldb/
    :alt: Openleveldb repo

.. |black| image:: https://img.shields.io/badge/code%20style-black-000000.svg
    :target: https://github.com/psf/black
    :alt: Black syntax
