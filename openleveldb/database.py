"""
Provides a pythonic dict-like interface to local or remote leveldb.
In remote mode it allows access from multiple processes.
"""
import io
from pathlib import Path
from typing import Any, Iterable, Iterator, Optional, Union

from openleveldb.backend.connectorclient import LevelDBClient
from openleveldb.backend.connectorlocal import LevelDBLocal


class LevelDB:
    def __init__(
        self,
        db_path: Optional[Union[str, Path]],
        server_address: Optional[str] = None,
        dbconnector: Optional[Union[LevelDBLocal, LevelDBClient]] = None,
        read_only: bool = False,
    ) -> None:
        """
        Provide access to a leveldb database, it if does not exists one is created.

        Local databases do not support multiprocessing.
        It is possible to access a local db with:

            >>> db = LevelDB(db_path)

        Remote databases support multiprocessing. Once the a leveldb server is running,
        it is possible to access the remote db with:

            >>> # db = LevelDB(remote_db_path, server_address)

        :param db_path: the path in the filesystem to the database
        :param server_address: the address of the remote server
        :param read_only: if true the db can not be modified
        :param dbconnector: provide directly an existing dbconnector
        """
        self.db_path = db_path
        self.server_address = server_address
        self.dbconnector: Union[LevelDBClient, LevelDBLocal]
        self.read_only: bool = read_only

        if dbconnector is not None:
            self.dbconnector = dbconnector
        elif server_address is not None:
            self.dbconnector = LevelDBClient.get_instance(db_path, server_address)
        else:
            self.dbconnector = LevelDBLocal.get_instance(db_path=db_path)

    def prefixed_iter(
        self,
        prefixes: Optional[Union[str, Iterable[str]]] = None,
        starting_by: Optional[Union[str, Iterable[str]]] = None,
        include_key=True,
        include_value=True,
    ) -> Iterable:
        """
        Builds a custom iterator.

        The parameters ``include_key`` and  ``include_value`` define what should be
        yielded:

            >>> list(db)
            [('a1', 'value1'), ('b1', 'value2'), ('b2', 'value3'), ('c1', 'value4')]
            >>> list(db.prefixed_iter(include_key=False, include_value=False))
            [None, None, None, None]
            >>> list(db.prefixed_iter(include_key=True, include_value=False))
            ['a1', 'b1', 'b2', 'c1']
            >>> list(db.prefixed_iter(include_key=False, include_value=True))
            ['value1', 'value2', 'value3', 'value4']


        The ``prefixes`` and ``starting_by`` parameters have a similar meaning. They
        determine over which keys it should iterate. The difference is that
        ``starting by`` preserves the prefix in the returned key.
        The iterations stops when all the available keys with the given prefixes have
        been yielded

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

        :param include_key: if False do not yield the keys
        :param include_value: if False do not yield the values

        :param prefixes: prefixes of the desired keys
            The prefixes  will  be removed from the keys returned

        :param starting_by: prefixes of the desired keys
            The prefixes  will  be preserved from the keys returned

        :return: the iterable over the keys and/or values
        """
        return self.dbconnector.prefixed_iter(
            prefixes=prefixes,
            starting_by=starting_by,
            include_key=include_key,
            include_value=include_value,
        )

    def prefixed_len(
        self,
        prefixes: Optional[Union[str, Iterable[str]]] = None,
        starting_by: Optional[str] = None,
    ) -> int:
        """
        Utility function to compute the number of keys with a given prefix,
        see :py:meth:~database.LevelDB.prefixed_iter for more details.

            >>> list(db)
            [('a1', 'value1'), ('b1', 'value2'), ('b2', 'value3'), ('c1', 'value4')]
            >>> db.prefixed_len(prefixes=["b"])
            2
            >>> db.prefixed_len(prefixes=["b", "1"])
            1

        :param prefixes: prefixes of the desired keys
            The prefixes  will  be removed from the keys returned

        :param starting_by: prefixes of the desired keys
            The prefixes  will  be preserved from the keys returned

        :return: the number of matching keys
        """
        return self.dbconnector.prefixed_len(
            prefixes=prefixes, starting_by=starting_by,
        )

    def __iter__(self) -> Iterator:
        """
        Iterator over (key, value) sorted by key

            >>> list(db)
            [('a1', 'value1'), ('b1', 'value2'), ('b2', 'value3'), ('c1', 'value4')]

        :returns: the iterator over the items
        """
        return iter(self.dbconnector)

    def __len__(self) -> int:
        """
        Computes the number of element in the database.

            >>> list(db)
            [('a1', 'value1'), ('b1', 'value2'), ('b2', 'value3'), ('c1', 'value4')]
            >>> len(db)
            4

        :returns: number of elements in the database
        """
        return len(self.dbconnector)

    def __setitem__(self, key: Union[str, Iterable[str]], value: Any) -> None:
        """
        Store the couple (key, value) in leveldb.
        The key and the value are automatically encoded together with the obj
        type information, in order to be able to automate the decoding.

            >>> import numpy as np
            >>> db["array"] = np.array([1, 2, 3], dtype=np.int8)
            >>> db["array"]
            array([1, 2, 3], dtype=int8)
            >>> del db["array"]


        The key may be one or more strings to specify prefixes.
        The last element is always the key:

            >>> db["prefix1", "prefix2", "key"] = "myvalue"
            >>> db["prefix1", "prefix2", "key"]
            'myvalue'
            >>> del db["prefix1", "prefix2", "key"]

        :param key: one or more strings to specify prefixes
        :returns: the value associated to the key in leveldb
        """
        if self.read_only:
            raise io.UnsupportedOperation("not writable")
        self.dbconnector[key] = value

    def __getitem__(
        self, key: Union[str, Iterable[Union[str, Ellipsis.__class__]]]
    ) -> Any:
        """
        Retrieve the couple (key, value) from leveldb.

        >>> db["a1"]
        'value1'
        >>> db["a", "1"]
        'value1'

        The value is automatically decoded into its original type. The value must
        have been stored with :py:~:py:meth:~database.LevelDB.__setitem__

            >>> import numpy as np
            >>> db["array"] = np.array([1, 2, 3], dtype=np.int8)
            >>> db["array"]
            array([1, 2, 3], dtype=int8)
            >>> del db["array"]

        The key may be one or more strings, to specify prefixes.
        The last element is always the key:

            >>> db["prefix1", "prefix2", "key"] = "myvalue"
            >>> db["prefix1", "prefix2", "key"]
            'myvalue'
            >>> del db["prefix1", "prefix2", "key"]

        It is possible to retrieve a stateful instance of :py:class:~database.LevelDB
        that accounts for prefixes using Ellipsis as key:

            >>> list(db)
            [('a1', 'value1'), ('b1', 'value2'), ('b2', 'value3'), ('c1', 'value4')]
            >>> db_b = db['b', ...]
            >>> db_b["1"]
            'value2'
            >>> list(db_b)
            [('1', 'value2'), ('2', 'value3')]
            >>> list(db["c", ...])
            [('1', 'value4')]

        :param key: one or more strings to specify prefixes. It's possible to specify
            sub-db using the Ellipsis as key.
        :returns: the value associated to the key in leveldb or a sub-db.
        """
        out = self.dbconnector[key]
        return (
            out
            if not isinstance(out, type(self.dbconnector))
            else LevelDB(
                db_path=self.db_path,
                server_address=self.server_address,
                dbconnector=out,
                read_only=self.read_only,
            )
        )

    def __delitem__(self, key: Union[str, Iterable[str]]) -> None:
        """
        Delete the couple (key, value) from leveldb.

        The key may be one or more strings, to specify prefixes.
        The last element is always the key:


            >>> db["prefix", "key"] = "value"
            >>> db["prefix", "key"]
            'value'
            >>> db["prefixkey"]
            'value'
            >>> del db["prefix", "key"]
            >>> print(db["prefix", "key"])
            None
            >>> print(db["prefixkey"])
            None

        :param key: one or more strings to specify prefixes.
        """
        if self.read_only:
            raise io.UnsupportedOperation("not writable")
        del self.dbconnector[key]

    def __repr__(self) -> str:
        name = self.__class__.__name__
        path = self.db_path
        ip = f"'{self.server_address}'" if self.server_address is not None else None
        return (
            f"{name}(db_path='{path}', "
            f"server_address={ip}, "
            f"dbconnector={self.dbconnector})"
        )

    def close(self) -> None:
        """
        Close the database
        """
        self.dbconnector.close()


if __name__ == "__main__":
    import doctest
    import tempfile

    import numpy as np

    db_path = tempfile.mkdtemp()
    db = LevelDB(db_path=db_path)

    db["a", "1"] = "value1"
    db["b", "1"] = "value2"
    db["b", "2"] = "value3"
    db["c", "1"] = "value4"

    doctest.testmod(extraglobs={"tmpfile": db_path, "db": db})
