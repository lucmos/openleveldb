import atexit
import os
import sys
from multiprocessing import Process
from pathlib import Path
from time import sleep
from typing import Any, Iterable, Iterator, Optional, Union

import plyvel
from openleveldb.config import get_env, load_envs
from openleveldb.connectorclient import LevelDBClient
from openleveldb.connectorlocal import LevelDBLocal
from openleveldb.server import dummy_server
from tqdm import tqdm


class LevelDB:
    def __init__(
        self,
        db_path: Optional[Union[str, Path]],
        server_address: Optional[str] = None,
        dbconnector: Optional[Union[LevelDBLocal, LevelDBClient]] = None,
    ) -> None:
        load_envs()
        self.db_path = db_path
        self.server_address = server_address
        self.dbconnector: Union[LevelDBClient, LevelDBLocal]

        if dbconnector is not None:
            self.dbconnector = dbconnector
        elif server_address is not None:
            self.dbconnector = LevelDBClient.get_instance(db_path, server_address)
        else:
            self.dbconnector = LevelDBLocal.get_instance(db_path=db_path)

    def __eq__(self, other) -> bool:
        if not isinstance(other, LevelDB):
            return False
        return (
            self.db_path == other.db_path
            and self.server_address == other.server_address
        )

    def __hash__(self) -> int:
        return hash(self.db_path) ^ hash(self.server_address)

    def prefixed_iter(
        self,
        prefixes: Optional[Union[str, Iterable[str]]] = None,
        starting_by: Optional[Union[str, Iterable[str]]] = None,
        include_key=True,
        include_value=True,
    ) -> Iterable:
        """
        Builds a custom iterator exploiting the parameters available in plyvel.DB

        :param prefixes: the prefix or the iterable of prefixes to apply
        :param starting_by: start the iteration from the specified prefix
        :param kwargs: additional arguments of plyvel.DB.iterator()
        :returns: the custom iterable
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
        **kwargs,
    ) -> int:
        return self.dbconnector.prefixed_len(
            prefixes=prefixes, starting_by=starting_by, **kwargs
        )

    def __iter__(self) -> Iterator:
        """
        Default iterator over (key, value) couples

        :returns: the iterator
        """
        return iter(self.dbconnector)

    def __len__(self) -> int:
        """
        Computes the number of element in the database.
        It may be very slow, use with caution.

        :returns: number of elements in the database
        """
        # todo: fix the dblen of prefixed db!
        return len(self.dbconnector)

    def __setitem__(self, key: Union[str, Iterable[str]], value: Any) -> None:
        """
        Store the couple (key, value) in leveldb.
        The key and the value are automatically encoded using the encoders registered
        in the serializer.

        The key may be a single string or an iterable of strings, to specify prefixes.
        The last element is always the key.

            >>> ldb = LevelDB(tmpfile)
            >>> ldb['key'] = 'value_string1'
            >>> ldb['key']
            'value_string1'
            >>> ldb['prefix1', 'prefix2', 'prefix2', 'key'] = 'value_string2'
            >>> ldb['prefix1', 'prefix2', 'prefix2', 'key']
            'value_string2'
            >>> del ldb['key'], ldb['prefix1', 'prefix2', 'prefix2', 'key'], ldb['key']

        :param key: string or iterable of string to specify prefixes, encoded
                    by the serializer.
        :returns: the value associated to the key in leveldb, decoded by the serializer.
        """
        self.dbconnector[key] = value

    def __getitem__(
        self, key: Union[str, Iterable[Union[str, Ellipsis.__class__]]]
    ) -> Any:
        """
        Retrieve the couple (key, value) from leveldb.
        The key is automatically encoded using the encoders registered in the
        serializer, and the value is automatically decoded using the decoders
        registered in the serializer.

        The key may be a single string or an iterable of strings, to specify prefixes.
        The last element is always the key.

            >>> ldb = LevelDB(tmpfile)
            >>> ldb['key'] = 'value_string1'
            >>> ldb['key']
            'value_string1'
            >>> ldb['prefix1', 'prefix2', 'prefix2', 'key'] = 'value_string2'
            >>> ldb['prefix1', 'prefix2', 'prefix2', 'key']
            'value_string2'
            >>> del ldb['key'], ldb['prefix1', 'prefix2', 'prefix2', 'key'], ldb['key']

        It is possible to retrieve prefixedDB specifying prefixes and using Ellipsis
        as the actual key:

            >>> a = LevelDB(tmpfile)
            >>> isinstance(ldb.db, plyvel.DB)
            True
            >>> isinstance(ldb, LevelDB) and isinstance(ldb['prefix', ...], LevelDB)
            True

        :param key: string or iterable of string to specify prefixes, auto-encoded
                    by the serializer. It's possible to specify sub-db with Ellipsis.
        :returns: the value associated to the key in leveldb, decoded by the serializer.
        """
        out = self.dbconnector[key]
        return (
            out
            if not isinstance(out, type(self.dbconnector))
            else LevelDB(
                db_path=self.db_path,
                server_address=self.server_address,
                dbconnector=out,
            )
        )

    def __delitem__(self, key: Union[str, Iterable[str]]) -> None:
        """
        Delete the couple (key, value) from leveldb.
        The key is automatically encoded using the encoders registered in the serializer.

        The key may be a single string or an iterable of strings, to specify prefixes.
        The last element is always the key.

            >>> ldb = LevelDB(tmpfile)
            >>> ldb['key'] = 'value_string1'
            >>> ldb['prefix1', 'prefix2', 'prefix2', 'key'] = 'value_string2'
            >>> del ldb['key'], ldb['prefix1', 'prefix2', 'prefix2', 'key'], ldb['key']
            >>> dblen(ldb)
            0

        :param key: string or iterable of string to specify prefixes, auto-encoded by the serializer.
        """
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
        self.dbconnector.close()
        if hasattr(self, "bg_server"):
            self.bg_server.terminate()


if __name__ == "__main__":
    import doctest
    import tempfile

    import numpy as np

    # tmpfile = tempfile.mkdtemp()
    # doctest.testmod(extraglobs={"tmpfile": tmpfile})

    def test_db(db):
        nu = 10
        for x in tqdm(range(nu), desc="writing"):
            key = f"{x}"
            db[key] = np.random.rand(3, 10)

        for x in tqdm(range(nu), desc="reading"):
            key = f"{x}"
            v = db[key]

        for x in tqdm(db, desc="iterating"):
            pass

        for x in tqdm(range(nu), desc="deleting"):
            key = f"{x}"
            del db[key]

        print(f"len(db) ={len(db)}", file=sys.stderr)
        print("", file=sys.stderr)

    db_path = "/home/luca/Scrivania/azz"

    print("Testing local LevelDB", file=sys.stderr)
    db = LevelDB(db_path=db_path)
    test_db(db)
    db.close()
    sleep(0.25)

    print("Testing REST LevelDB", file=sys.stderr)
    db = LevelDB(db_path=db_path, server_address="http://127.0.0.1:5000")
    test_db(db)
    db.close()
    sleep(0.25)
