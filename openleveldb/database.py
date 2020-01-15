from pathlib import Path
from time import sleep
from typing import Any, Iterable, Iterator, Optional, Union

import plyvel
from openleveldb.client import LevelDBClient
from openleveldb.dbconnector import LevelDBConnector
from plyvel._plyvel import PrefixedDB
from tqdm import tqdm


class LevelDB:
    def __init__(
        self, db_path: Optional[Union[str, Path]], server_address: Optional[str] = None,
    ) -> None:
        if server_address is not None:
            self.db = LevelDBClient.get_instance(db_path, server_address)
        else:
            self.db = LevelDBConnector.get_instance(db_path=db_path)

    def custom_iterator(
        self, prefixes: bytes = None, starting_by: Optional[str] = None, **kwargs
    ) -> Iterable:
        """
        Builds a custom iterator exploiting the parameters available in plyvel.DB

        :param prefixes: the prefix or the iterable of prefixes to apply
        :param starting_by: start the iteration from the specified prefix
        :param kwargs: additional arguments of plyvel.DB.iterator()
        :returns: the custom iterable
        """
        return self.db.custom_iterator(
            prefixes=prefixes, starting_by=starting_by, **kwargs
        )

    def __iter__(self) -> Iterator:
        """
        Default iterator over (key, value) couples

        :returns: the iterator
        """
        return iter(self.db)

    def __len__(self) -> int:
        """
        Computes the number of element in the database.
        It may be very slow, use with caution.

        :returns: number of elements in the database
        """
        # todo: fix the dblen of prefixed db!
        return len(self.db)

    def __setitem__(self, key: Union[str, Iterable[str]], value: Any) -> None:
        """
        Store the couple (key, value) in leveldb.
        The key and the value are automatically encoded using the encoders registered
        in the serializer.

        The key may be a single string or an iterable of strings, to specify prefixes.
        The last element is always the key.

            >>> ldb = LevelDB.get_instance(tmpfile)
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
        self.db[key] = value

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

            >>> ldb = LevelDB.get_instance(tmpfile)
            >>> ldb['key'] = 'value_string1'
            >>> ldb['key']
            'value_string1'
            >>> ldb['prefix1', 'prefix2', 'prefix2', 'key'] = 'value_string2'
            >>> ldb['prefix1', 'prefix2', 'prefix2', 'key']
            'value_string2'
            >>> del ldb['key'], ldb['prefix1', 'prefix2', 'prefix2', 'key'], ldb['key']

        It is possible to retrieve prefixedDB specifying prefixes and using Ellipsis
        as the actual key:

            >>> a = LevelDB.get_instance(tmpfile)
            >>> isinstance(ldb.db, plyvel.DB)
            True
            >>> isinstance(ldb['prefix', ...].db, PrefixedDB)
            True
            >>> isinstance(ldb, LevelDB) and isinstance(ldb['prefix', ...], LevelDB)
            True

        :param key: string or iterable of string to specify prefixes, auto-encoded
                    by the serializer. It's possible to specify sub-db with Ellipsis.
        :returns: the value associated to the key in leveldb, decoded by the serializer.
        """
        return self.db[key]

    def __delitem__(self, key: Union[str, Iterable[str]]) -> None:
        """
        Delete the couple (key, value) from leveldb.
        The key is automatically encoded using the encoders registered in the serializer.

        The key may be a single string or an iterable of strings, to specify prefixes.
        The last element is always the key.

            >>> ldb = LevelDB.get_instance(tmpfile)
            >>> ldb['key'] = 'value_string1'
            >>> ldb['prefix1', 'prefix2', 'prefix2', 'key'] = 'value_string2'
            >>> del ldb['key'], ldb['prefix1', 'prefix2', 'prefix2', 'key'], ldb['key']
            >>> dblen(ldb)
            0

        :param key: string or iterable of string to specify prefixes, auto-encoded by the serializer.
        """
        del self.db[key]

    def __repr__(self) -> str:
        return repr(self.db)


if __name__ == "__main__":
    import doctest
    import tempfile
    import numpy as np

    # tmpfile = tempfile.mkdtemp()
    # doctest.testmod(extraglobs={"tmpfile": tmpfile})

    def test_db(db):
        nu = 20
        for x in tqdm(range(nu), desc="writing"):
            key = f"{x}"
            db[key] = np.random.rand(3, 1000)

        for x in tqdm(range(nu), desc="reading"):
            key = f"{x}"
            v = db[key]

        try:
            for x in tqdm(db, desc="iterating"):
                pass
        except NotImplementedError:
            print(f"Error: {NotImplementedError}")
        print()

    print("Testing local LevelDB")
    db = LevelDB(db_path="azz")
    test_db(db)

    sleep(0.25)

    print("Testing REST LevelDB")
    db = LevelDB(db_path="azz", server_address="http://127.0.0.1:5000")
    test_db(db)
