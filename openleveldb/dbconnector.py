"""
Class that wraps the LevelDB plyvel to support dict-like behaviour, with automatic serialization to bytes and
deserialization from bytes provided by the serialization module


TEST BASE KEYSTORE CAPABILITIES


Test base retrieval:

    >>> ldb = LevelDBConnector.get_instance(tmpfile)
    >>> ldb['key'] = 'value_string1'
    >>> ldb['key']
    'value_string1'
    >>> del ldb['key']
    >>> len(ldb)
    0
    >>> ldb['prefix1', 'prefix2', 'prefix2', 'key'] = 'value_string2'
    >>> ldb['prefix1', 'prefix2', 'prefix2', 'key']
    'value_string2'
    >>> del ldb['prefix1', 'prefix2', 'prefix2', 'key']
    >>> len(ldb)
    0


Test json serialization and missing identifier detection:

    >>> ldb = LevelDBConnector.get_instance(tmpfile)
    >>> a = {'key1': 10.5, 'key2': [1, 2, {'key3': -1}]}
    >>> ldb['key'] = a
    >>> ldb['key']
    {'key1': 10.5, 'key2': [1, 2, {'key3': -1}]}
    >>> a == ldb['key']
    True
    >>> import orjson
    >>> ldb['key'] = orjson.dumps(a)
    >>> ldb['key']
    Traceback (most recent call last):
     ...
    openleveldb.serializer.DecodeError: missing DecodeType identifier in bytes blob
    >>> ldb['key'] = serializer.encode(a)
    >>> ldb['key']
    {'key1': 10.5, 'key2': [1, 2, {'key3': -1}]}
    >>> del ldb['key']
    >>> len(ldb)
    0


Test NumPy serialization and missing identifier detection:

    >>> ldb = LevelDBConnector.get_instance(tmpfile)
    >>> a = np.array([[1, 2, 3, 4, 5], [6, 7, 8, 9, 10]], dtype=np.float16)
    >>> ldb['key'] = a
    >>> ldb['key']
    array([[ 1.,  2.,  3.,  4.,  5.],
           [ 6.,  7.,  8.,  9., 10.]], dtype=float16)
    >>> np.array_equal(a, ldb['key'])
    True
    >>> from openleveldb.serializer import Serializer
    >>> ldb['key'] = Serializer.ndarray_tobytes(np.array([1, 2, 3]))
    >>> ldb['key']
    Traceback (most recent call last):
     ...
    openleveldb.serializer.DecodeError: missing DecodeType identifier in bytes blob
    >>> ldb['key'] = serializer.encode(np.array([1, 2, 3]))
    >>> ldb['key']
    array([1, 2, 3])
    >>> del ldb['key']
    >>> len(ldb)
    0


Test int serialization and missing identifier detection:

    >>> ldb = LevelDBConnector.get_instance(tmpfile)
    >>> a = 42
    >>> ldb['key'] = a
    >>> ldb['key']
    42
    >>> a == ldb['key']
    True
    >>> a = -10000
    >>> ldb['key'] = a
    >>> ldb['key']
    -10000
    >>> a == ldb['key']
    True
    >>> ldb['key'] = int.to_bytes(42, length=16, byteorder='big', signed=True)
    >>> ldb['key']
    Traceback (most recent call last):
     ...
    openleveldb.serializer.DecodeError: missing DecodeType identifier in bytes blob
    >>> ldb['key'] = serializer.encode(42)
    >>> ldb['key']
    42
    >>> ldb['key'] = int.to_bytes(-10000000000000000000000000000000000000, length=16, byteorder='big', signed=True)
    >>> ldb['key']
    Traceback (most recent call last):
     ...
    openleveldb.serializer.DecodeError: missing DecodeType identifier in bytes blob
    >>> ldb['key'] = serializer.encode(-10000000000000000000000000000000000000)
    >>> ldb['key']
    -10000000000000000000000000000000000000
    >>> del ldb['key']
    >>> len(ldb)
    0


Test string serialization and missing identifier detection:

    >>> ldb = LevelDBConnector.get_instance(tmpfile)
    >>> a = "just testing some conversions... :)"
    >>> ldb['key'] = a
    >>> ldb['key']
    'just testing some conversions... :)'
    >>> a == ldb['key']
    True
    >>> ldb['key'] = b'value_bytes1'
    >>> ldb['key']
    Traceback (most recent call last):
    openleveldb.serializer.DecodeError: missing DecodeType identifier in bytes blob
    >>> ldb['key'] = serializer.encode('value_bytes1')
    >>> ldb['key']
    'value_bytes1'
    >>> del ldb['key']
    >>> len(ldb)
    0


Test prefixes internals:

    >>> ldb = LevelDBConnector.get_instance(tmpfile)
    >>> ldb['prefix1', 'prefix2', 'prefix2', 'key_string2'] = 'value_string2'
    >>> ldb['prefix1prefix2prefix2key_string2'] == ldb['prefix1', 'prefix2', 'prefix2', 'key_string2']
    True
    >>> ldb['prefix1', 'prefix2', 'prefix2', 'key_string2']
    'value_string2'
    >>> ldb['prefix1prefix2prefix2key_string2'] = 'value_string3'
    >>> ldb['prefix1prefix2prefix2key_string2'] == ldb['prefix1', 'prefix2', 'prefix2', 'key_string2']
    True
    >>> ldb['prefix1', 'prefix2', 'prefix2', 'key_string2']
    'value_string3'
    >>> [(x, y) for x, y in ldb]
    [('prefix1prefix2prefix2key_string2', 'value_string3')]
    >>> del ldb['prefix1', 'prefix2', 'prefix2', 'key_string2']
    >>> len(ldb)
    0


Test that key blobs that don't have an identifier throw an error (i.e. they do not get decoded implicitly):

    >>> ldb = LevelDBConnector.get_instance(tmpfile)
    >>> ldb[b'key_bytes1'] = 'value_bytes1'
    Traceback (most recent call last):
    ...
    TypeError: str prefix or key expected, got bytes
    >>> ldb['key', b'key_bytes1'] = 'value_bytes1'
    Traceback (most recent call last):
    ...
    TypeError: str prefix or key expected, got bytes
    >>> ldb[b'key_bytes1', 'key'] = 'value_bytes1'
    Traceback (most recent call last):
    ...
    TypeError: str prefix or key expected, got bytes

    
Test singleton pattern:

    >>> a = LevelDBConnector(tmpfile)
    Traceback (most recent call last):
    RuntimeError: A LevelDB instance for the same db already exists.
    >>> a = LevelDBConnector.get_instance(tmpfile)
    >>> tmpfile2 = tempfile.mkdtemp()
    >>> b = LevelDBConnector(tmpfile2)
    >>> b = LevelDBConnector(tmpfile2)
    Traceback (most recent call last):
    RuntimeError: A LevelDB instance for the same db already exists.


Test database retrieval:

    >>> a = LevelDBConnector.get_instance(tmpfile)

    >>> isinstance(ldb, LevelDBConnector)
    True
    >>> isinstance(ldb.db, plyvel.DB)
    True
    >>> isinstance(ldb['prefix', ...], LevelDBConnector)
    True
    >>> isinstance(ldb['prefix', ...].db, PrefixedDB)
    True

"""
from pathlib import Path
from typing import Any, Callable, Iterable, Iterator, Optional, Type, Union

import numpy as np
import plyvel
from openleveldb import serializer
from openleveldb.serializer import DecodeError, normalize_strings
from openleveldb.utils import get_prefixed_db
from plyvel._plyvel import PrefixedDB


class LevelDBConnector:
    """
    LevelDB connector

    Sample usage:

        >>> ldb = LevelDBConnector.get_instance(tmpfile)
        >>> ldb['key'] = 'value_string1'
        >>> ldb['key']
        'value_string1'
        >>> del ldb['key']
        >>> len(ldb)
        0
    """

    _instances = {}

    @staticmethod
    def get_instance(db_path: Union[str, Path]) -> "LevelDBConnector":
        """
        Return a singleton instance of LevelDB for each path

        :param db_path: the path to the LevelDB database
        :returns: the LevelDB object
        """
        db_path = Path(db_path)
        if db_path not in LevelDBConnector._instances:
            LevelDBConnector._instances[db_path] = LevelDBConnector(db_path)
        return LevelDBConnector._instances[db_path]

    def __init__(
        self,
        db_path: Optional[Union[str, Path]],
        db: plyvel.DB = None,
        key_encoder: Callable[[str], bytes] = serializer.DecodeType.STR.pure_encode_fun,
        key_decoder: Callable[[bytes], str] = serializer.DecodeType.STR.pure_decode_fun,
        value_encoder: Callable[[Any], bytes] = serializer.encode,
        value_decoder: Callable[[bytes], Any] = serializer.decode,
    ) -> None:
        self.key_encoder, self.key_decoder = key_encoder, key_decoder
        self.value_encoder, self.value_decoder = value_encoder, value_decoder

        self.db_path: Path = Path(db_path) if db_path is not None else None
        if self.db_path in LevelDBConnector._instances:
            raise RuntimeError(f"A LevelDB instance for the same db already exists.")

        if db is not None:
            self.db = db
            return

        LevelDBConnector._instances[
            self.db_path
        ] = None  # Ensure the singleton pattern is enforced

        self.db = plyvel.DB(
            self.db_path.expanduser().absolute().as_posix(), create_if_missing=True
        )
        self.iter_params: dict = {}
        self.iter_prefixes: Optional[str, Iterable[str]] = None
        self.iter_starting_by: Optional[str] = None

    def _prefixed_db(self, prefixes: Iterable[bytes]) -> "LevelDBConnector":
        """
        Apply all the prefixes (last one included) to obtain the desired prefixed database

        :param prefixes: the prefix or the iterable of prefixes to apply
        :returns: the prefixed database
        """
        return LevelDBConnector(db_path=None, db=get_prefixed_db(self.db, prefixes))

    def __call__(
        self,
        prefixes: Optional[Union[str, Iterable[str]]] = None,
        starting_by: Optional[str] = None,
        **kwargs,
    ) -> Iterable:
        """
        Builds a custom iterator exploiting the parameters available in plyvel.DB

        :param prefixes: the prefix or the iterable of prefixes to apply
        :param starting_by: start the iteration from the specified prefix
        :param kwargs: additional arguments of plyvel.DB.iterator()
        :returns: the custom iterable
        """
        self.iter_prefixes = prefixes
        self.iter_starting_by = starting_by
        self.iter_params = kwargs
        return iter(self)

    def __iter__(self) -> Iterator:
        """
        Iterate over the database, possibly using the parameters defined in the __call__

        :returns: the iterator
        """
        prefixes = (
            normalize_strings(self.key_encoder, self.iter_prefixes)
            if self.iter_prefixes is not None
            else []
        )
        starting_by = (
            self.key_encoder(self.iter_starting_by)
            if self.iter_starting_by is not None
            else None
        )
        iter_params = self.iter_params
        self.iter_params = {}
        self.iter_prefixes = None
        self.iter_starting_by = None

        subdb = self._prefixed_db(prefixes=prefixes)
        for x in subdb.db.iterator(prefix=starting_by, **iter_params):
            if isinstance(x, bytes):
                try:
                    yield self.key_decoder(x)
                except DecodeError:
                    yield self.value_decoder(x)

            else:
                key, value = x
                yield self.key_decoder(key), self.value_decoder(value)

    def __len__(self) -> int:
        """
        Computes the number of element in the database.
        It may be very slow, use with caution.

        :returns: number of elements in the database
        """
        # todo: fix the dblen of prefixed db!
        return sum(1 for _ in self(include_key=True, include_value=False))

    def __setitem__(self, key: Union[str, Iterable[str]], value: Any) -> None:
        """
        Store the couple (key, value) in leveldb.
        The key and the value are automatically encoded using the encoders registered
        in the serializer.

        The key may be a single string or an iterable of strings, to specify prefixes.
        The last element is always the key.

            >>> ldb = LevelDBConnector.get_instance(tmpfile)
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
        *prefixes, key = normalize_strings(self.key_encoder, key)
        ldb = self._prefixed_db(prefixes)
        ldb.db.put(key, self.value_encoder(value))

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

            >>> ldb = LevelDBConnector.get_instance(tmpfile)
            >>> ldb['key'] = 'value_string1'
            >>> ldb['key']
            'value_string1'
            >>> ldb['prefix1', 'prefix2', 'prefix2', 'key'] = 'value_string2'
            >>> ldb['prefix1', 'prefix2', 'prefix2', 'key']
            'value_string2'
            >>> del ldb['key'], ldb['prefix1', 'prefix2', 'prefix2', 'key'], ldb['key']

        It is possible to retrieve prefixedDB specifying prefixes and using Ellipsis
        as the actual key:

            >>> a = LevelDBConnector.get_instance(tmpfile)
            >>> isinstance(ldb.db, plyvel.DB)
            True
            >>> isinstance(ldb['prefix', ...].db, PrefixedDB)
            True
            >>> isinstance(ldb, LevelDBConnector) and isinstance(ldb['prefix', ...], LevelDBConnector)
            True

        :param key: string or iterable of string to specify prefixes, auto-encoded
                    by the serializer. It's possible to specify sub-db with Ellipsis.
        :returns: the value associated to the key in leveldb, decoded by the serializer.
        """
        *prefixes, key = normalize_strings(self.key_encoder, key)
        ldb = self._prefixed_db(prefixes)
        if key is Ellipsis:
            return ldb
        return self.value_decoder(ldb.db.get(key))

    def __delitem__(self, key: Union[str, Iterable[str]]) -> None:
        """
        Delete the couple (key, value) from leveldb.
        The key is automatically encoded using the encoders registered in the serializer.

        The key may be a single string or an iterable of strings, to specify prefixes.
        The last element is always the key.

            >>> ldb = LevelDBConnector.get_instance(tmpfile)
            >>> ldb['key'] = 'value_string1'
            >>> ldb['prefix1', 'prefix2', 'prefix2', 'key'] = 'value_string2'
            >>> del ldb['key'], ldb['prefix1', 'prefix2', 'prefix2', 'key'], ldb['key']
            >>> len(ldb)
            0

        :param key: string or iterable of string to specify prefixes, auto-encoded by the serializer.
        """
        *prefixes, key = normalize_strings(self.key_encoder, key)
        ldb = self._prefixed_db(prefixes)
        ldb.db.delete(key)

    def __repr__(self) -> str:
        innerdb = f"{self.db}"
        if isinstance(self.db, PrefixedDB):
            innerdb = f"{innerdb[:-21]}>"
        return f"{self.__class__.__name__}(path={self.db_path}, db={innerdb})"


if __name__ == "__main__":
    import doctest
    import tempfile

    tmpfile = tempfile.mkdtemp()
    doctest.testmod(extraglobs={"tmpfile": tmpfile})
    # ldb = LevelDB.get_instance(tmpfile)
    # ldb["prefix1", "prefix2", "prefix2", "key_string2"] = "value_string2"
    # ldb["prefix1sprefix2sprefix2key_string2"]
    # ldb["prefix1", "prefix2", "prefix2", "key_string2"]
