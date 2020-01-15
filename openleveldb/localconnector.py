"""
Class that wraps the LevelDB plyvel to support dict-like behaviour, with automatic serialization to bytes and
deserialization from bytes provided by the serialization module
"""
from pathlib import Path
from typing import Any, Callable, Iterable, Iterator, Optional, Type, Union

import numpy as np
import plyvel
from openleveldb import serializer
from openleveldb.serializer import DecodeError, normalize_strings
from openleveldb.utils import get_prefixed_db
from plyvel._plyvel import PrefixedDB


class LevelDBLocal:
    """
    LevelDB connector

    Sample usage:

        >>> ldb = LevelDBLocal.get_instance(tmpfile)
        >>> ldb['key'] = 'value_string1'
        >>> ldb['key']
        'value_string1'
        >>> del ldb['key']
        >>> len(ldb)
        0
    """

    _instances = {}

    @staticmethod
    def get_instance(db_path: Union[str, Path]) -> "LevelDBLocal":
        """
        Return a singleton instance of LevelDB for each path

        :param db_path: the path to the LevelDB database
        :returns: the LevelDB object
        """
        db_path = Path(db_path)
        if db_path not in LevelDBLocal._instances:
            LevelDBLocal._instances[db_path] = LevelDBLocal(db_path)
        return LevelDBLocal._instances[db_path]

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
        if self.db_path in LevelDBLocal._instances:
            raise RuntimeError(
                f"{LevelDBLocal._instances[self.db_path]} already exists."
            )

        if db is not None:
            self.db = db
            return

        LevelDBLocal._instances[
            self.db_path
        ] = None  # Ensure the singleton pattern is enforced

        self.db = plyvel.DB(
            self.db_path.expanduser().absolute().as_posix(), create_if_missing=True
        )
        self.iter_params: dict = {}
        self.iter_prefixes: Optional[str, Iterable[str]] = None
        self.iter_starting_by: Optional[str] = None

    def _prefixed_db(self, prefixes: Iterable[bytes]) -> "LevelDBLocal":
        """
        Apply all the prefixes (last one included) to obtain the desired prefixed database

        :param prefixes: the prefix or the iterable of prefixes to apply
        :returns: the prefixed database
        """
        return LevelDBLocal(db_path=None, db=get_prefixed_db(self.db, prefixes))

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

            >>> ldb = LevelDBLocal.get_instance(tmpfile)
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

        if key is Ellipsis:
            raise TypeError(f"str prefix or key expected, got {type(key).__name__}")

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

            >>> ldb = LevelDBLocal.get_instance(tmpfile)
            >>> ldb['key'] = 'value_string1'
            >>> ldb['key']
            'value_string1'
            >>> ldb['prefix1', 'prefix2', 'prefix2', 'key'] = 'value_string2'
            >>> ldb['prefix1', 'prefix2', 'prefix2', 'key']
            'value_string2'
            >>> del ldb['key'], ldb['prefix1', 'prefix2', 'prefix2', 'key'], ldb['key']

        It is possible to retrieve prefixedDB specifying prefixes and using Ellipsis
        as the actual key:

            >>> a = LevelDBLocal.get_instance(tmpfile)
            >>> isinstance(ldb.db, plyvel.DB)
            True
            >>> isinstance(ldb['prefix', ...].db, PrefixedDB)
            True
            >>> isinstance(ldb, LevelDBLocal) and isinstance(ldb['prefix', ...], LevelDBLocal)
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

            >>> ldb = LevelDBLocal.get_instance(tmpfile)
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
        # if isinstance(self.db, PrefixedDB):
        #     innerdb = f"{innerdb[:-21]}>"
        return f"{self.__class__.__name__}(path='{self.db_path}', db={innerdb})"

    def close(self) -> None:
        self.db.close()


if __name__ == "__main__":
    import doctest
    import tempfile

    tmpfile = tempfile.mkdtemp()
    doctest.testmod(extraglobs={"tmpfile": tmpfile})
