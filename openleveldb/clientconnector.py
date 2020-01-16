import pickle
from pathlib import Path
from typing import Any, Callable, Iterable, Optional, Union

import requests

import numpy as np
import plyvel
from flask import Response
from openleveldb.serializer import (
    DecodeError,
    DecodeType,
    decode,
    encode,
    normalize_strings,
)
from plyvel._plyvel import PrefixedDB
from tqdm import tqdm


class LevelDBClient:

    _instances = {}

    @staticmethod
    def get_instance(
        db_path: Union[str, Path], server_address: str = "http://127.0.0.1:5000",
    ) -> "LevelDBClient":
        """
        Return a singleton instance of LevelDB for each path

        :param server_address:
        :param db_path:
        :returns: the LevelDBClient object
        """
        db_path = Path(db_path)
        if db_path not in LevelDBClient._instances:
            LevelDBClient._instances[(server_address, db_path)] = LevelDBClient(
                server_address, db_path
            )
        return LevelDBClient._instances[(server_address, db_path)]

    def __init__(
        self,
        server_address: str,
        db_path: Union[str, Path],
        value_encoder: Callable[[Any], bytes] = encode,
        value_decoder: Callable[[bytes], Any] = decode,
    ) -> None:
        self.value_encoder, self.value_decoder = value_encoder, value_decoder

        self.server_address = server_address
        self.db_path = db_path

    def _prefixed_db(self, prefixes: Iterable[str]) -> str:
        """
        Apply all the prefixes (last one included) to obtain the desired prefixed database

        :param prefixes: the prefix or the iterable of prefixes to apply
        :returns: the prefixed database
        """
        res = requests.get(
            url=self.server_address + "/get_prefixed_db_path",
            params={"prefixes": prefixes, "dbpath": self.db_path},
        )
        return res.text

    def prefixed_iter(
        self,
        prefixes: Optional[Union[str, Iterable[str]]] = None,
        starting_by: Optional[str] = None,
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
        res = requests.get(
            url=self.server_address + "/iterator",
            params={
                "dbpath": self.db_path,
                "include_key": include_key,
                "include_value": include_value,
                "starting_by": starting_by,
                "prefixes": prefixes,
            },
        )
        for x in pickle.loads(res.content):
            if isinstance(x, bytes):
                try:
                    yield DecodeType.STR.pure_decode_fun(x)
                except DecodeError:
                    yield self.value_decoder(x)

            else:
                key, value = x
                yield DecodeType.STR.pure_decode_fun(key), self.value_decoder(value)

    def __iter__(self) -> Iterable:
        """
        Default iterator over (key, value) couples

        :returns: the iterator
        """
        return self.prefixed_iter([], None, True, True)

    def prefixed_len(
        self,
        prefixes: Optional[Union[str, Iterable[str]]] = None,
        starting_by: Optional[str] = None,
    ) -> int:
        res = requests.get(
            url=self.server_address + "/prefixed_dblen",
            params={
                "dbpath": self.db_path,
                "prefixes": prefixes,
                "starting_by": starting_by,
            },
        )
        return decode(res.content)

    def __len__(self) -> int:
        """
        Computes the number of element in the database.
        It may be very slow, use with caution.

        :returns: number of elements in the database
        """
        res = requests.get(
            url=self.server_address + "/dblen", params={"dbpath": self.db_path},
        )
        return decode(res.content)

    def __setitem__(self, key: Union[str, Iterable[str]], value: Any) -> Response:
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
        *prefixes, key = normalize_strings(lambda x: x, key)

        if key is Ellipsis:
            raise TypeError(f"str prefix or key expected, got {type(key).__name__}")

        res = requests.post(
            url=self.server_address + "/setitem",
            data=encode(value),
            headers={"Content-Type": "application/octet-stream"},
            params={"dbpath": self.db_path, "key": key, "prefixes": prefixes},
        )
        return res

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
        *prefixes, key = normalize_strings(lambda x: x, key)

        if key is Ellipsis:
            raise NotImplementedError

        res = requests.get(
            url=self.server_address + "/getitem",
            params={"dbpath": self.db_path, "key": key, "prefixes": prefixes},
        )

        return self.value_decoder(res.content)

    def __delitem__(self, key: Union[str, Iterable[str]]) -> Response:
        """
        Delete the couple (key, value) from leveldb.
        The key is automatically encoded using the encoders registered in the serializer.

        The key may be a single string or an iterable of strings, to specify prefixes.
        The last element is always the key.

            >>> ldb = LevelDBLocal.get_instance(tmpfile)
            >>> ldb['key'] = 'value_string1'
            >>> ldb['prefix1', 'prefix2', 'prefix2', 'key'] = 'value_string2'
            >>> del ldb['key'], ldb['prefix1', 'prefix2', 'prefix2', 'key'], ldb['key']
            >>> dblen(ldb)
            0

        :param key: string or iterable of string to specify prefixes, auto-encoded by the serializer.
        """
        *prefixes, key = normalize_strings(lambda x: x, key)

        res = requests.delete(
            url=self.server_address + "/delitem",
            params={"dbpath": self.db_path, "key": key, "prefixes": prefixes},
        )
        return res

    def __repr__(self) -> str:
        res = requests.get(
            url=self.server_address + "/repr",
            params={"dbpath": self.db_path, "classname": self.__class__.__name__},
        )
        return res.text

    def close(self) -> None:
        pass


if __name__ == "__main__":

    db = LevelDBClient.get_instance(
        db_path="azz", server_address="http://127.0.0.1:5000"
    )

    db["_", "_", ...] = "value_bytes1"

    nu = 100
    for x in tqdm(range(nu), desc="writing"):
        key = f"{x}"
        db[key] = np.random.rand(3, 1000)

    for x in tqdm(range(nu), desc="reading"):
        key = f"{x}"
        v = db[key]
    # db["ciaosdf"] = np.array([1, 2])
    # a = db["ciaosdf"]
    #
    # testing = [
    #     len(db),
    #     db,
    # ]
    # for x in testing:
    #     print(x)
