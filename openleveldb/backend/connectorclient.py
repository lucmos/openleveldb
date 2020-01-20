"""
Class that communicates with the server module to support dict-like behaviour,
with automatic serialization to bytes and deserialization from bytes provided
by the serialization module
"""

import pickle
from pathlib import Path
from typing import Any, Callable, Iterable, Optional, Union

import requests

from flask import Response
from openleveldb.backend.serializer import DecodeType, decode, encode, normalize_strings


class LevelDBClient:

    _instances = {}

    @staticmethod
    def get_instance(
        db_path: Union[str, Path], server_address: str,
    ) -> "LevelDBClient":
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
                if include_key:
                    yield DecodeType.STR.pure_decode_fun(x)
                else:
                    yield self.value_decoder(x)

            else:
                try:
                    key, value = x
                    yield DecodeType.STR.pure_decode_fun(key), self.value_decoder(value)
                except TypeError:
                    yield None

    def __iter__(self) -> Iterable:
        return self.prefixed_iter([], None, True, True)

    def prefixed_len(
        self,
        prefixes: Optional[Union[str, Iterable[str]]] = None,
        starting_by: Optional[str] = None,
    ) -> int:
        res = requests.get(
            url=self.server_address + "/dblen",
            params={
                "dbpath": self.db_path,
                "prefixes": prefixes,
                "starting_by": starting_by,
            },
        )
        return decode(res.content)

    def __len__(self) -> int:
        res = requests.get(
            url=self.server_address + "/dblen",
            params={"dbpath": self.db_path, "prefixes": None, "starting_by": None,},
        )
        return decode(res.content)

    def __setitem__(self, key: Union[str, Iterable[str]], value: Any) -> Response:
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
        *prefixes, key = normalize_strings(lambda x: x, key)

        if key is Ellipsis:
            raise NotImplementedError

        res = requests.get(
            url=self.server_address + "/getitem",
            params={"dbpath": self.db_path, "key": key, "prefixes": prefixes},
        )

        return self.value_decoder(res.content) if res.content else None

    def __delitem__(self, key: Union[str, Iterable[str]]) -> Response:
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
    pass
