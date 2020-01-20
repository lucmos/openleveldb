"""
Class that wraps the LevelDB plyvel to support dict-like behaviour,
with automatic serialization to bytes and deserialization from bytes provided
by the serialization module
"""
from pathlib import Path
from typing import Any, Callable, Iterable, Optional, Union

import plyvel
from openleveldb.backend import serializer
from openleveldb.backend.connectorcommon import get_prefixed_db
from openleveldb.backend.serializer import normalize_strings


class LevelDBLocal:

    _instances = {}

    @staticmethod
    def get_instance(db_path: Union[str, Path]) -> "LevelDBLocal":
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

    def prefixed_db(self, prefixes: Iterable[bytes]) -> "LevelDBLocal":
        return LevelDBLocal(
            db_path=None,
            db=get_prefixed_db(self.db, prefixes) if prefixes is not None else self.db,
        )

    # todo: must be tested
    def prefixed_iter(
        self,
        prefixes: Optional[Union[str, Iterable[str]]] = None,
        starting_by: Optional[str] = None,
        include_key=True,
        include_value=True,
    ) -> Iterable:
        prefixes = (
            normalize_strings(self.key_encoder, prefixes)
            if prefixes is not None
            else ()
        )
        starting_by = b"".join(
            serializer.normalize_strings(self.key_encoder, starting_by)
            if starting_by is not None
            else ()
        )
        subdb = self.prefixed_db(prefixes=prefixes)
        for x in subdb.db.iterator(
            prefix=starting_by, include_key=include_key, include_value=include_value
        ):
            if isinstance(x, bytes):
                if include_key:
                    yield self.key_decoder(x)
                else:
                    yield self.value_decoder(x)

            else:
                try:
                    key, value = x
                    yield self.key_decoder(key), self.value_decoder(value)
                except TypeError:
                    yield None

    def prefixed_len(
        self,
        prefixes: Optional[Union[str, Iterable[str]]] = None,
        starting_by: Optional[str] = None,
    ) -> int:
        return sum(
            1
            for _ in self.prefixed_iter(
                prefixes=prefixes,
                starting_by=starting_by,
                include_key=True,
                include_value=False,
            )
        )

    def __iter__(self) -> Iterable:
        return self.prefixed_iter()

    def __len__(self) -> int:
        return sum(1 for _ in self.prefixed_iter(include_key=True, include_value=False))

    def __setitem__(self, key: Union[str, Iterable[str]], value: Any) -> None:
        *prefixes, key = normalize_strings(self.key_encoder, key)

        if key is Ellipsis:
            raise TypeError(f"str prefix or key expected, got {type(key).__name__}")

        ldb = self.prefixed_db(prefixes)
        ldb.db.put(key, self.value_encoder(value))

    def __getitem__(
        self, key: Union[str, Iterable[Union[str, Ellipsis.__class__]]]
    ) -> Any:
        *prefixes, key = normalize_strings(self.key_encoder, key)
        ldb = self.prefixed_db(prefixes)
        if key is Ellipsis:
            return ldb
        out = ldb.db.get(key)
        return self.value_decoder(out) if out else None

    def __delitem__(self, key: Union[str, Iterable[str]]) -> None:
        *prefixes, key = normalize_strings(self.key_encoder, key)
        ldb = self.prefixed_db(prefixes)
        ldb.db.delete(key)

    def __repr__(self) -> str:
        innerdb = f"{self.db}"
        return f"{self.__class__.__name__}(path='{self.db_path}', db={innerdb})"

    def close(self) -> None:
        self.db.close()


if __name__ == "__main__":
    import doctest
    import tempfile

    tmpfile = tempfile.mkdtemp()
    doctest.testmod(extraglobs={"tmpfile": tmpfile})
