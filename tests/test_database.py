import os
import sys
import tempfile
from multiprocessing import Process
from pathlib import Path
from threading import Thread

import numpy as np
import openleveldb.server
import orjson
import plyvel
import pytest
from openleveldb import __version__
from openleveldb.clientconnector import LevelDBClient
from openleveldb.database import LevelDB
from openleveldb.localconnector import LevelDBLocal
from openleveldb.serializer import DecodeError, Serializer, decode, encode


def test_version() -> None:
    assert __version__ == "0.1.0"


LOCAL_TEMP_DATABASE = tempfile.mkdtemp()
REMOTE_TEMP_DATABASE = tempfile.mkdtemp()
REMOTE_PORT = 9999


@pytest.fixture(scope="session", autouse=True)
def dummy_server() -> None:
    def runflask() -> None:
        sys.stdout = open(os.devnull, "w")
        openleveldb.server.run(REMOTE_PORT)

    p = Process(target=runflask)
    p.start()
    yield None
    p.kill()


# @pytest.fixture(scope="session")
# def db_path(tmp_path_factory) -> Path:
#     return tmp_path_factory.mktemp("pytest.leveldb", numbered=True)


@pytest.fixture(
    scope="module",
    params=[
        (LOCAL_TEMP_DATABASE, None),
        (REMOTE_TEMP_DATABASE, f"http://127.0.0.1:{REMOTE_PORT}",),
    ],
)
def db(request) -> LevelDB:
    tempdbpath, address = request.param
    ldb = LevelDB(db_path=tempdbpath, server_address=address,)
    yield ldb
    del ldb


# class TestPrint:
#     def test_1(self, capsys, db) -> None:
#         with capsys.disabled():
#             print(db)
#             # print(db.dbconnector)
#
#     def test_2(self, capsys, db) -> None:
#         with capsys.disabled():
#             print(db)
#             # print(db.dbconnector)


class TestDatabase:
    def test_db_creation(self, db: LevelDB) -> None:
        if db.server_address is None:
            assert isinstance(db.dbconnector, LevelDBLocal)
        else:
            assert isinstance(db.dbconnector, LevelDBClient)

    def test_db_repr(self, db: LevelDB) -> None:
        name = db.__class__.__name__
        path = db.db_path
        ip = f"'{db.server_address}'" if db.server_address is not None else None
        out = f"{name}(db_path='{path}', server_address={ip},"
        assert repr(db)[: len(out)] == out

    def test_base_retrieval(self, capsys, db: LevelDB) -> None:
        db["key"] = "value_string1"
        with capsys.disabled():
            print(db["key"])
        assert db["key"] == "value_string1"
        del db["key"]
        assert len(db) == 0
        db["prefix1", "prefix2", "prefix2", "key"] = "value_string2"
        assert db["prefix1", "prefix2", "prefix2", "key"] == "value_string2"
        del db["prefix1", "prefix2", "prefix2", "key"]
        assert len(db) == 0

    def test_json_serialization(self, db: LevelDB) -> None:
        a = {"key1": 10.5, "key2": [1, 2, {"key3": -1}]}
        db["key"] = a
        assert db["key"] == a
        assert a == db["key"]

        db["key"] = orjson.dumps(a)

        with pytest.raises(
            DecodeError, match="missing DecodeType identifier in bytes blob",
        ) as execinfo:
            value = db["key"]

        db["key"] = encode(a)
        assert db["key"] == a

        del db["key"]
        assert len(db) == 0

    def test_numpy_serialization(self, db: LevelDB) -> None:
        a = np.array([[1, 2, 3, 4, 5], [6, 7, 8, 9, 10]], dtype=np.float16)
        db["key"] = a
        assert repr(db["key"]) == repr(a)
        assert np.array_equal(a, db["key"])

        db["key"] = Serializer.ndarray_tobytes(a)
        with pytest.raises(
            DecodeError, match="missing DecodeType identifier in bytes blob",
        ) as execinfo:
            value = db["key"]

        db["key"] = encode(a)
        assert repr(db["key"]) == repr(a)
        assert np.array_equal(a, db["key"])

        del db["key"]
        assert len(db) == 0

    def test_int_serialization(self, db: LevelDB) -> None:
        for a in [42, -100000000000000000000000000, int(1e10)]:
            db["key"] = a
            assert db["key"] == a

            db["key"] = int.to_bytes(a, length=16, byteorder="big", signed=True)
            with pytest.raises(
                DecodeError, match="missing DecodeType identifier in bytes blob",
            ) as execinfo:
                value = db["key"]

            db["key"] = encode(a)
            assert db["key"] == a

            del db["key"]
            assert len(db) == 0

    def test_str_serialization(self, db: LevelDB) -> None:
        a = "just testing some conversions... :)"

        db["key"] = a
        assert a == db["key"]

        db["key"] = b"value_bytes1"
        with pytest.raises(
            DecodeError, match="missing DecodeType identifier in bytes blob",
        ) as execinfo:
            value = db["key"]

        db["key"] = encode(a)
        assert db["key"] == a
        del db["key"]
        assert len(db) == 0

    def test_prefix_consistency(self, db: LevelDB) -> None:
        db["prefix1", "prefix2", "prefix2", "key_string2"] = "value_string2"
        assert (
            db["prefix1prefix2prefix2key_string2"]
            == db["prefix1", "prefix2", "prefix2", "key_string2"]
        )
        assert db["prefix1", "prefix2", "prefix2", "key_string2"] == "value_string2"
        db["prefix1prefix2prefix2key_string2"] = "value_string3"
        assert (
            db["prefix1prefix2prefix2key_string2"]
            == db["prefix1", "prefix2", "prefix2", "key_string2"]
        )
        assert db["prefix1", "prefix2", "prefix2", "key_string2"] == "value_string3"
        assert db["prefix1prefix2prefix2key_string2"] == "value_string3"
        del db["prefix1", "prefix2", "prefix2", "key_string2"]
        assert len(db) == 0

    @pytest.mark.parametrize("key,", [b"key", 0, 354.342, None, ...])
    def test_key_str_type(self, db: LevelDB, key) -> None:
        with pytest.raises(
            TypeError, match=f"str prefix or key expected, got {type(key).__name__}",
        ) as execinfo:
            db[key] = "value_bytes1"

        with pytest.raises(
            TypeError, match=f"str prefix or key expected, got {type(key).__name__}",
        ) as execinfo:
            db["_", "_", key] = "value_bytes1"

        with pytest.raises(
            TypeError, match=f"str prefix or key expected, got {type(key).__name__}",
        ) as execinfo:
            db["_", key, "_"] = "value_bytes1"

        with pytest.raises(
            TypeError, match=f"str prefix or key expected, got {type(key).__name__}",
        ) as execinfo:
            db[key, "_", "_"] = "value_bytes1"

    def test_singleton_pattern(self, tmp_path: Path) -> None:
        a = LevelDBLocal(tmp_path)
        with pytest.raises(RuntimeError, match=f".* already exists.",) as execinfo:
            a2 = LevelDBLocal(tmp_path)
        a3 = LevelDBLocal.get_instance(tmp_path)

    def test_ellipsis_syntax(self, db: LevelDB) -> None:
        dbconnector = db.dbconnector
        assert isinstance(db, LevelDB)
        assert isinstance(db.dbconnector, (LevelDBClient, LevelDBLocal))

        # todo: implement even in remote db
        subdb = None
        if isinstance(dbconnector, LevelDBLocal):
            subdb = db["prefix", ...]
            assert isinstance(subdb, LevelDB)
            assert isinstance(subdb.dbconnector, (LevelDBClient, LevelDBLocal))

        # only in local connector
        if isinstance(dbconnector, LevelDBLocal):
            assert isinstance(db.dbconnector.db, plyvel.DB)
            assert isinstance(subdb.dbconnector.db, plyvel._plyvel.PrefixedDB)
