import io
import tempfile
from operator import itemgetter
from pathlib import Path

import numpy as np
import openleveldb.backend.server
import plyvel
import pytest
import rapidjson
from openleveldb import __version__
from openleveldb.backend.config import get_env, load_envs
from openleveldb.backend.connectorclient import LevelDBClient
from openleveldb.backend.connectorlocal import LevelDBLocal
from openleveldb.backend.serializer import DecodeError, Serializer, decode, encode
from openleveldb.database import LevelDB


def test_version() -> None:
    assert __version__ == "0.1.2"


LOCAL_TEMP_DATABASE = tempfile.mkdtemp()
REMOTE_TEMP_DATABASE = tempfile.mkdtemp()
LOCAL_READONLY_TEMP_DATABASE = tempfile.mkdtemp()
REMOTE_READONLY_TEMP_DATABASE = tempfile.mkdtemp()

load_envs()
TEST_SERVER_ADDRESS = get_env("TEST_SERVER_ADDRESS")
TEST_SERVER_PORT = get_env("TEST_SERVER_PORT")
TEST_READONLY_SERVER_PORT = get_env("TEST_READONLY_SERVER_PORT")


@pytest.fixture(
    scope="session", autouse=True,
)
def start_dummy_server() -> None:
    p = openleveldb.backend.server.dummy_server(TEST_SERVER_PORT)
    yield None
    p.kill()


@pytest.fixture(
    scope="session", autouse=True,
)
def start_dummy_read_only_server() -> None:
    p = openleveldb.backend.server.dummy_server(TEST_READONLY_SERVER_PORT)
    yield None
    p.kill()


@pytest.fixture(
    scope="module",
    params=[
        (LOCAL_TEMP_DATABASE, None, False),
        (LOCAL_READONLY_TEMP_DATABASE, None, True),
        (REMOTE_TEMP_DATABASE, f"{TEST_SERVER_ADDRESS}:{TEST_SERVER_PORT}", False),
        (
            REMOTE_READONLY_TEMP_DATABASE,
            f"{TEST_SERVER_ADDRESS}:{TEST_READONLY_SERVER_PORT}",
            True,
        ),
    ],
)
def db(request) -> LevelDB:
    tempdbpath, address, read_only = request.param
    ldb = LevelDB(
        db_path=tempdbpath,
        server_address=address,
        dbconnector=None,
        read_only=read_only,
    )
    yield ldb
    ldb.close()


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
    def test_db_readonly(self, db: LevelDB) -> None:
        def _write(db):
            db["key"] = 0

        def _read(db):
            return db["key"]

        def _del(db):
            del db["key"]

        if db.read_only:
            with pytest.raises(
                io.UnsupportedOperation, match="not writable"
            ) as execinfo:
                _write(db)
            _read(db)
            with pytest.raises(
                io.UnsupportedOperation, match="not writable"
            ) as execinfo:
                _del(db)
        else:
            _write(db)
            _read(db)
            _del(db)
        assert len(db) == 0

    def test_db_creation(self, db: LevelDB) -> None:
        if db.server_address is None:
            assert isinstance(db.dbconnector, LevelDBLocal)
        else:
            assert isinstance(db.dbconnector, LevelDBClient)

    def test_db_operativity(self, db: LevelDB) -> None:
        if db.read_only:
            return
        nu = 10

        # writing
        for x in range(nu):
            key = f"{x}"
            db[key] = np.random.rand(3, 10)
        assert len(db) == nu

        # reading
        for x in range(nu):
            key = f"{x}"
            v = db[key]

        # iterating
        for x in db:
            pass

        # deleting
        for x in range(nu):
            key = f"{x}"
            del db[key]

        assert len(db) == 0

    keys1 = (
        ("prefixb", "k1"),
        ("prefixc", "k4"),
        ("prefixb", "k2"),
        ("prefixb", "k3"),
        ("prefixc", "k5"),
    )
    values1 = ("v1", "v2", "v3", "v4", "v5")
    prefixeslens1 = [
        ("", 5),
        ("prefixb", 3),
        ("prefixc", 2),
        ("prefixa", 0),
        ("prefixd", 0),
        ("prefix", 5),
        ("prefixaa", 0),
    ]

    @pytest.mark.parametrize(
        argnames="keys,values", argvalues=[(keys1, values1,)],
    )
    def test_db_prefixed_iter(self, db: LevelDB, keys, values) -> None:
        if db.read_only:
            return

        for k, v in zip(keys, values):
            db[k] = v

        assert (
            repr([x for x in db.prefixed_iter(include_key=False)])
            == "['v1', 'v3', 'v4', 'v2', 'v5']"
        )
        assert (
            repr([x for x in db.prefixed_iter(include_value=False)])
            == "['prefixbk1', 'prefixbk2', 'prefixbk3', 'prefixck4', 'prefixck5']"
        )
        all_pairs = (
            "[('prefixbk1', 'v1'), "
            "('prefixbk2', 'v3'), "
            "('prefixbk3', 'v4'), "
            "('prefixck4', 'v2'), "
            "('prefixck5', 'v5')]"
        )
        res_prefix_startingby = [
            (
                "prefixb",
                "[('k1', 'v1'), ('k2', 'v3'), ('k3', 'v4')]",
                "[('prefixbk1', 'v1'), ('prefixbk2', 'v3'), ('prefixbk3', 'v4')]",
            ),
            (
                "prefixc",
                "[('k4', 'v2'), ('k5', 'v5')]",
                "[('prefixck4', 'v2'), ('prefixck5', 'v5')]",
            ),
            ("prefixa", "[]", "[]",),
            ("prefixd", "[]", "[]",),
            ("", all_pairs, all_pairs),
        ]
        assert repr([x for x in db]) == all_pairs

        for key, res_prefix, res_startingby in res_prefix_startingby:

            assert repr([x for x in db.prefixed_iter(prefixes=[key])]) == res_prefix
            assert repr([x for x in db.prefixed_iter(prefixes=key)]) == res_prefix

            assert (
                repr([x for x in db.prefixed_iter(starting_by=[key])]) == res_startingby
            )
            assert (
                repr([x for x in db.prefixed_iter(starting_by=key)]) == res_startingby
            )

        for x in keys:
            del db[x]
        assert len(db) == 0

    @pytest.mark.parametrize(
        argnames="keys,values", argvalues=[(keys1, values1,)],
    )
    def test_db_iter(self, db: LevelDB, keys, values) -> None:
        if db.read_only:
            return

        for k, v in zip(keys, values):
            db[k] = v

        for (x, y), (xx, yy) in zip(db, (sorted(zip(keys, values), key=itemgetter(0)))):
            assert db[x] == yy
            assert db[xx] == y

        for x in keys:
            del db[x]
        assert len(db) == 0

    @pytest.mark.parametrize(
        argnames="keys,values,prefixeslens",
        argvalues=[(keys1, values1, prefixeslens1,)],
    )
    def test_db_prefixed_len(self, db: LevelDB, keys, values, prefixeslens) -> None:
        if db.read_only:
            return

        for k, v in zip(keys, values):
            db[k] = v

        for k, v in zip(keys, values):
            assert v == db[k]

        for prefixlen in prefixeslens:
            prefix, plen = prefixlen
            assert db.prefixed_len(prefix) == plen

        assert len(db) == len(keys) == len(values)
        for x in keys:
            del db[x]
        assert len(db) == 0

    def test_db_repr(self, db: LevelDB) -> None:
        name = db.__class__.__name__
        path = db.db_path
        ip = f"'{db.server_address}'" if db.server_address is not None else None
        out = f"{name}(db_path='{path}', server_address={ip},"
        assert repr(db)[: len(out)] == out

    def test_base_retrieval(self, db: LevelDB) -> None:
        if db.read_only:
            return
        db["key"] = "value_string1"
        assert db["key"] == "value_string1"
        del db["key"]
        assert len(db) == 0
        db["prefix1", "prefix2", "prefix2", "key"] = "value_string2"
        assert db["prefix1", "prefix2", "prefix2", "key"] == "value_string2"
        del db["prefix1", "prefix2", "prefix2", "key"]
        assert len(db) == 0

    def test_json_serialization(self, db: LevelDB) -> None:
        if db.read_only:
            return
        a = {"key1": 10.5, "key2": [1, 2, {"key3": -1}]}
        db["key"] = a
        assert db["key"] == a
        assert a == db["key"]

        db["key"] = str.encode(rapidjson.dumps(a), encoding="utf-8")
        assert db["key"] == str.encode(rapidjson.dumps(a), encoding="utf-8")

        db["key"] = encode(a)
        assert db["key"] == encode(a)

        del db["key"]
        assert len(db) == 0

    def test_numpy_serialization(self, db: LevelDB) -> None:
        if db.read_only:
            return
        a = np.array([[1, 2, 3, 4, 5], [6, 7, 8, 9, 10]], dtype=np.float16)
        db["key"] = a
        assert repr(db["key"]) == repr(a)
        assert np.array_equal(a, db["key"])

        db["key"] = Serializer.ndarray_tobytes(a)
        assert db["key"] == Serializer.ndarray_tobytes(a)

        db["key"] = encode(a)
        assert repr(decode(db["key"])) == repr(a)
        assert np.array_equal(a, decode(db["key"]))

        del db["key"]
        assert len(db) == 0

    def test_int_serialization(self, db: LevelDB) -> None:
        if db.read_only:
            return
        for a in [42, -100000000000000000000000000, int(1e10)]:
            db["key"] = a
            assert db["key"] == a

            db["key"] = int.to_bytes(a, length=16, byteorder="big", signed=True)
            assert db["key"] == int.to_bytes(a, length=16, byteorder="big", signed=True)

            db["key"] = encode(a)
            assert db["key"] == encode(a)

            del db["key"]
            assert len(db) == 0

    def test_str_serialization(self, db: LevelDB) -> None:
        if db.read_only:
            return
        a = "just testing some conversions... :)"

        db["key"] = a
        assert a == db["key"]

        db["key"] = b"value_bytes1"
        assert db["key"] == b"value_bytes1"

        db["key"] = encode(a)
        assert db["key"] == encode(a)
        del db["key"]
        assert len(db) == 0

    def test_prefix_consistency(self, db: LevelDB) -> None:
        if db.read_only:
            return
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
        if db.read_only:
            return
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

    def test_regression_prefix_iterator(self, db: LevelDB):
        if db.read_only:
            return
        # wrong detection of what the prefixed iter should return
        db["key"] = "value"
        assert list(db.prefixed_iter(include_key=False, include_value=False))[0] is None
        assert list(db.prefixed_iter(include_key=True, include_value=False)) == ["key"]
        assert list(db.prefixed_iter(include_key=False, include_value=True)) == [
            "value"
        ]
        del db["key"]
        assert len(db) == 0

    def test_regression_none(self, db: LevelDB):
        if db["key"] is not None:
            raise RuntimeError(f'{db["key"]}')

    keysvalues2 = [
        (["a", "0", "key"], 0.42),
        (["a", "n", "key"], np.array([[1, 2], [2, 4]])),
        (["a", "N", "key"], None),
        (["a", "", "key"], ""),
        (["a", "h", "key"], "hola"),
        (["a", "{", "key"], {":)": ":("}),
        (["a", "t", "key"], []),
        (["a", "s", "key"], [3, 4, 5]),
        (["a", "s", "key", "a"], [[3, 4, 5], [3, [3, [3, 4, 5], 5], 5]]),
        (["a", "s", "key", "b"], {":(": [[3, 4, 5], [3, [3, [3, 4, 5], 5], 5]]}),
        (["a", "^", "key"], -42),
        (["a", "1", "key"], 1e100),
        (["a", "-", "key"], -1e100),
    ]

    @pytest.mark.parametrize(
        argnames="keysvalues2", argvalues=[keysvalues2],
    )
    def test_db_fancystore(self, db: LevelDB, keysvalues2) -> None:
        if db.read_only:
            return
        for k, v in keysvalues2:
            db[k] = v

        for k, v in keysvalues2:
            value = db[k]
            if isinstance(value, np.ndarray):
                assert np.array_equal(value, v)
            else:
                assert v == db[k]
