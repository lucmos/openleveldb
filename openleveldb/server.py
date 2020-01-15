import http
import io
import os
import sys
from pathlib import Path
from typing import Iterable, Optional, Tuple, Union

import plyvel
from flask import Flask, Response, g, make_response, request
from openleveldb import serializer
from openleveldb.serializer import DecodeType
from openleveldb.utils import get_prefixed_db

app = Flask(__name__)


def get_db(dbpath: Union[str, Path]) -> plyvel.DB:
    dbpath: Path = Path(dbpath)

    if not hasattr(g, "dbs"):
        g.dbs = {}

    if dbpath not in g.dbs:
        g.dbs[dbpath] = plyvel.DB(
            dbpath.expanduser().absolute().as_posix(), create_if_missing=True
        )

    return g.dbs[dbpath]


@app.teardown_appcontext
def close_db(error) -> None:
    """Closes the database again at the end of the request."""
    if hasattr(g, "dbs"):
        for x, y in g.dbs.items():
            if hasattr(y, "close"):
                y.close()


@app.route("/custom_iterator", methods=["GET"])
def custom_iterator() -> Iterable[Union[bytes, Tuple[bytes, bytes]]]:
    # todo: stream https://flask.palletsprojects.com/en/1.1.x/patterns/streaming/
    raise NotImplementedError


@app.route("/iterator", methods=["GET"])
def iterator() -> Iterable[Union[bytes, Tuple[bytes, bytes]]]:
    # todo: stream https://flask.palletsprojects.com/en/1.1.x/patterns/streaming/
    raise NotImplementedError


@app.route("/dblen", methods=["GET"])
def dblen() -> str:
    dbpath = request.args.get("dbpath")

    db = get_db(dbpath)
    out = sum(1 for _ in db.iterator(include_key=True, include_value=False))

    response = make_response(serializer.encode(out))
    response.headers.set("Content-Type", "application/octet-stream")
    return response


@app.route("/setitem", methods=["POST"])
def setitem() -> None:
    dbpath = request.args.get("dbpath")
    key = request.args.get("key")
    value = request.get_data()
    keybytes = DecodeType.STR.pure_encode_fun(key)

    prefixes = request.args.getlist("prefixes")
    prefixes = serializer.normalize_strings(DecodeType.STR.pure_encode_fun, prefixes)
    db = get_prefixed_db(get_db(dbpath), prefixes)

    db.put(keybytes, value)

    response = make_response(key)
    response.headers.set("Content-Type", "application/octet-stream")
    return response


@app.route("/getitem", methods=["GET"])
def getitem() -> Response:
    dbpath = request.args.get("dbpath")
    key = request.args.get("key")
    keybytes = DecodeType.STR.pure_encode_fun(key)

    prefixes = request.args.getlist("prefixes")
    prefixes = serializer.normalize_strings(DecodeType.STR.pure_encode_fun, prefixes)
    db = get_prefixed_db(get_db(dbpath), prefixes)

    out = db.get(keybytes, default=b"")
    response = make_response(out)
    response.headers.set("Content-Type", "application/octet-stream")
    return response


@app.route("/delitem", methods=["DELETE"])
def delitem() -> (str, http.HTTPStatus):
    dbpath = request.args.get("dbpath")
    key = request.args.get("key")
    keybytes = DecodeType.STR.pure_encode_fun(key)

    prefixes = request.args.getlist("prefixes")
    prefixes = serializer.normalize_strings(DecodeType.STR.pure_encode_fun, prefixes)
    db = get_prefixed_db(get_db(dbpath), prefixes)

    db.delete(keybytes)

    response = make_response(key)
    response.headers.set("Content-Type", "text")
    return response


@app.route("/repr", methods=["GET"])
def repr() -> str:
    dbpath = request.args.get("dbpath")
    classname = request.args.get("classname")

    prefixes = request.args.getlist("prefixes")
    prefixes = serializer.normalize_strings(DecodeType.STR.pure_encode_fun, prefixes)
    db = get_prefixed_db(get_db(dbpath), prefixes)

    innerdb = f"{db}"
    # if isinstance(db, PrefixedDB):
    #     innerdb = f"{innerdb[:-21]}>"
    dbrepr = f"{classname}(path='{dbpath}', db={innerdb})"

    response = make_response(dbrepr)
    response.headers.set("Content-Type", "text")
    return response


def run(port: int) -> None:
    app.run(port=port)


if __name__ == "__main__":
    import requests
    import numpy as np

    # # # Get prefixed db
    # res = requests.get(
    #     url="http://127.0.0.1:5000/get_prefixed_db_path",
    #     params={"prefixes": ["prefix1", "prefix2", "prefix3", "key"], "dbpath": "azz",},
    # )
    #
    # print("prefixed", res.content, type(res.content))
    # print()

    # # Len item
    res = requests.get(
        url="http://127.0.0.1:5000/dblen", params={"dbpath": "azz", "key": "chiave"},
    )
    print("len", serializer.decode(res.content), type(res.content))
    print()

    # # Del item
    res = requests.delete(
        url="http://127.0.0.1:5000/delitem", params={"dbpath": "azz", "key": f"key{0}"},
    )

    print("delitem", res.content, type(res.content))
    print()

    # # Set item
    res = requests.post(
        url="http://127.0.0.1:5000/setitem",
        data=serializer.encode(np.random.rand(100)),
        params={"dbpath": "azz", "key": f"key{0}"},
    )
    print("setitem", res.content, type(res.content))
    print()

    # # Get item
    res = requests.get(
        url="http://127.0.0.1:5000/getitem", params={"dbpath": "azz", "key": f"key{0}"},
    )
    print("getitem", bool(res.text), type(res.content))
    if res.content:
        print(
            "\t",
            serializer.decode(res.content).shape,
            type(serializer.decode(res.content)),
        )
    print()

    # # Repr db
    res = requests.get(
        url="http://127.0.0.1:5000/repr", params={"dbpath": "azz", "classname": "Oh"},
    )
    print("repr", res.text, type(res.content))
