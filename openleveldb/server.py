import http
import os
import pickle
import sys
from multiprocessing import Process
from pathlib import Path
from typing import Iterable, Tuple, Union

import plyvel
from flask import Flask, Response, g, request
from openleveldb import serializer
from openleveldb.serializer import DecodeType, decode
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


@app.route("/iterator", methods=["GET"])
def iterator() -> Iterable[Union[bytes, Tuple[bytes, bytes]]]:
    dbpath = request.args.get("dbpath")
    prefixes = request.args.getlist("prefixes")
    prefixes = (
        serializer.normalize_strings(DecodeType.STR.pure_encode_fun, prefixes)
        if prefixes is not None
        else ()
    )
    starting_by = request.args.get("starting_by")
    starting_by = b"".join(
        serializer.normalize_strings(DecodeType.STR.pure_encode_fun, starting_by)
        if starting_by is not None
        else ()
    )

    include_key = request.args.get("include_key")
    include_value = request.args.get("include_value")
    print(include_key)
    conv = lambda x: True if x == "True" else False
    include_key = conv(include_key)
    include_value = conv(include_value)
    db = get_prefixed_db(get_db(dbpath), prefixes)
    out = pickle.dumps(
        list(
            db.iterator(
                prefix=starting_by, include_key=include_key, include_value=include_value
            )
        )
    )
    return Response(out, content_type="application/octet-stream")


@app.route("/prefixed_dblen", methods=["GET"])
def prefixed_dblen() -> str:
    dbpath = request.args.get("dbpath")
    prefixes = request.args.getlist("prefixes")
    prefixes = (
        serializer.normalize_strings(DecodeType.STR.pure_encode_fun, prefixes)
        if prefixes is not None
        else ()
    )
    starting_by = request.args.get("starting_by")
    starting_by = b"".join(
        serializer.normalize_strings(DecodeType.STR.pure_encode_fun, starting_by)
        if starting_by is not None
        else ()
    )
    db = get_prefixed_db(get_db(dbpath), prefixes)
    out = serializer.encode(
        sum(
            1
            for _ in db.iterator(
                include_key=True, include_value=False, prefix=starting_by
            )
        )
    )
    return Response(out, content_type="application/octet-stream")


@app.route("/dblen", methods=["GET"])
def dblen() -> str:
    dbpath = request.args.get("dbpath")

    db = get_db(dbpath)
    out = serializer.encode(
        sum(1 for _ in db.iterator(include_key=True, include_value=False))
    )

    return Response(out, content_type="application/octet-stream")


@app.route("/setitem", methods=["POST"])
def setitem() -> Response:
    dbpath = request.args.get("dbpath")
    key = request.args.get("key")
    value = request.get_data()
    keybytes = DecodeType.STR.pure_encode_fun(key)

    prefixes = request.args.getlist("prefixes")
    prefixes = serializer.normalize_strings(DecodeType.STR.pure_encode_fun, prefixes)
    db = get_prefixed_db(get_db(dbpath), prefixes)

    db.put(keybytes, value)

    return Response(key, content_type="text")


@app.route("/getitem", methods=["GET"])
def getitem() -> Response:
    dbpath = request.args.get("dbpath")
    key = request.args.get("key")
    keybytes = DecodeType.STR.pure_encode_fun(key)

    prefixes = request.args.getlist("prefixes")
    prefixes = serializer.normalize_strings(DecodeType.STR.pure_encode_fun, prefixes)
    db = get_prefixed_db(get_db(dbpath), prefixes)

    out = db.get(keybytes, default=b"")
    return Response(out, content_type="application/octet-stream")


@app.route("/delitem", methods=["DELETE"])
def delitem() -> (str, http.HTTPStatus):
    dbpath = request.args.get("dbpath")
    key = request.args.get("key")
    keybytes = DecodeType.STR.pure_encode_fun(key)

    prefixes = request.args.getlist("prefixes")
    prefixes = serializer.normalize_strings(DecodeType.STR.pure_encode_fun, prefixes)
    db = get_prefixed_db(get_db(dbpath), prefixes)

    db.delete(keybytes)

    return Response(key, content_type="text")


@app.route("/repr", methods=["GET"])
def repr() -> str:
    dbpath = request.args.get("dbpath")
    classname = request.args.get("classname")

    prefixes = request.args.getlist("prefixes")
    prefixes = serializer.normalize_strings(DecodeType.STR.pure_encode_fun, prefixes)
    db = get_prefixed_db(get_db(dbpath), prefixes)

    innerdb = f"{db}"

    dbrepr = f"{classname}(path='{dbpath}', db={innerdb})"
    return Response(dbrepr, content_type="text")


def run(port: int) -> None:
    app.run(port=port)


if __name__ == "__main__":
    import requests
    import numpy as np

    # # # Get item
    # starting_by = None
    # include_key = True
    # include_value = True
    # prefixes = []  # ["key"]
    # res = requests.get(
    #     url="http://127.0.0.1:5000/iterator",
    #     params={
    #         "dbpath": "azz",
    #         "key": f"key{0}",
    #         "include_key": include_key,
    #         "include_value": include_value,
    #         "starting_by": starting_by,
    #         "prefixes": prefixes,
    #     },
    # )
    # out = pickle.loads(res.content)
    #
    # for z in out:
    #     print(z)

    # # # Get prefixed db
    # res = requests.get(
    #     url="http://127.0.0.1:5000/get_prefixed_db_path",
    #     params={"prefixes": ["prefix1", "prefix2", "prefix3", "key"], "dbpath": "azz",},
    # )
    #
    # print("prefixed", res.content, type(res.content))
    # print()

    # # # Len item
    # res = requests.get(
    #     url="http://127.0.0.1:5000/dblen", params={"dbpath": "azz", "key": "chiave"},
    # )
    # print("len", serializer.decode(res.content), type(res.content))
    # print()
    #
    # # # Del item
    # res = requests.delete(
    #     url="http://127.0.0.1:5000/delitem", params={"dbpath": "azz", "key": f"key{0}"},
    # )
    #
    # print("delitem", res.content, type(res.content))
    # print()
    #
    # # # Set item
    # res = requests.post(
    #     url="http://127.0.0.1:5000/setitem",
    #     data=serializer.encode(np.random.rand(100)),
    #     params={"dbpath": "azz", "key": f"key{0}"},
    # )
    # print("setitem", res.content, type(res.content))
    # print()
    #
    # # # Get item
    # res = requests.get(
    #     url="http://127.0.0.1:5000/getitem", params={"dbpath": "azz", "key": f"key{0}"},
    # )
    # print("getitem", bool(res.text), type(res.content))
    # if res.content:
    #     print(
    #         "\t",
    #         serializer.decode(res.content).shape,
    #         type(serializer.decode(res.content)),
    #     )
    # print()
    #
    # # # Repr db
    # res = requests.get(
    #     url="http://127.0.0.1:5000/repr", params={"dbpath": "azz", "classname": "Oh"},
    # )
    # print("repr", res.text, type(res.content))
