"""
Class that exposes the leveldb through REST API, with automatic serialization
to bytes and deserialization from bytes provided by the serialization module
"""
import http
import os
import pickle
import sys
from multiprocessing import Process
from pathlib import Path
from typing import Iterable, Tuple, Union

import plyvel
from flask import Flask, Response, g, request
from openleveldb.backend import serializer
from openleveldb.backend.connectorcommon import get_prefixed_db
from openleveldb.backend.serializer import DecodeType

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


def _parse_and_get_prefixed_db() -> plyvel.DB:
    dbpath = request.args.get("dbpath")
    prefixes = request.args.getlist("prefixes")
    prefixes = (
        serializer.normalize_strings(DecodeType.STR.pure_encode_fun, prefixes)
        if prefixes is not None
        else ()
    )
    return get_prefixed_db(get_db(dbpath), prefixes)


@app.teardown_appcontext
def close_db(error) -> None:
    """Closes the database again at the end of the request."""
    if hasattr(g, "dbs"):
        for x, y in g.dbs.items():
            if hasattr(y, "close"):
                y.close()


@app.route("/iterator", methods=["GET"])
def iterator() -> Iterable[Union[bytes, Tuple[bytes, bytes]]]:
    db = _parse_and_get_prefixed_db()

    starting_by = request.args.get("starting_by")
    starting_by = b"".join(
        serializer.normalize_strings(DecodeType.STR.pure_encode_fun, starting_by)
        if starting_by is not None
        else ()
    )

    include_key = request.args.get("include_key") == "True"
    include_value = request.args.get("include_value") == "True"

    out = pickle.dumps(
        list(
            db.iterator(
                prefix=starting_by, include_key=include_key, include_value=include_value
            )
        )
    )
    return Response(out, content_type="application/octet-stream")


@app.route("/dblen", methods=["GET"])
def dblen() -> str:
    db = _parse_and_get_prefixed_db()

    starting_by = request.args.get("starting_by")
    starting_by = b"".join(
        serializer.normalize_strings(DecodeType.STR.pure_encode_fun, starting_by)
        if starting_by is not None
        else ()
    )
    out = serializer.encode(
        sum(
            1
            for _ in db.iterator(
                include_key=True, include_value=False, prefix=starting_by
            )
        )
    )
    return Response(out, content_type="application/octet-stream")


@app.route("/setitem", methods=["POST"])
def setitem() -> Response:
    db = _parse_and_get_prefixed_db()
    key = request.args.get("key")
    value = request.get_data()
    keybytes = DecodeType.STR.pure_encode_fun(key)

    db.put(keybytes, value)

    return Response(key, content_type="text")


@app.route("/getitem", methods=["GET"])
def getitem() -> Response:
    db = _parse_and_get_prefixed_db()
    key = request.args.get("key")
    keybytes = DecodeType.STR.pure_encode_fun(key)
    out = db.get(keybytes, default=b"")
    return Response(out, content_type="application/octet-stream")


@app.route("/delitem", methods=["DELETE"])
def delitem() -> (str, http.HTTPStatus):
    db = _parse_and_get_prefixed_db()

    key = request.args.get("key")
    keybytes = DecodeType.STR.pure_encode_fun(key)
    db.delete(keybytes)

    return Response(key, content_type="text")


@app.route("/repr", methods=["GET"])
def repr() -> str:
    db = _parse_and_get_prefixed_db()

    dbpath = request.args.get("dbpath")
    classname = request.args.get("classname")
    innerdb = f"{db}"

    dbrepr = f"{classname}(path='{dbpath}', db={innerdb})"
    return Response(dbrepr, content_type="text")


def dummy_server(port: Union[int, str]) -> Process:
    port = int(port)

    def runflask() -> None:
        sys.stdout = open(os.devnull, "w")
        sys.stderr = open(os.devnull, "w")
        app.run(port=port)

    dummy_server = Process(target=runflask)
    dummy_server.start()
    return dummy_server


if __name__ == "__main__":
    pass
