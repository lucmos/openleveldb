import base64

import numpy as np
import orjson
import pytest
from openleveldb.serializer import decode, encode


class TestSerializer:
    def test_int_encoding_decoding(self) -> None:
        a = 42
        a_encoded = encode(a)
        assert a_encoded[:1] == b"i"
        assert base64.b64encode(a_encoded) == b"aQAAAAAAAAAAAAAAAAAAACo="
        b = decode(a_encoded)
        assert b == 42
        assert a == b

    def test_str_encoding_decoding(self) -> None:
        a = "test in progress... :)"
        a_encoded = encode(a)
        assert a_encoded[:1] == b"s"
        assert a_encoded == b"stest in progress... :)"
        b = decode(a_encoded)
        assert b == "test in progress... :)"
        assert a == b

    def test_json_encoding_decoding(self) -> None:
        a = {"test1": 3.0, "test2": {"inner": 0}}
        a_encoded = encode(a)
        assert a_encoded[:1] == b"j"
        assert a_encoded == b'j{"test1":3.0,"test2":{"inner":0}}'
        b = decode(a_encoded)
        assert b == {"test1": 3.0, "test2": {"inner": 0}}
        assert a == b

    def test_numpy_encoding_decoding(self) -> None:
        a = np.array([[0, 1.13], [1, 42]])
        a_encoded = encode(a)
        assert a_encoded[:1] == b"n"
        assert (
            base64.b64encode(a_encoded)
            == b"bpNOVU1QWQEAdgB7J2Rlc2NyJzogJzxmOCcsICdmb3J0cmFuX29yZGVyJz"
            b"ogRmFsc2UsICdzaGFwZSc6ICgyLCAyKSwgfSAgICAgICAgICAgICAgICAgI"
            b"CAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAKAAAAA"
            b"AAAAAAUrkfhehTyPwAAAAAAAPA/AAAAAAAARUA="
        )
        b = decode(a_encoded)

        assert repr(b) == repr(a)
        assert np.array_equal(a, b)
