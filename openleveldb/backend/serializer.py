"""
Automatic conversion to and from bytes
"""
import base64
import collections
import functools
import json
import typing
from enum import Enum
from io import BytesIO
from typing import Any, Callable, Iterable, Union

import numpy as np
import rapidjson


class Serializer:
    @staticmethod
    def ndarray_frombytes(blob: bytes) -> np.ndarray:
        """
        Convert the bytes representation of the array back into a numpy array.

        Use BytesIO to convert the binary version of the array back into a numpy array.

        :param blob: BLOB containing a NumPy array
        :returns: One steaming hot NumPy array
        """
        out = BytesIO(blob)
        out.seek(0)

        return np.load(out)

    @staticmethod
    def ndarray_tobytes(array: np.ndarray) -> bytes:
        """
        Convert the numpy array into bytes

        Use the numpy.save function to save a binary version of the array,
        and BytesIO to catch the stream of data and convert it into a BLOB.

        :param array: NumPy array to turn into bytes
        :returns: NumPy array as BLOB
        """
        out = BytesIO()
        np.save(out, array)
        out.seek(0)

        return out.read()

    @staticmethod
    def bytes_to_string(blob: bytes) -> str:
        """
        Convert a numpy array into an ascii representation

        Using the numpy.save function to save a binary version of the array,
        and BytesIO to catch the stream of data and convert it into a BLOB.
        Then convert the BLOB in a base64 ascii representation.

        :param obj: NumPy array to turn into bytes
        :returns: NumPy array as an ascii string
        """
        byte_string = base64.b64encode(blob)
        ndarray_string = byte_string.decode("ascii")
        return ndarray_string

    @staticmethod
    def string_to_bytes(obj: str) -> bytes:
        """
        Convert the ascii representation of the array back into a numpy array.

        :param obj: ascii string containing a NumPy array
        :returns: One steaming hot NumPy array
        """
        blob = base64.b64decode(obj)
        return blob


class DecodeError(BaseException):
    pass


class DecodeType(Enum):
    """
    Enum that establish how each object should be serialized into bytes and de-serialized.
    Perform automatic decode adding an identifier to the byte representation of the objects

    Changing this class may yield compatibility issues with old data.
    """

    @staticmethod
    def _id_length() -> int:
        """
        The expected length of the identifier that will be concatenated to the bytes representation.
        Changing this return value will break old data

        :return: the identifier length
        """
        return 1

    def __new__(
        cls,
        id: bytes,
        encode_fun: Callable[[Any], bytes],
        decode_fun: Callable[[bytes], Any],
    ) -> "DecodeType":
        """
        Custom constructor for enum constants.
        The constants are indexed only by the identifier, but the have other attributes

        :param id: the identifier that will be concatenated to the byte representation
        :param encode_fun: the function to use to convert objects into bytes
        :param decode_fun: the function to use to convert bytes into objects
        """
        if len(id) != cls._id_length():
            raise TypeError(f"unsupported {len(id)}-byte identifier '{id}'")

        obj = object.__new__(cls)
        obj._value_ = obj.identifier = id
        obj.pure_encode_fun = encode_fun
        obj.pure_decode_fun = decode_fun
        return obj

    def encode(self, obj: Any) -> bytes:
        """
        Encode method, available for every constants.
        Encodes the object into bytes and concatenates the identifier

        :param obj: the object to encode
        :return: the encoded object
        """
        return self.identifier + self.pure_encode_fun(obj)

    @staticmethod
    def decode(blob: bytes) -> Any:
        """
        Decode method, available for every constants.
        Decodes the bytes blob into the object, uses the identifier contained in the blob to select the right decoder.

        :param blob: the blob to decode
        :return: the decoded object
        """
        idlen = DecodeType._id_length()
        enc, obj = blob[:idlen], blob[idlen:]
        try:
            return DecodeType(enc).pure_decode_fun(obj)
        except json.decoder.JSONDecodeError as ex:
            raise ex
        except ValueError as ex:
            raise DecodeError(f"missing DecodeType identifier in bytes blob") from ex

    BYTES = (b"b", lambda x: x, lambda x: x)
    INT = (
        b"i",
        functools.partial(int.to_bytes, length=16, byteorder="big", signed=True),
        functools.partial(int.from_bytes, byteorder="big", signed=True),
    )
    STR = (
        b"s",
        functools.partial(str.encode, encoding="utf-8"),
        functools.partial(bytes.decode, encoding="utf-8"),
    )
    JSON = (
        b"j",
        lambda x: str.encode(rapidjson.dumps(x), encoding="utf-8"),
        lambda x: rapidjson.loads(bytes.decode(x, encoding="utf-8")),
    )
    NUMPY = (
        b"n",
        Serializer.ndarray_tobytes,
        Serializer.ndarray_frombytes,
    )


# todo: use singledispatchmethod and move inside the enum in python 3.8
@functools.singledispatch
def encode(obj: Any) -> bytes:
    """
    Utility method to select the correct DecodeType to perform the encoding, dispatching based on the obj type
    Encode the object into bytes, the encoding may change depending on the obj type

    It supports json serializable, string, int and np.ndarray

    :param obj: the object to encode
    :return: the encoded object
    """
    return DecodeType.JSON.encode(obj)


@encode.register
def _(obj: bytes) -> bytes:
    return DecodeType.BYTES.encode(obj)


@encode.register
def _(obj: int) -> bytes:
    return DecodeType.INT.encode(obj)


@encode.register
def _(obj: str) -> bytes:
    return DecodeType.STR.encode(obj)


@encode.register
def _(obj: np.ndarray) -> bytes:
    return DecodeType.NUMPY.encode(obj)


@functools.singledispatch
def decode(blob: Any) -> Any:
    """
    Utility method to expose DecodeType.decode

    Decode the bytes back into an object. The blob is expected to contain a type identifier in the first 4 bytes.
    The decoding may change depending on blob's identifier

    :param blob: the blob to decode
    :return: the decoded object
    """
    return blob


@decode.register
def _(blob: bytes) -> Any:
    return DecodeType.decode(blob)


def normalize_strings(
    key_encoder: Callable[[str], bytes], strings: Union[str, typing.Sequence[str]]
) -> Iterable[bytes]:
    """
    Normalize an iterable of strings to the correspondent iterable of
    bytes-encoded strings. It always returns an Iterable

    :param strings: the string or iterable of strings to encode
    :param key_encoder: the encoder to use to encode the strings
    :returns: the encoded strings
    """
    if strings is None:
        yield None

    if strings is Ellipsis:
        yield strings

    if isinstance(strings, bytes):
        raise TypeError(f"str prefix or key expected, got {type(strings).__name__}")

    if isinstance(strings, str):
        strings = [strings]

    if not isinstance(strings, collections.abc.Sequence):
        raise TypeError(f"str prefix or key expected, got {type(strings).__name__}")

    num_strings = len(strings)

    for i, s in enumerate(strings):
        if i == num_strings - 1 and s is Ellipsis:
            yield s
        elif not isinstance(s, str):
            raise TypeError(f"str prefix or key expected, got {type(s).__name__}")
        else:
            yield key_encoder(s)


if __name__ == "__main__":
    import doctest

    doctest.testmod()
