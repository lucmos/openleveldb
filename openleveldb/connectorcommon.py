import random
import uuid
from typing import Iterable

import plyvel


class RandomUuidGenerator:
    """Generates a sequence of pseudo-random UUIDs.

    Given the same seed, it will generate the same sequence.
    """

    def __init__(self, seed: int) -> None:
        self.rng = random.Random(seed)

    def gen_uuid(self) -> uuid.UUID:
        return uuid.UUID(
            bytes=bytes(self.rng.getrandbits(8) for _ in range(16)), version=4
        )


a = RandomUuidGenerator(0)

BYTES_DELIMITER = a.gen_uuid().bytes


def get_prefixed_db(db: plyvel.DB, prefixes: Iterable[bytes]) -> plyvel.DB:
    """
    Apply all the prefixes (last one included) to obtain the desired prefixed database

    :param db: the initial database
    :param prefixes: the prefix or the iterable of prefixes to apply
    :returns: the prefixed database
    """

    for prefix in prefixes:
        if prefix is Ellipsis:
            raise TypeError(f"str prefix or key expected, got {type(prefix).__name__}")
        db = db.prefixed_db(prefix)

    return db
