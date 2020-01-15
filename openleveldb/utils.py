from typing import Iterable

import plyvel


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
