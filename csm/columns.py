#! /usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import

from cassandra.cluster import Cluster
import collections
import logging
import re


logger = logging.getLogger(__name__)  # pylint: disable=invalid-name
__all__ = ["get_system_columns"]

_MARSHAL_PACKAGE = "org.apache.cassandra.db.marshal"
_PRE_BASIC_VALIDATORS_BY_TYPE = {
    "ascii": "AsciiType",
    "bigint": "LongType",
    "blob": "BytesType",
    "boolean": "BooleanType",
    "counter": "CounterColumnType",
    "decimal": "DecimalType",
    "double": "DoubleType",
    "float": "FloatType",
    "inet": "InetAddressType",
    "int": "Int32Type",
    "text": "UTF8Type",
    "timestamp": "TimestampType",
    "timeuuid": "TimeUUIDType",
    "uuid": "UUIDType",
    "varchar": "UTF8Type",
    "varint": "IntegerType"
}
_BASIC_VALIDATORS_BY_TYPE = {
    k: "{}.{}".format(_MARSHAL_PACKAGE, v)
    for k, v in _PRE_BASIC_VALIDATORS_BY_TYPE.items()
}
_BASIC_TYPES_BY_VALIDATOR = {
    v: k for k, v in _BASIC_VALIDATORS_BY_TYPE.items()
}
_PRE_COLLECTION_VALIDATORS_BY_TYPE = {
    "list": "ListType",
    "map": "MapType",
    "set": "SetType",
}
_COLLECTION_VALIDATORS_BY_TYPE = {
    k: "{}.{}".format(_MARSHAL_PACKAGE, v)
    for k, v in _PRE_COLLECTION_VALIDATORS_BY_TYPE.items()
}
_COLLECTION_TYPES_BY_VALIDATOR = {
    v: k for k, v in _COLLECTION_VALIDATORS_BY_TYPE.items()
}


class _Tree(collections.defaultdict):
    def __init__(self, *args, **kwargs):
        super(_Tree, self).__init__(_Tree, *args, **kwargs)


def get_system_columns(server=None):
    rows = _get_system_column_rows(server=server)
    return _schema_from_rows(rows)


def _get_system_column_rows(server=None):
    server = server or "127.0.0.1"
    logging.info("Getting server columns from server: '%s'", server)
    session = Cluster([server]).connect()
    return session.execute("SELECT * FROM system.schema_columns")


def _schema_from_rows(rows):
    res = _Tree()
    for row in rows:
        res[row.keyspace_name][row.columnfamily_name][row.column_name] = (
            _cqltype_from_validator(row.validator)
        )
    return res


def _cqltype_from_validator(validator):
    logger.debug("Parsing cqltype from validator: '%s'", validator)
    split = re.split("[()]", validator)
    base_validator = split[0]
    logger.debug("Base validator: '%s'", base_validator)
    try:
        return _BASIC_TYPES_BY_VALIDATOR[base_validator]
    except KeyError as err:
        pass

    try:
        base_type = _COLLECTION_TYPES_BY_VALIDATOR[base_validator]
    except KeyError as err:
        raise ValueError("Invalid cql validator: {}".format(err))

    try:
        logger.debug("Split: '%s'", split)
        subvalidators = split[1]
    except IndexError:
        raise ValueError("Incomplete validator, expected subvalidators: '{}'"
                         .format(validator))

    try:
        subtypes = [_BASIC_TYPES_BY_VALIDATOR[sv]
                    for sv in subvalidators.split(",")]
    except KeyError as err:
        raise ValueError("Invalid basic validator: '{}'".format(err))

    return "{}<{}>".format(base_type, ",".join(subtypes))
