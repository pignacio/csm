#! /usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import

from nose.tools import eq_
import itertools
import logging

from .utils import mock_row
from csm.columns import _cqltype_from_validator, _schema_from_rows

logger = logging.getLogger(__name__) #pylint: disable=invalid-name

BASIC_VALIDATORS = {
    "ascii": "org.apache.cassandra.db.marshal.AsciiType",
    "bigint": "org.apache.cassandra.db.marshal.LongType",
    "blob": "org.apache.cassandra.db.marshal.BytesType",
    "boolean": "org.apache.cassandra.db.marshal.BooleanType",
    "counter": "org.apache.cassandra.db.marshal.CounterColumnType",
    "decimal": "org.apache.cassandra.db.marshal.DecimalType",
    "double": "org.apache.cassandra.db.marshal.DoubleType",
    "float": "org.apache.cassandra.db.marshal.FloatType",
    "inet": "org.apache.cassandra.db.marshal.InetAddressType",
    "int": "org.apache.cassandra.db.marshal.Int32Type",
    "text": "org.apache.cassandra.db.marshal.UTF8Type",
    "timestamp": "org.apache.cassandra.db.marshal.TimestampType",
    "timeuuid": "org.apache.cassandra.db.marshal.TimeUUIDType",
    "uuid": "org.apache.cassandra.db.marshal.UUIDType",
    "varint": "org.apache.cassandra.db.marshal.IntegerType"
}
COLLECTION_VALIDATORS = {
    "list": "org.apache.cassandra.db.marshal.ListType",
    "map": "org.apache.cassandra.db.marshal.MapType",
    "set": "org.apache.cassandra.db.marshal.SetType",
}


def _parse_validator(validator, expected_cqltype):
    eq_(_cqltype_from_validator(validator), expected_cqltype)


def test_parse_basic_column_types():
    for cqltype, validator in BASIC_VALIDATORS.items():
        yield _parse_validator, validator, cqltype


def test_parse_collection_types():
    def _make_validator(collection, *subtypes):
        return "{}({})".format(
            COLLECTION_VALIDATORS[collection],
            ",".join(BASIC_VALIDATORS[st] for st in subtypes)
        )

    for collection in ['set', 'list']:
        for cqltype in BASIC_VALIDATORS:
            yield (_parse_validator,
                   _make_validator(collection, cqltype),
                   "{}<{}>".format(collection, cqltype))

    for keytype, valuetype in itertools.product(BASIC_VALIDATORS,
                                                BASIC_VALIDATORS):
        yield (_parse_validator,
               _make_validator("map", keytype, valuetype),
               "map<{},{}>".format(keytype, valuetype))


def test_schema_from_rows():
    rows = [
        mock_row(keyspace_name='keyspace', columnfamily_name='table',
                 column_name='column',
                 validator='org.apache.cassandra.db.marshal.Int32Type')
    ]
    schema = _schema_from_rows(rows)
    eq_(schema['keyspace']['table']['column'], 'int')
