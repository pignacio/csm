#! /usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import

from nose.tools import eq_
import itertools
import logging
import unittest

from .utils import mock_row
from csm.columns import parse_validator, _schema_from_rows


logger = logging.getLogger(__name__)  # pylint: disable=invalid-name


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


class ParseValidatorTests(unittest.TestCase):
    ''' Tests for :py:meth:`csm.columns.parse_validator` '''
    def _test_parse_validator(self, validator, cqltype, is_reversed):
        parsed = parse_validator(validator)
        self.assertEqual(
            parsed.cqltype, cqltype,
            msg=("Wrong cqltype. Got {}. Expected {}. Locals={}"
                 .format(parsed.cqltype, cqltype, locals())))
        self.assertEqual(
            parsed.is_reversed, is_reversed,
            msg=("Wrong is_reversed. Got {}. Expected {}. Locals={}"
                 .format(parsed.is_reversed, is_reversed, locals())))

    def test_parse_basic_column_types(self):
        for cqltype, validator in BASIC_VALIDATORS.items():
            self._test_parse_validator(validator, cqltype, is_reversed=False)

    def _make_collection_validator(self, collection, *subtypes):
        return "{}({})".format(
            COLLECTION_VALIDATORS[collection],
            ",".join(BASIC_VALIDATORS[st] for st in subtypes)
        )

    def test_parse_list_types(self):
        self._test_parse_collection_type("list")

    def test_parse_set_types(self):
        self._test_parse_collection_type("set")

    def _test_parse_collection_type(self, collection):
        for cqltype in BASIC_VALIDATORS:
            self._test_parse_validator(
                self._make_collection_validator(collection, cqltype),
                "{}<{}>".format(collection, cqltype),
                is_reversed=False,
            )

    def test_parse_map_types(self):
        for keytype, valuetype in itertools.product(BASIC_VALIDATORS,
                                                    BASIC_VALIDATORS):
            self._test_parse_validator(
                self._make_collection_validator("map", keytype, valuetype),
                "map<{},{}>".format(keytype, valuetype),
                is_reversed=False
            )


    def test_parse_reversed_types(self):
        def _reversed_validator(validator):
            return "org.apache.cassandra.db.marshal.ReversedType({})".format(
                validator
            )

        for cqltype, validator in BASIC_VALIDATORS.items():
            self._test_parse_validator(_reversed_validator(validator), cqltype,
                                       is_reversed=True)



def test_schema_from_rows():
    rows = [
        mock_row(keyspace_name='keyspace', columnfamily_name='table',
                 column_name='column',
                 validator='org.apache.cassandra.db.marshal.Int32Type')
    ]
    schema = _schema_from_rows(rows)
    eq_(schema['keyspace']['table']['column'], 'int')
