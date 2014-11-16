#! /usr/bin/env python
# -*- coding: utf-8 -*-
# pylint: disable=too-many-public-methods
from __future__ import absolute_import

import logging
import unittest

from csm.primary_key import PrimaryKey
from test.utils import mock_row


logger = logging.getLogger(__name__)  # pylint: disable=invalid-name

_INT_VALIDATOR = 'org.apache.cassandra.db.marshal.Int32Type'
_REVERSED_INT_VALIDATOR = ('org.apache.cassandra.db.marshal.ReversedType({})'
                           .format(_INT_VALIDATOR))


class FromSchemaColumnsTests(unittest.TestCase):
    ''' Tests for :py:meth:`csm.primary_key.PrimaryKey.from_schema_columns` '''
    def setUp(self):
        self.single_partition = [
            self._mock_schema_column('a', 0, _INT_VALIDATOR)
        ]
        self.double_partition = [
            self._mock_schema_column('a', 0, _INT_VALIDATOR),
            self._mock_schema_column('b', 1, _INT_VALIDATOR),
        ]
        self.double_partition_reversed = [
            self._mock_schema_column('a', 1, _INT_VALIDATOR),
            self._mock_schema_column('b', 0, _INT_VALIDATOR),
        ]
        self.single_clustering = [
            self._mock_schema_column('a', 0, _INT_VALIDATOR),
            self._mock_schema_column('b', 0, _INT_VALIDATOR,
                                     is_partition=False),
        ]
        self.double_clustering = [
            self._mock_schema_column('a', 0, _INT_VALIDATOR),
            self._mock_schema_column('b', 0, _INT_VALIDATOR,
                                     is_partition=False),
            self._mock_schema_column('c', 1, _INT_VALIDATOR,
                                     is_partition=False),
        ]
        self.double_clustering_reversed = [
            self._mock_schema_column('a', 0, _INT_VALIDATOR),
            self._mock_schema_column('b', 1, _INT_VALIDATOR,
                                     is_partition=False),
            self._mock_schema_column('c', 0, _INT_VALIDATOR,
                                     is_partition=False),
        ]
        self.clustering_order = [
            self._mock_schema_column('a', 0, _INT_VALIDATOR),
            self._mock_schema_column('b', 0, _INT_VALIDATOR,
                                     is_partition=False),
            self._mock_schema_column('c', 1, _REVERSED_INT_VALIDATOR,
                                     is_partition=False),
        ]

    def test_partition_key(self):
        ''' Test partition key parsing '''
        for rows, expected_partition in (
                (self.single_partition, ["a"]),
                (self.double_partition, ["a", "b"]),
                (self.double_partition_reversed, ["b", "a"]),
                (self.single_clustering, ["a"]),
                (self.double_clustering, ["a"]),
                (self.double_clustering_reversed, ["a"]),
                (self.clustering_order, ["a"]),
        ):
            key = PrimaryKey.from_schema_columns(rows)
            self.assertEqual(
                key.partition_key, expected_partition,
                msg=("Wrong partition key. Got {}, expected {}, locals={}"
                     .format(key.partition_key, expected_partition, locals())))

    def test_clustering_key(self):
        ''' Test clustering key parsing '''
        for rows, expected_clustering in (
                (self.single_partition, []),
                (self.double_partition, []),
                (self.double_partition_reversed, []),
                (self.single_clustering, ["b"]),
                (self.double_clustering, ["b", "c"]),
                (self.double_clustering_reversed, ["c", "b"]),
                (self.clustering_order, ["b", "c"]),
        ):
            key = PrimaryKey.from_schema_columns(rows)
            self.assertEqual(
                key.clustering_key, expected_clustering,
                msg=("Wrong clustering key. Got {}, expected {}, locals={}"
                     .format(key.clustering_key, expected_clustering, locals())
                    ))

    def test_clustering_order(self):
        ''' Test clustering key order '''
        for rows, expected_order in (
                (self.single_partition, {}),
                (self.double_partition, {}),
                (self.double_partition_reversed, {}),
                (self.single_clustering, {"b": "ASC"}),
                (self.double_clustering, {"b": "ASC" , "c": "ASC"}),
                (self.double_clustering_reversed, {"c": "ASC", "b": "ASC"}),
                (self.clustering_order, {"b": "ASC", "c": "DESC"}),
        ):
            key = PrimaryKey.from_schema_columns(rows)
            self.assertEqual(
                key.clustering_order(), expected_order,
                msg=("Wrong clustering order. Got {}, expected {}, locals={}"
                     .format(key.clustering_order(), expected_order, locals())))

    def test_no_partition_fails(self):
        ''' Test that parsing fails when no partition key is present '''
        rows = [
            self._mock_schema_column("a", 0, _INT_VALIDATOR, is_partition=False)
        ]
        self.assertRaises(ValueError,
                          lambda: PrimaryKey.from_schema_columns(rows))

    def _mock_schema_column(self, column, component_index, validator,
                            is_partition=True, keyspace='keyspace',
                            table='table'):
        return mock_row(
            keyspace_name=keyspace,
            columnfamily_name=table,
            column_name=column,
            type="partition_key" if is_partition else "clustering_key",
            component_index=component_index,
            validator=validator,
        )

class PartitionKeyTests(unittest.TestCase):
    ''' Tests for :py:class:`csm.primary_key.PrimaryKey` . '''
    def test_full_key(self):
        ''' Test :py:meth:`csm.primary_key.PrimaryKey.full_key` method '''
        for partition, clustering, expected_full in (
                (["a"], [], [["a"]]),
                (["a"], ["b"], [["a"], "b"]),
                (["a"], ["b", "c"], [["a"], "b", "c"]),
                (["a", "b"], [], [["a", "b"]]),
                (["a", "b"], ["b"], [["a", "b"], "b"]),
                (["a", "b"], ["b", "c"], [["a", "b"], "b", "c"]),):
            key = PrimaryKey(partition, clustering)
            self.assertEqual(
                key.full_key(), expected_full,
                msg=("Wrong full key. Got {}, expected {}. locals={}".format(
                    key.full_key(), expected_full, locals())))

