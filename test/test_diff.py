#! /usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import

import logging
import unittest

from csm.diff import table_diff, keyspace_diff


logger = logging.getLogger(__name__)  # pylint: disable=invalid-name


class TableDiffTests(unittest.TestCase):
    def setUp(self):
        self.table_a = {
            "a": "timestamp",
        }
        self.table_b = {
            "a": "timestamp",
            "b": "int",
        }
        self.table_c = {
            "a" : "int",
            "b" : "timestamp",
        }
        self.table_d = {
            "a": "text",
            "b": "text",
            "c": "int",
        }
        self.empty_table = {}


    def test_table_diff_add(self):
        self._test_table_diff_add(self.table_a, self.empty_table,
                                  {"a": "timestamp"})

        self._test_table_diff_add(self.table_b, self.table_a, {"b": "int"})
        self._test_table_diff_add(self.table_c, self.table_b, {})
        self._test_table_diff_add(self.table_d, self.table_c, {"c", "int"})

    def test_table_diff(self):
        self._test_table_diff_remove(self.empty_table, self.table_a, {"a"})
        self._test_table_diff_remove(self.table_a, self.table_b, {"b"})
        self._test_table_diff_remove(self.table_b, self.table_c, set())
        self._test_table_diff_remove(self.table_c, self.table_d, {"c"})

    def test_table_rename(self):
        self.assertFalse(table_diff(self.empty_table, self.table_a).change)
        self.assertFalse(table_diff(self.table_a, self.table_b).change)
        change = table_diff(self.table_b, self.table_c).change
        self._test_change(change["a"], "timestamp", "int")
        self._test_change(change["b"], "int", "timestamp")
        self.assertEqual(len(change), 2)
        change = table_diff(self.table_c, self.table_d).change
        self._test_change(change["a"], "int", "text")
        self._test_change(change["b"], "timestamp", "text")
        self.assertEqual(len(change), 2)

    def _test_table_diff_add(self, pre, post, add):
        self.assertDictEqual(table_diff(pre, post).add, add)

    def _test_table_diff_remove(self, pre, post, remove):
        self.assertDictEqual(table_diff(pre, post).remove, remove)

    def _test_change(self, change, before, after):
        self.assertEqual(change.before, before)
        self.assertEqual(change.after, after)
