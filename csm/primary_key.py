#! /usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import

import logging

from .columns import parse_validator


logger = logging.getLogger(__name__)  # pylint: disable=invalid-name


class PrimaryKey(object):
    def __init__(self, partition_key, clustering_key,
                 reversed_clustering_keys=None):
        if not partition_key:
            raise ValueError("Primary key must have a non empty partition key")
        self._partition_key = partition_key
        self._clustering_key = clustering_key
        self._reversed_clustering_keys = set(reversed_clustering_keys or [])

    @classmethod
    def from_schema_columns(cls, rows):
        tables = set((r.keyspace_name, r.columnfamily_name) for r in rows)
        if len(tables) > 1:
            raise ValueError("The rows supplied represent more than one table: "
                             "{}".format(tables))
        sorted_rows = sorted(rows, key=lambda r: r.component_index)
        partition_key = [r.column_name for r in sorted_rows
                         if r.type == 'partition_key']
        clustering_key = [r.column_name for r in sorted_rows
                          if r.type == 'clustering_key']
        reversed_keys = [r.column_name for r in sorted_rows
                         if r.type == 'clustering_key' and
                         parse_validator(r.validator).is_reversed]
        return cls(partition_key, clustering_key, reversed_keys)

    @property
    def partition_key(self):
        return list(self._partition_key)

    @property
    def clustering_key(self):
        return list(self._clustering_key)

    def clustering_order(self):
        return {k: "DESC" if k in self._reversed_clustering_keys else "ASC"
                for k in self.clustering_key}

    def full_key(self):
        full_key = [self._partition_key]
        full_key.extend(self.clustering_key)
        return full_key


