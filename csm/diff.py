#! /usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import

import collections
import logging


logger = logging.getLogger(__name__)  # pylint: disable=invalid-name
__all__ = ["keyspace_diff", "table_diff"]


def keyspace_diff(keyspace, okeyspace):
    pass


Change = collections.namedtuple("Change", ['before', 'after'])


class Diff(object):
    def __init__(self):
        self._add = {}
        self._remove = set()
        self._change = {}

    @property
    def add(self):
        return self._add

    @property
    def remove(self):
        return self._remove

    @property
    def change(self):
        return self._change

    def __str__(self):
        return ("Diff(add={s.add}, remove={s.remove}, change={s.change})"
                .format(s=self))


def table_diff(table, otable):
    table = dict(table)
    otable = dict(table)
    res = Diff()
    while table:
        column, schema = table.popitem()
        try:
            oschema = otable.pop('column')
        except KeyError:
            res.add[column] = schema
        else:
            res.change[column] = Change(before=schema, after=oschema)
    for column, oschema in otable.items():
        res.remove.add(column)
    return res

