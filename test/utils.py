#! /usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import

import collections
import logging


logger = logging.getLogger(__name__)  # pylint: disable=invalid-name


def mock_row(**values):
    clz = collections.namedtuple('TestRow', values.keys())
    return clz(**values)
