#! /usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import

import json
import logging

from .columns import get_system_columns


logger = logging.getLogger(__name__)  # pylint: disable=invalid-name


def csm():
    print "Current system columns:"
    columns = get_system_columns()
    print json.dumps(columns, indent=1)
