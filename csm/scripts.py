#! /usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import

import argparse
import json
import logging

from .columns import get_system_columns


logger = logging.getLogger(__name__)  # pylint: disable=invalid-name


def _get_csm_argument_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--server",
                        action="store", default='localhost',
                        help=("Server to retrieve schema from. Defaults to "
                              "localhost"))
    return parser


def csm():
    options = _get_csm_argument_parser().parse_args()
    print "Schema for '{}'".format(options.server)
    columns = get_system_columns(options.server)
    print json.dumps(columns, indent=1)
