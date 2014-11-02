#! /usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import

from cassandra.cluster import Cluster
import json
import logging
import random


logger = logging.getLogger(__name__) #pylint: disable=invalid-name


TYPES = [
    'ascii',
    'bigint',
    'blob',
    'boolean',
    'counter',
    'decimal',
    'double',
    'float',
    'inet',
    'int',
    'list<int>',
    'map<int,int>',
    'set<int>',
    'text',
    'timestamp',
    'uuid',
    'timeuuid',
    'varchar',
    'varint',
]
KEYSPACE = "test"
TABLE = "test_validator_extractor_%s" % random.randint(0, 1000)


def main():
    logging.basicConfig(level=logging.INFO)
    logging.info("Using table: %s,%s", KEYSPACE, TABLE)
    session = Cluster(["127.0.0.1"]).connect(KEYSPACE)
    validators = {}
    for val in TYPES:
        logger.info("Creating table for '%s'", val)
        session.execute("CREATE TABLE {} (key int primary key, value {})"
                        .format(TABLE, val))
        row = session.execute("SELECT * FROM system.schema_columns WHERE "
                              "keyspace_name = '{}' AND columnfamily_name = "
                              "'{}' AND column_name = 'value'"
                              .format(KEYSPACE, TABLE))[0]
        validator = row.validator
        logging.info("validator for '%s' = '%s'", val, validator)
        logger.info("Dropping table...")
        session.execute("DROP TABLE {}".format(TABLE))
        validators[val] = validator
    print json.dumps(validators, indent=1, sort_keys=True)


if __name__ == '__main__':
    main()
