# -*- coding: utf-8 -*-

import unittest
import time

from uuid import uuid4, UUID
from cassandra.query import dict_factory
from caes.client import CassandraClient
from caes.test.utils import random_string


class CassandraClientTestCase(unittest.TestCase):
    def tearDown(self):
        query = """
            DROP KEYSPACE %s
        """ % self.keyspace

        session = self.cclient._cluster.connect(self.keyspace)
        session.execute(query)
        session.shutdown()

    def setUp(self):
        self.keyspace = random_string().lower()
        self.data_column_family = random_string().lower()
        self.cclient = CassandraClient(self.keyspace, self.data_column_family)

        session = self.cclient._cluster.connect()

        query = """
            CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
        """ % self.keyspace

        session.execute(query)
        session.shutdown()

        session = self.cclient._cluster.connect(self.keyspace)

        query = """
            CREATE TABLE %s (
              %s int,
              timestamp int,
              %s uuid,
              PRIMARY KEY(%s, timestamp)
            );
        """ % (self.cclient._timeseries_column_family,
               self.cclient._timeseries_id_field_name,
               self.cclient._data_id_field_name,
               self.cclient._timeseries_id_field_name)

        session.execute(query)

        query = """
            CREATE TABLE %s (
              %s uuid,
              vint int,
              vstring text,
              PRIMARY KEY(%s)
            );
        """ % (self.data_column_family,
               self.cclient._data_id_field_name,
               self.cclient._data_id_field_name)

        session.execute(query)
        session.shutdown()

    def test_write_doc(self):
        data = dict(vint=1, vstring="Hi")
        did = uuid4()
        timestamp = int(time.time())
        self.cclient.write([(data, did, timestamp)])

        self.cclient.flush()

        query = """
            SELECT *
            FROM %s
            WHERE did = ?
        """ % self.data_column_family

        session = self.cclient._cluster.connect(self.keyspace)
        session.row_factory = dict_factory
        prepared = session.prepare(query)
        results = session.execute(prepared, (did,))
        session.shutdown()

        self.assertNotEqual(len(results), 0)
        self.assertDictContainsSubset(data, results[0])

    def test_write_doc_none(self):
        did = uuid4()
        timestamp = int(time.time())
        self.cclient.write([(None, did, timestamp)])

        self.cclient.flush()

        query = """
            SELECT *
            FROM %s
            WHERE did = ?
        """ % self.data_column_family

        session = self.cclient._cluster.connect(self.keyspace)
        session.row_factory = dict_factory
        prepared = session.prepare(query)
        results = session.execute(prepared, (did,))
        session.shutdown()

        self.assertEqual(len(results), 0)

    def test_latest(self):
        session = self.cclient._cluster.connect(self.keyspace)

        params = dict(keyspace=self.keyspace,
                      ts_family=self.cclient._timeseries_column_family,
                      dt_family=self.cclient._data_column_family,
                      ts_id_name=self.cclient._timeseries_id_field_name,
                      did_name=self.cclient._data_id_field_name,
                      ts_field_name=self.cclient._timestamp_field_name,
                      data_columns=None,
                      data_values=None)

        data1 = dict(vint=2, vstring="Hello")
        did1 = uuid4()
        t1 = int(time.time())
        kv1 = zip(*data1.iteritems())

        params['data_columns'] = ", ".join(kv1[0])
        params['data_values'] = ", ".join("?" for _ in range(len(kv1[1])))

        query = """
            BEGIN BATCH
                INSERT INTO %(ts_family)s (%(ts_id_name)s, %(ts_field_name)s, %(did_name)s) VALUES (?, ?, ?)
                INSERT INTO %(dt_family)s (%(did_name)s, %(data_columns)s) VALUES (?, %(data_values)s)
            APPLY BATCH;
        """ % params

        prepared = session.prepare(query)
        session.execute(prepared, (0, t1, did1) + (did1, ) + tuple(kv1[1]))

        time.sleep(1)

        tm = int(time.time())

        time.sleep(1)

        data2 = dict(vint=1, vstring="Bye")
        did2 = uuid4()
        t2 = int(time.time())
        kv2 = zip(*data2.iteritems())

        params['data_columns'] = ", ".join(kv1[0])
        params['data_values'] = ", ".join("?" for _ in range(len(kv2[1])))

        query = """
            BEGIN BATCH
                INSERT INTO %(ts_family)s (%(ts_id_name)s, %(ts_field_name)s, %(did_name)s) VALUES (?, ?, ?)
                INSERT INTO %(dt_family)s (%(did_name)s, %(data_columns)s) VALUES (?, %(data_values)s)
            APPLY BATCH;
        """ % params

        prepared = session.prepare(query)
        session.execute(prepared, (0, t2, did2) + (did2, ) + tuple(kv2[1]))

        self.cclient.flush()

        results3 = self.cclient.latest(t2 + 1)
        results2 = self.cclient.latest(t2)
        resultsm = self.cclient.latest(tm)
        results1 = self.cclient.latest(t1)

        self.assertEqual(len(results1), 2)
        self.assertEqual(len(resultsm), 1)
        self.assertEqual(len(results2), 1)
        self.assertEqual(len(results3), 0)

        self.assertIn(did1, [did for _, did, _ in results1])
        self.assertNotIn(did1, [did for _, did, _ in resultsm])
        self.assertNotIn(did1, [did for _, did, _ in results2])
        self.assertNotIn(did1, [did for _, did, _ in results3])
        self.assertIn(did2, [did for _, did, _ in results1])
        self.assertIn(did2, [did for _, did, _ in resultsm])
        self.assertIn(did2, [did for _, did, _ in results2])
        self.assertEqual(len(results3), 0)

        session.shutdown()


def test_suite():
    return unittest.TestLoader().loadTestsFromTestCase(CassandraClientTestCase)