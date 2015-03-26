# -*- coding: utf-8 -*-

import unittest
import time

from uuid import uuid4, UUID
from cassandra.query import dict_factory
from caes.client import ElasticSearchClient, CassandraClient
from caes.test.utils import random_string
from caes.sync import Sync


class SyncTestCase(unittest.TestCase):
    def tearDown(self):
        self.iclient.flush(self.index)
        self.iclient.delete(self.index)

    def setUp(self):
        self.index = random_string().lower()
        self.doc_type = random_string().lower()
        self.eclient = ElasticSearchClient(self.index, self.doc_type)
        self.iclient = self.eclient._iclient
        self.iclient.create(self.index)
        self.iclient.put_mapping(index=[self.index], doc_type=self.doc_type,
                                 body={self.doc_type: {'_timestamp': {'enabled': True, 'store': True}}})

        self.keyspace = random_string().lower()
        self.data_column_family = random_string().lower()
        self.cclient = CassandraClient(self.keyspace, self.data_column_family)

        session = self.cclient._cluster.connect()

        query = """
            CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
        """ % self.keyspace

        session.execute(query)
        session.shutdown()

        self.sync = Sync(self.eclient, self.cclient)

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

    def _get_cassandra_row_by_id(self, did):
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

        return results[0] if len(results) > 0 else None

    def test_simple_cassandra_to_es(self):
        datac = dict(vint=1, vstring="Hi")
        didc = uuid4()
        timestampc = 10

        self.cclient.write([(datac, didc, timestampc)])
        self.cclient.flush()

        self.sync.sync(9)

        result = self.eclient._es.get(index=self.index, doc_type=self.doc_type, id=didc)

        self.assertIsNotNone(result)
        self.assertEqual(datac['vint'], result['_source']['vint'])
        self.assertEqual(datac['vstring'], result['_source']['vstring'])

    def test_simple_es_to_cassandra(self):
        datae = dict(vint=1, vstring="Hi")
        dide = uuid4()
        timestampe = 10

        self.eclient.write([(datae, dide, timestampe)])
        self.eclient.flush()

        self.sync.sync(9)

        result = self._get_cassandra_row_by_id(dide)

        self.assertIsNotNone(result)
        self.assertEqual(datae['vint'], result['vint'])
        self.assertEqual(datae['vstring'], result['vstring'])




def test_suite():
    return unittest.TestLoader().loadTestsFromTestCase(SyncTestCase)

