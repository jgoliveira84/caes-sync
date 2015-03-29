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

    def _outside_write_to_cassandra(self, data, did, ts):
        session = self.cclient._cluster.connect(self.keyspace)
        params = dict(keyspace=self.keyspace,
                      ts_family=self.cclient._timeseries_column_family,
                      dt_family=self.cclient._data_column_family,
                      ts_id_name=self.cclient._timeseries_id_field_name,
                      did_name=self.cclient._data_id_field_name,
                      ts_field_name=self.cclient._timestamp_field_name,
                      data_columns=None,
                      data_values=None)

        kv = zip(*data.iteritems())

        params['data_columns'] = ", ".join(kv[0])
        params['data_values'] = ", ".join("?" for _ in range(len(kv[1])))

        query = """
            BEGIN BATCH
                INSERT INTO %(ts_family)s (%(ts_id_name)s, %(ts_field_name)s, %(did_name)s) VALUES (?, ?, ?)
                INSERT INTO %(dt_family)s (%(did_name)s, %(data_columns)s) VALUES (?, %(data_values)s)
            APPLY BATCH;
        """ % params

        prepared = session.prepare(query)
        session.execute(prepared, (0, ts, did) + (did, ) + tuple(kv[1]))

        self.cclient.flush()

        session.shutdown()

    def _outside_write_to_elasticsearch(self, data, did, ts):
        self.eclient._es.index(self.index,
                               self.doc_type,
                               data, did,
                               timestamp=ts,
                               version=ts,
                               version_type="external")

        self.eclient.flush()

    def test_simple_cassandra_to_es(self):
        datac = dict(vint=1, vstring="Hi")
        didc = uuid4()
        timestampc = 10

        self._outside_write_to_cassandra(datac, didc, timestampc)
        self.sync.sync(9)

        result = self.eclient._es.get(index=self.index,
                                      doc_type=self.doc_type,
                                      id=didc)

        self.assertIsNotNone(result)
        self.assertEqual(datac['vint'], result['_source']['vint'])
        self.assertEqual(datac['vstring'], result['_source']['vstring'])

    def test_simple_es_to_cassandra(self):
        datae = dict(vint=1, vstring="Hi")
        dide = uuid4()
        timestampe = 10

        self._outside_write_to_elasticsearch(datae, dide, timestampe)

        self.sync.sync(9)

        result = self._get_cassandra_row_by_id(dide)

        self.assertIsNotNone(result)
        self.assertEqual(datae['vint'], result['vint'])
        self.assertEqual(datae['vstring'], result['vstring'])

    def test_most_recent(self):
        did = uuid4()

        datae = dict(vint=99, vstring="The most recent!!")
        timestampe = 11

        datac = dict(vint=1, vstring="Hi")
        timestampc = 10

        self._outside_write_to_elasticsearch(datae, did, timestampe)
        self._outside_write_to_cassandra(datac, did, timestampc)

        self.sync.sync(9)

        resultc = self._get_cassandra_row_by_id(did)
        resulte = self.eclient._es.get(index=self.index,
                                       doc_type=self.doc_type,
                                       id=did)

        self.assertIsNotNone(resultc)
        self.assertEqual(datae['vint'], resultc['vint'])
        self.assertEqual(datae['vstring'], resultc['vstring'])

        self.assertIsNotNone(resulte)
        self.assertEqual(datae['vint'], resulte['_source']['vint'])
        self.assertEqual(datae['vstring'], resulte['_source']['vstring'])


def test_suite():
    return unittest.TestLoader().loadTestsFromTestCase(SyncTestCase)

