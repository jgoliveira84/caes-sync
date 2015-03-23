# -*- coding: utf-8 -*-

import json
import logging
from uuid import UUID

from cassandra import OperationTimedOut, InvalidRequest, Timeout
from cassandra.query import dict_factory
from elasticsearch import Elasticsearch
from cassandra.cluster import Cluster
from elasticsearch.exceptions import ImproperlyConfigured, ElasticsearchException


class CassandraClient(object):
    def __init__(self,
                 keyspace,
                 data_column_family,
                 timeseries_column_family='ts',
                 data_id_field_name='id',
    ):
        self.__logger = logging.getLogger(__name__)
        self.__cluster = Cluster()

        self._keyspace = keyspace
        self._timeseries_column_family = timeseries_column_family
        self._timeseries_id_field_name = 'id'
        self._data_column_family = data_column_family
        self._timestamp_field_name = 'timestamp'
        self._data_id_field_name = data_id_field_name

    def latest(self, since):
        results = []

        query = """
            SELECT *
            FROM %s
            WHERE %s > %d
            AND %s = 0
        """ % (self._timeseries_column_family,
               self._timestamp_field_name,
               since,
               self._timeseries_id_field_name)

        self.__logger.debug(query)

        try:
            session = self.__cluster.connect(self._keyspace)
            session.row_factory = dict_factory
            results = session.execute(query)
            session.shutdown()
        except (OperationTimedOut, Timeout, InvalidRequest) as e:
            self.__logger.exception(e)
        except:
            raise

        return results

    def get_by_timeseries_entry(self, tsentry):
        did = tsentry.pop(self._data_id_field_name)
        ts = tsentry.pop(self._timeseries_id_field_name)

        results = []

        query = """
            SELECT *
            FROM %s
            WHERE %s = ?
        """ % (self._data_column_family,
               self._data_id_field_name)

        self.__logger.debug(query)

        try:
            session = self.__cluster.connect(self._keyspace)
            session.row_factory = dict_factory
            prepared = session.prepare(query)
            results = session.execute(prepared, (did,))
            session.shutdown()
        except (OperationTimedOut, Timeout, InvalidRequest) as e:
            self.__logger.exception(e)
        except:
            raise

        if len(results) == 0:
            self.__logger.warning("Doc %s does not exist.", str(docid))
            return

        res = results[0]
        res.pop(self._data_id_field_name)

        return results[0], did, ts

    def write(self, esdata):
        fields = esdata['_source']
        ts = esdata['_version']
        did = UUID(esdata['_id'])

        kv = zip(*fields.iteritems())

        self.__logger.debug("Syncing from ES to Cassandra: %s", json.dumps(esdata))

        params = dict(keyspace=self._keyspace,
                      ts_family=self._timeseries_column_family,
                      dt_family=self._data_column_family,
                      ts_id_name=self._timeseries_id_field_name,
                      did_name=self._data_id_field_name,
                      ts_field_name=self._timestamp_field_name,
                      data_columns=", ".join(kv[0]),
                      data_values=", ".join("?" for _ in range(len(kv[0]))))

        query = """
            BEGIN BATCH
                INSERT INTO %(ts_family)s (%(ts_id_name)s, %(ts_field_name)s, %(did_name)s) VALUES (?, ?, ?)
                INSERT INTO %(dt_family)s (did, %(data_columns)s) VALUES (?, %(data_values)s)
            APPLY BATCH;
        """ % params

        self.__logger.debug(query)

        try:
            session = self.__cluster.connect(self._keyspace)
            prepared = session.prepare(query)
            session.execute(prepared, (0, ts, did) + (did, ) + tuple(kv[1]))
            session.shutdown()
        except (OperationTimedOut, Timeout, InvalidRequest) as e:
            self.__logger.exception(e)
        except:
            raise


class ElasticSearchClient(object):
    def __init__(self,
                 index):
        self.__logger = logging.getLogger(__name__)

        self._index = index
        self._timestamp_field_name = '_timestamp'

        self.data_id_field_name = '_id'

        self._es = Elasticsearch()

    def latest(self, since):
        results = []
        query = {"query": {"constant_score": {"filter": {"range": {self._timestamp_field_name: {"gte": since}}}}}}

        self.__logger.info('Querying..')

        try:
            results = self._es.search(index=self._index,
                                      body=query,
                                      version=True)['hits']['hits']
        except (ImproperlyConfigured, ElasticsearchException) as e:
            self.__logger.exception(e)
        except:
            raise

        return results

    def write(self,
              cassdata,
              did,
              timestamp):
        self.__logger.debug("Syncing from ES to Cassandra: %s", json.dumps(cassdata))

        self._es.index(self._index, 'tweet', cassdata, did, timestamp=timestamp)

