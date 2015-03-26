# -*- coding: utf-8 -*-

import json
import logging

from uuid import UUID
from cassandra import OperationTimedOut, InvalidRequest, Timeout
from cassandra.query import dict_factory
from elasticsearch import Elasticsearch
from cassandra.cluster import Cluster
from elasticsearch.client.indices import IndicesClient
from elasticsearch.exceptions import ImproperlyConfigured, ElasticsearchException


_CASSANDRA_TIMESERIES_ID_FIELD_NAME = 'id'
_CASSANDRA_TIMESTAMP_FIELD_NAME = 'timestamp'
_CASSANDRA_TIMESERIES_COLUMN_FAMILY = 'ts'
_CASSANDRA_DATA_ID_FIELD_NAME = 'did'


class CassandraClient(object):
    def __init__(self,
                 keyspace,
                 data_column_family,
                 timeseries_column_family=_CASSANDRA_TIMESERIES_COLUMN_FAMILY,
                 data_id_field_name=_CASSANDRA_DATA_ID_FIELD_NAME,
                 cassandra_driver_params=dict(),
                 exclude=None,
                 include=None
    ):
        self.__logger = logging.getLogger(__name__)

        self._cluster = Cluster()
        self._keyspace = keyspace
        self._timeseries_column_family = timeseries_column_family
        self._timeseries_id_field_name = _CASSANDRA_TIMESERIES_ID_FIELD_NAME
        self._data_column_family = data_column_family
        self._timestamp_field_name = _CASSANDRA_TIMESTAMP_FIELD_NAME
        self._data_id_field_name = data_id_field_name
        self._exclude = exclude
        self._include = include

    def _get_by_timeseries_entry(self, tsentry):
        did = tsentry.pop(self._data_id_field_name)
        ts = tsentry.pop(self._timestamp_field_name)

        results = []

        query = """
            SELECT *
            FROM %s
            WHERE %s = ?
        """ % (self._data_column_family,
               self._data_id_field_name)

        self.__logger.debug(query)

        try:
            session = self._cluster.connect(self._keyspace)
            session.row_factory = dict_factory
            prepared = session.prepare(query)
            results = session.execute(prepared, (did,))
            session.shutdown()
        except (OperationTimedOut, Timeout, InvalidRequest) as e:
            self.__logger.exception(e)
        except:
            raise

        if len(results) == 0:
            self.__logger.warning("Doc %s does not exist.", str(did))
            res = None
        else:
            res = results[0]
            res.pop(self._data_id_field_name)

            res_list = None
            if self._include is not None:
                res_list = [(k, v) for k, v in res.iteritems() if k in self._include]
            elif self._exclude is not None:
                res_list = [(k, v) for k, v in res.iteritems() if k not in self._exclude]

            if res_list is not None:
                res = dict(res_list)

        return res, did, ts

    def latest(self, since):
        results = []

        self.__logger.info('Querying Cassandra for updates...')

        query = """
            SELECT *
            FROM %s
            WHERE %s >= %d
            AND %s = 0
        """ % (self._timeseries_column_family,
               self._timestamp_field_name,
               since,
               self._timeseries_id_field_name)

        self.__logger.debug(query)

        try:
            session = self._cluster.connect(self._keyspace)
            session.row_factory = dict_factory
            results = session.execute(query)
            session.shutdown()
        except (OperationTimedOut, Timeout, InvalidRequest) as e:
            self.__logger.exception(e)
        except:
            raise

        self.__logger.info('Cassandra: %s', results)

        return results

    def flush(self):
        pass

    def prepare_for_writing(self, cassdata):
        return self._get_by_timeseries_entry(cassdata)

    def write(self, dlist):
        try:
            session = self._cluster.connect(self._keyspace)
        except (OperationTimedOut, Timeout) as e:
            self.__logger.exception(e)
            return
        except:
            raise

        for data, did, ts in dlist:
            if data is None:
                self.__warning.info("Data is None for id %s. Can't sync.", str(did))
                continue

            kv = zip(*data.iteritems())

            self.__logger.info("Syncing from ES to Cassandra: %s", json.dumps(data))

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
                    INSERT INTO %(dt_family)s (%(did_name)s, %(data_columns)s) VALUES (?, %(data_values)s)
                APPLY BATCH;
            """ % params

            self.__logger.debug(query)

            try:
                prepared = session.prepare(query)
                session.execute(prepared, (0, ts, did) + (did, ) + tuple(kv[1]))
            except (OperationTimedOut, Timeout, InvalidRequest) as e:
                self.__logger.exception(e)
            except:
                raise

        try:
            session.shutdown()
        except (OperationTimedOut, Timeout) as e:
            self.__logger.exception(e)
        except:
            raise



    def close(self):
        self._cluster.shutdown()


class ElasticSearchClient(object):
    def __init__(self,
                 index,
                 doc_type,
                 es_driver_params=dict()):
        self.__logger = logging.getLogger(__name__)

        self._index = index
        self._doc_type = doc_type
        self._timestamp_field_name = '_timestamp'
        self._data_id_field_name = '_id'
        self._es = Elasticsearch(**es_driver_params)

        self._iclient = self._es.indices

    def latest(self, since):
        results = []
        query = {"query": {"constant_score": {"filter": {"range": {self._timestamp_field_name: {"gte": since}}}}}}

        self.__logger.info('Querying Elastic Search for updates...')

        try:
            results = self._es.search(index=self._index,
                                      body=query,
                                      version=True)['hits']['hits']
        except (ImproperlyConfigured, ElasticsearchException) as e:
            self.__logger.exception(e)
        except:
            raise

        self.__logger.info('Elastic Search: %s', results)

        return results

    def flush(self):
        self._iclient.flush(index=self._index)

    def prepare_for_writing(self, esdata):
        data = esdata['_source']
        ts = esdata['_version']
        did = UUID(esdata['_id'])

        return data, did, ts

    def write(self, dlist):
        for data, did, ts in dlist:
            if data is None:
                self.__logger.warning("Data is None for id %s. Can't sync.", str(did))
                continue

            self.__logger.info("Syncing from Cassandra to ES: %s", json.dumps(data))

            try:
                self._es.index(self._index, self._doc_type, data, did, timestamp=ts, version=ts, version_type="external")
            except (ImproperlyConfigured, ElasticsearchException) as e:
                self.__logger.exception(e)
            except:
                raise

    def close(self):
        pass



