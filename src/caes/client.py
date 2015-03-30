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


class CassandraClient(object):
    def __init__(self,
                 keyspace,
                 data_column_family,
                 insertQuery="",
                 timeseries_column_family='ts',
                 timeseries_id_field_name='id',
                 data_id_field_name='did',
                 timestamp_field_name='timestamp',
                 cassandra_driver_params=dict(),
                 ttl=3600
    ):
        self.__logger = logging.getLogger(__name__)

        self._cluster = Cluster()
        self._keyspace = keyspace
        self._timeseries_column_family = timeseries_column_family
        self._timeseries_id_field_name = timeseries_id_field_name
        self._data_column_family = data_column_family
        self._timestamp_field_name = timestamp_field_name
        self._data_id_field_name = data_id_field_name
        self._insert_query = insertQuery
        self._ttl = ttl

        self.__last = []

    def _get_by_timeseries_entry(self, tsentry):
        did = tsentry.pop(self._data_id_field_name)
        ts = tsentry.pop(self._timestamp_field_name)

        results = []

        if (did, ts) in self.__last:
            self.__logger.debug("%s already synced.", str(did))
            return None, did, ts

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
            data = None
        else:
            data = results[0]
            data.pop(self._data_id_field_name)

        return data, did, ts

    def _prepare_for_writing(self, cassdata):
        return self._get_by_timeseries_entry(cassdata)

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

        results = [self._prepare_for_writing(r) for r in results]

        self.__logger.info("Cassandra: %s", results)

        return results

    def flush(self):
        pass

    def write(self, dlist):
        try:
            session = self._cluster.connect(self._keyspace)
        except (OperationTimedOut, Timeout) as e:
            self.__logger.exception(e)
            return
        except:
            raise

        last_synced = []
        for data, did, ts in dlist:
            if data is None:
                self.__logger.info("Data is None for id %s. Can't sync.", str(did))
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
                          data_values=", ".join("%(" + str(f) + ")s" for f in kv[0]))

            insert_schema_ts = "INSERT INTO %(ts_family)s (%(ts_id_name)s, %(ts_field_name)s, %(did_name)s) " % params
            insert_values_ts = "VALUES (0, %(ts)s, %(did)s) USING TTL " + str(self._ttl)
            insert_ts = insert_schema_ts + insert_values_ts

            insert_schema_data = "INSERT INTO %(dt_family)s (%(did_name)s, %(data_columns)s) " % params
            insert_values_data = "VALUES (%(did)s, " + ("%(data_values)s) " % params)
            insert_data = insert_schema_data + insert_values_data

            query = """
                BEGIN BATCH
                    %s
                    %s
                    %s
                APPLY BATCH;
            """ % (insert_ts, insert_data, self._insert_query)

            self.__logger.debug(query)

            values_dict = dict(did=did, ts=ts)
            for k, v in data.iteritems():
                values_dict[k] = v

            try:
                session.execute(query, values_dict)
            except (OperationTimedOut, Timeout, InvalidRequest) as e:
                self.__logger.exception(e)
            except:
                raise

            last_synced.append((did, ts))

        try:
            session.shutdown()
        except (OperationTimedOut, Timeout) as e:
            self.__logger.exception(e)
        except:
            raise

        self.__last = last_synced

    def close(self):
        self._cluster.shutdown()


class ElasticSearchClient(object):
    def __init__(self,
                 index,
                 doc_type,
                 es_driver_params=dict(),
                 exclude=None,
                 include=None):
        self.__logger = logging.getLogger(__name__)

        self._index = index
        self._doc_type = doc_type
        self._timestamp_field_name = '_timestamp'
        self._data_id_field_name = '_id'
        self._es = Elasticsearch(**es_driver_params)
        self._exclude = exclude
        self._include = include

        self._iclient = self._es.indices

        self.__last = []

    def _prepare_for_writing(self, esdata):
        data = esdata['_source']
        ts = esdata['_version']
        did = UUID(esdata['_id'])

        if (did, ts) in self.__last:
            self.__logger.debug("%s already synced.", str(did))
            return None, did, ts

        res_list = None
        if self._include is not None:
            res_list = [(k, v) for k, v in data.iteritems() if k in self._include]
        elif self._exclude is not None:
            res_list = [(k, v) for k, v in data.iteritems() if k not in self._exclude]

        if res_list is not None:
            data = dict(res_list)

        return data, did, ts

    def latest(self, since):
        results = []
        query = {"query": {"constant_score": {"filter": {"range": {self._timestamp_field_name: {"gte": since}}}}}}

        self.__logger.info('Querying Elastic Search for updates...')

        try:
            results = []
            offset = 0
            while True:
                res = self._es.search(index=self._index,
                                      body=query,
                                      version=True,
                                      from_=offset,
                                      size=50)['hits']['hits']
                if len(res) == 0:
                    break

                results.extend(res)
                offset += 50

        except (ImproperlyConfigured, ElasticsearchException) as e:
            self.__logger.exception(e)
        except:
            raise

        results = [self._prepare_for_writing(r) for r in results]

        self.__logger.info('Elastic Search: %s', results)

        return results

    def flush(self):
        self._iclient.flush(index=self._index)

    def write(self, dlist):
        last_synced = []
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

            last_synced.append((did, ts))

        self.__last = last_synced

    def close(self):
        pass



