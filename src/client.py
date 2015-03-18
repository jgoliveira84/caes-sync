from elasticsearch import Elasticsearch
from cassandra.cluster import Cluster


class CassandraClient(object):
    def __init__(self,
                 keyspace,
                 timestamp_column_family,
                 data_column_family):
        self._keyspace = keyspace
        self._timestamp_column_family = timestamp_column_family
        self._data_column_family = data_column_family

        cluster = Cluster()
        self._session = cluster.connect(keyspace)

    def latest(self, since):
        query = """
            SELECT temperature
            FROM temperature
            WHERE event_time > %d
        """ % since

        results = self._session.execute(query)

        return results

    def write(self, row):
        pass


class ElasticSearchClient(object):
    def __init__(self,
                 index,
                 id_field_name,
                 timestamp_field_name):

        self._index = index
        self._id_field_name = id_field_name
        self._timestamp_field_name = timestamp_field_name

        self._es = Elasticsearch()

    def latest(self, since):
        query = {"query": {"constant_score": {"filter": {"range": {self._timestamp_field_name: {"gte": since}}}}}}

        results = self._es.search(index=self._index,
                              body=query)

        return results

    def write(self, doc):
        pass