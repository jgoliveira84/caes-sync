#!/usr/bin/python
# -*- coding: utf-8 -*-

import time
import logging
import yaml

from logging.config import dictConfig
from daemon import runner
from os.path import exists, expanduser, join
from os import getcwd
from caes.client import CassandraClient, ElasticSearchClient


class Sync(object):
    def __init__(self, eclient, cclient):
        self.__logger = logging.getLogger(__name__)
        self._eclient = eclient
        self._cclient = cclient

    def sync(self, since):
        self.__logger.info("Syncing since %d", since)

        self._eclient.flush()
        self._cclient.flush()
        elatest = self._eclient.latest(since)
        clatest = self._cclient.latest(since)

        self._cclient.write((self._eclient.prepare_for_writing(e) for e in elatest))
        self._eclient.write((self._cclient.prepare_for_writing(c) for c in clatest))

    def __enter__(self):
        return self

    def __exit__(self, type, value, tb):
        self._eclient.close()
        self._cclient.close()


class App(object):
    def __init__(self):
        self.__logger = logging.getLogger(__name__)

        self.stdin_path = '/dev/null'
        self.stdout_path = '/dev/tty'
        self.stderr_path = '/dev/tty'
        self.pidfile_path = '/tmp/caes.pid'
        self.pidfile_timeout = 5

    def _config(self):
        config_dict = None

        if exists(expanduser("~/.caes/config.yaml")):
            config_path = expanduser("~/.caes/config.yaml")
        else:
            raise ValueError("Config file not found!!!")

        print "Config file found at %s" % config_path

        with open(config_path) as f:
            config_dict = yaml.load(f)

        logging.config.dictConfig(config_dict['logging'])

        interval = config_dict.get('interval') if config_dict.get('interval') is not None else 10

        es_config_dict = config_dict['ElasticSearchConfig']
        eclient = self._config_es(es_config_dict)

        cassandra_config_dict = config_dict['CassandraConfig']
        cclient = self._config_cassandra(cassandra_config_dict)

        return eclient, cclient, interval

    def _config_es(self, es_config_dict):
        index = es_config_dict['index']
        doc_type = es_config_dict['type']

        driver = es_config_dict['driver'] if es_config_dict.get('driver') is not None else dict()

        return ElasticSearchClient(index, doc_type, es_driver_params=driver)

    def _config_cassandra(self, cassandra_config_dict):
        keyspace = cassandra_config_dict['keyspace']
        data_column_family = cassandra_config_dict['dataColumnFamily']
        driver = cassandra_config_dict['driver'] if cassandra_config_dict.get('driver') is not None else dict()

        casskw = dict()
        if cassandra_config_dict.get('timeseriesColumnFamily') is not None:
            casskw['timeseries_column_family'] = cassandra_config_dict['timeseriesColumnFamily']

        if cassandra_config_dict.get('dataIdFieldName') is not None:
            casskw['data_id_field_name'] = cassandra_config_dict['dataIdFieldName']

        if cassandra_config_dict.get('exclude') is not None:
            casskw['exclude'] = cassandra_config_dict['exclude']

        if cassandra_config_dict.get('include') is not None:
            casskw['include'] = cassandra_config_dict['include']

        return CassandraClient(keyspace, data_column_family, cassandra_driver_params=driver, **casskw)

    def run(self):
        eclient, cclient, interval = self._config()

        last = int(time.time())

        print "Syncing starting from %d" % last

        with Sync(eclient, cclient) as s:
            while True:
                new_last = int(time.time())
                time.sleep(interval)
                s.sync(last)
                last = new_last




def sync():
    app = App()
    daemon_runner = runner.DaemonRunner(app)
    daemon_runner.do_action()