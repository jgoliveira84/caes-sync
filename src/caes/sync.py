#!/usr/bin/python
# -*- coding: utf-8 -*-

import time
import logging

from daemon import runner
from caes.client import CassandraClient, ElasticSearchClient


class Sync(object):
    def __init__(self, eclient, cclient):
        self.__logger = logging.getLogger(__name__)
        self._eclient = eclient
        self._cclient = cclient

    def sync(self, since):
        self.__logger.info("Syncing since %d", since)

        elatest = self._eclient.latest(since)
        clatest = self._cclient.latest(since)

        last = 0

        for e in elatest:
            self._cclient.write(e)

        for c in clatest:
            data, did, ts = self._cclient.get_by_timeseries_entry(c)
            self._eclient.write(data, did, ts)


class App(object):
    def __init__(self, eclient, cclient, interval):
        self.__logger = logging.getLogger(__name__)

        self.stdin_path = '/dev/null'
        self.stdout_path = '/dev/tty'
        self.stderr_path = '/dev/tty'
        self.pidfile_path = '/tmp/caes.pid'
        self.pidfile_timeout = 5

        self._eclient = eclient
        self._cclient = cclient
        self._interval = interval

    def run(self):
        s = Sync(self._eclient, self._cclient)

        last = 0
        while True:
            new_last = int(time.time())
            s.sync(last)
            last = 0
            time.sleep(self._interval)

def sync():
    FORMAT = '%(asctime)-15s %(message)s'
    logging.basicConfig(format=FORMAT, level=logging.DEBUG)

    eclient = ElasticSearchClient('data3')
    cclient = CassandraClient('demo',
                              'data2',
                              timeseries_column_family='ts2',
                              data_id_field_name='did')

    app = App(eclient, cclient, 10)
    daemon_runner = runner.DaemonRunner(app)
    daemon_runner.do_action()