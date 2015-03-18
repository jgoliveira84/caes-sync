#!/usr/bin/python

import time

from daemon import runner
from client import CassandraClient, ElasticSearchClient


class App():
    def __init__(self, interval):
        self.stdin_path = '/dev/null'
        self.stdout_path = '/dev/tty'
        self.stderr_path = '/dev/tty'
        self.pidfile_path = '/tmp/caes.pid'
        self.pidfile_timeout = 5

        self._interval = interval

    def run(self):
        cclient = CassandraClient('', '')
        eclient = ElasticSearchClient('data', '', '')
        last = int(time.time())
        while True:
            new_last = int(time.time())
            elatest = eclient.latest(last)
            clatest = cclient.latest(last)
            last = new_last

            for e in elatest:
                cclient.write(e)

            for c in clatest:
                eclient.write(c)

            time.sleep(self._interval)



app = App(10)
daemon_runner = runner.DaemonRunner(app)
daemon_runner.do_action()