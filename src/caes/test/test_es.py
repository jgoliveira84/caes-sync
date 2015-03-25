# -*- coding: utf-8 -*-

import unittest
import time

from uuid import uuid4
from elasticsearch.client.indices import IndicesClient
from caes.client import ElasticSearchClient
from caes.test.utils import random_string


class ElasticSearchClientTestCase(unittest.TestCase):
    def tearDown(self):
        self.iclient.delete(self.index)

    def setUp(self):
        self.index = random_string().lower()
        self.doc_type = random_string().lower()
        self.eclient = ElasticSearchClient(self.index, self.doc_type)
        self.iclient = IndicesClient(self.eclient._es)

        self.iclient.create(self.index)
        self.iclient.put_mapping(index=[self.index], doc_type=self.doc_type, body={self.doc_type: {'_timestamp': {'enabled': True, 'store':True}}})

    def test_write_doc(self):
        data = dict(f1=1, f2="Hi")
        did = uuid4()
        timestamp = int(time.time())

        self.eclient.write(data, did, timestamp)
        self.iclient.flush(index=self.index)

        results = self.eclient.latest(0)

        self.assertTrue(len(results) > 0)


def test_suite():
    suite1 = unittest.TestLoader().loadTestsFromTestCase(ElasticSearchClientTestCase)
    return unittest.TestSuite([suite1])