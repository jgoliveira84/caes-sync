# -*- coding: utf-8 -*-

import unittest
import time

from uuid import uuid4, UUID
from caes.client import ElasticSearchClient
from caes.test.utils import random_string


class ElasticSearchClientTestCase(unittest.TestCase):
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

    def test_write_doc(self):
        data = dict(f1=1, f2="Hi")
        did = uuid4()
        timestamp = int(time.time())
        self.eclient.write([(data, did, timestamp)])

        self.eclient.flush()

        result = self.eclient._es.get(index=self.index, doc_type=self.doc_type, id=did)

        self.assertIsNotNone(result)
        self.assertDictEqual(data, result['_source'])

    def test_write_doc_none(self):
        data = dict(f1=1, f2="Hi")
        did = uuid4()
        did_none = uuid4()

        self.eclient.write([(data, did, int(time.time())), (None, did_none, int(time.time()))])
        self.eclient.flush()

        results = self.eclient._es.search(index=self.index, doc_type=self.doc_type)['hits']['hits']

        self.assertEqual(len(results), 1)

    def test_latest(self):
        data1 = dict(f1=1, f2="Hi")
        did1 = uuid4()
        t1 = int(time.time())

        self.eclient._es.index(self.index,
                               self.doc_type,
                               data1, did1,
                               timestamp=t1,
                               version=t1,
                               version_type="external")

        time.sleep(1)

        tm = int(time.time())

        time.sleep(1)

        data2 = dict(f1=1, f2="Hi")
        did2 = uuid4()
        t2 = int(time.time())

        self.eclient._es.index(self.index,
                               self.doc_type,
                               data2, did2,
                               timestamp=t2,
                               version=t2,
                               version_type="external")

        self.eclient.flush()

        results1 = self.eclient.latest(t1)
        resultsm = self.eclient.latest(tm)
        results2 = self.eclient.latest(t2)
        results3 = self.eclient.latest(t2 + 1)

        self.assertEqual(len(results1), 2)
        self.assertEqual(len(resultsm), 1)
        self.assertEqual(len(results2), 1)
        self.assertEqual(len(results3), 0)

        self.assertIn(did1, [UUID(data['_id']) for data in results1])
        self.assertNotIn(did1, [UUID(data['_id']) for data in resultsm])
        self.assertNotIn(did1, [UUID(data['_id']) for data in results2])
        self.assertNotIn(did1, [UUID(data['_id']) for data in results3])
        self.assertIn(did2, [UUID(data['_id']) for data in results1])
        self.assertIn(did2, [UUID(data['_id']) for data in resultsm])
        self.assertIn(did2, [UUID(data['_id']) for data in results2])
        self.assertEqual(len(results3), 0)


def test_suite():
    return unittest.TestLoader().loadTestsFromTestCase(ElasticSearchClientTestCase)