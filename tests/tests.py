
###

from os import path
import sys
import unittest
from unittest.mock import patch, Mock

import requests
import requests_mock

from salesforce_bulk_query import bulk_query


class call_api(unittest.TestCase):
     
    @classmethod
    def setUpClass(cls):
        'called once, before any tests'
        cls.response_text = 'api'
        cls.uri = 'https://salesforce.com/services'
        
        
    @classmethod
    def tearDownClass(cls):
        'called once, after all tests, if setUpClass successful'
        cls.response_text = None
        cls.uri = None 
    
    @requests_mock.Mocker()
    def test_get_token(self, mock_request):
        #bulk = call_api.bulk_query("Select name from Account")
        url = f"{call_api.uri}/oauth2/token" 
        mock_request.register_uri('POST',
                                  url = url,
                                  text = '''{"access_token":"test!this","instance_url":"my.salesforce.com","id":"https://test.salesforce.com/id/00Dr00000008w2cEAA/005r0000002dO1tAAE","token_type":"Bearer","issued_at":"1551s446637689","signature":"h0"}''',
                                  status_code = 200)
        bulk = bulk_query("Select name from Account")
        result = bulk.get_token()
        expected = "test\!this"
        self.assertEqual(result, expected)
    
    
    @patch('salesforce_bulk_query.bulk_query.get')
    def test_as_df(self,mock_get):
        mock_get.return_value = {'752r0000000lgyv': {'Type': 1, 'Website': 1}}
        result = bulk_query("Select name from Account").as_df()
        expected = (1,2)
        self.assertEqual(result.shape, expected)

    @patch('salesforce_bulk_query.bulk_query.get_job')    
    def test_job_id(self,mock_get_job):
            mock_get_job.return_value  = '''<?xml version="1.0"><jobInfo
                                            xmlns="http://www.force.com/2009/06/asyncapi/dataload">
                                            <id>750r0000000pTp0AAE</id>
                                            <operation>query</operation>
                                            <object>Account</object>
                                            <createdById>005r0000002dO1tAAE</createdById>
                                            <createdDate>2019-03-01T13:25:54.000Z</createdDate>
                                            <systemModstamp>2019-03-01T13:25:54.000Z</systemModstamp>
                                            <state>Open</state>
                                            <concurrencyMode>Parallel</concurrencyMode>
                                            <contentType>JSON</contentType>
                                            <numberBatchesQueued>0</numberBatchesQueued>
                                            <numberBatchesInProgress>0</numberBatchesInProgress>
                                            <numberBatchesCompleted>0</numberBatchesCompleted>
                                            <numberBatchesFailed>0</numberBatchesFailed>
                                            <numberBatchesTotal>0</numberBatchesTotal>
                                            <numberRecordsProcessed>0</numberRecordsProcessed>
                                            <numberRetries>0</numberRetries>
                                            <apiVersion>40.0</apiVersion>
                                            <numberRecordsFailed>0</numberRecordsFailed>
                                            <totalProcessingTime>0</totalProcessingTime>
                                            <apiActiveProcessingTime>0</apiActiveProcessingTime>
                                            <apexProcessingTime>0</apexProcessingTime>
                                            </jobInfo>'''                
            result = bulk_query("Select name from Account").get_job_id()
            expected = "750r0000000pTp0AAE"
            self.assertEqual(result, expected)
    
    @patch('salesforce_bulk_query.bulk_query.create_headers') 
    @requests_mock.Mocker()
    def test_get_batch(self,mock_create_headers, mock_request):
        mock_create_headers.return_value  = None
        #bulk = call_api.bulk_query("Select name from Account")
        jobId = '750r0000000pTp0AAE'
        url = f"{call_api.uri}/async/40.0/job/{jobId}/batch" 
        mock_request.register_uri('POST',
                                  url = url,
                                  text = '''{"apexProcessingTime":0,"apiActiveProcessingTime":0,"createdDate":"2019-03-01T13:27:50.000+0000","id":"751r0000001STxbAAG","jobId":"750r0000000pTp0AAE","numberRecordsFailed":0,"numberRecordsProcessed":0,"state":"Queued","stateMessage":null,"systemModstamp":"2019-03-01T13:27:50.000+0000","totalProcessingTime":0}''',
                                  status_code = 200)
        bulk = bulk_query("Select name from Account")
        result = bulk.get_batch(jobId)
        expected = "751r0000001STxbAAG"
        self.assertEqual(result, expected)

    @patch('salesforce_bulk_query.bulk_query._not_complete')    
    def test__wait_for_batch_completion(self,mock__not_complete):
            mock__not_complete.return_value  = {'batchInfo': [{'apexProcessingTime': 0, 'apiActiveProcessingTime': 0, 'createdDate': '2019-05-09T18:30:08.000+0000', 'id': '751t00000025hI1AAI', 'jobId': '750t00000010GPBAA2', 'numberRecordsFailed': 0, 'numberRecordsProcessed': 0, 'state': 'NotProcessed', 'stateMessage': None, 'systemModstamp': '2019-05-09T18:30:08.000+0000', 'totalProcessingTime': 0}, {'apexProcessingTime': 0, 'apiActiveProcessingTime': 0, 'createdDate': '2019-05-09T18:30:08.000+0000', 'id': '751t00000025hI6AAI', 'jobId': '750t00000010GPBAA2', 'numberRecordsFailed': 0, 'numberRecordsProcessed': 3459, 'state': 'Completed', 'stateMessage': None, 'systemModstamp': '2019-05-09T18:30:10.000+0000', 'totalProcessingTime': 0}]}             
            batch_1 =info = {'batchInfo': [{'apexProcessingTime': 0, 'apiActiveProcessingTime': 0, 'createdDate': '2019-05-09T18:30:08.000+0000', 'id': '751t00000025hI1AAI', 'jobId': '750t00000010GPBAA2', 'numberRecordsFailed': 0, 'numberRecordsProcessed': 0, 'state': 'NotProcessed', 'stateMessage': None, 'systemModstamp': '2019-05-09T18:30:08.000+0000', 'totalProcessingTime': 0}, {'apexProcessingTime': 0, 'apiActiveProcessingTime': 0, 'createdDate': '2019-05-09T18:30:08.000+0000', 'id': '751t00000025hI6AAI', 'jobId': '750t00000010GPBAA2', 'numberRecordsFailed': 0, 'numberRecordsProcessed': 3459, 'state': 'Queued', 'stateMessage': None, 'systemModstamp': '2019-05-09T18:30:10.000+0000', 'totalProcessingTime': 0}]}
            result = bulk_query("Select name from Account")._wait_for_batch_completion(batch_1)
            self.assertEqual(result, mock__not_complete.return_value)
    
    def test__check_for_all_complete(self):
        batch_complete = {'batchInfo': [{'apexProcessingTime': 0, 'apiActiveProcessingTime': 0, 'createdDate': '2019-05-09T18:30:08.000+0000', 'id': '751t00000025hI1AAI', 'jobId': '750t00000010GPBAA2', 'numberRecordsFailed': 0, 'numberRecordsProcessed': 0, 'state': 'NotProcessed', 'stateMessage': None, 'systemModstamp': '2019-05-09T18:30:08.000+0000', 'totalProcessingTime': 0}, {'apexProcessingTime': 0, 'apiActiveProcessingTime': 0, 'createdDate': '2019-05-09T18:30:08.000+0000', 'id': '751t00000025hI6AAI', 'jobId': '750t00000010GPBAA2', 'numberRecordsFailed': 0, 'numberRecordsProcessed': 3459, 'state': 'Completed', 'stateMessage': None, 'systemModstamp': '2019-05-09T18:30:10.000+0000', 'totalProcessingTime': 0}]}
        result = bulk_query("Select name from Account")._check_for_all_complete(batch_complete)
        self.assertTrue(result)
    
    def test__get_data_object(self):
        result = bulk_query("Select name from Account")._get_data_object("Select name from Account")
        self.assertEqual(result,"account")


    @patch('salesforce_bulk_query.bulk_query.get_result_id')
    @patch('salesforce_bulk_query.bulk_query.create_headers')
    @requests_mock.Mocker()
    def test__get_results(self, mock_create_headers, mock_get_result_id,  mock_request):
        mock_get_result_id.return_value = '752r0000000lgyv'
        mock_create_headers.return_value  = None

        url = f"{call_api.uri}/async/40.0/job/750t00000010GPBAA2/batch/751t00000025hI6AAI/result/752r0000000lgyv" 
        mock_request.register_uri('GET',
                                  url = url,
                                  json =   {"Type" : 1,"Website" :1},
                                  status_code = 200)
        
        batches = {'batchInfo': [{'apexProcessingTime': 0, 'apiActiveProcessingTime': 0, 'createdDate': '2019-05-09T18:30:08.000+0000', 'id': '751t00000025hI6AAI', 'jobId': '750t00000010GPBAA2', 'numberRecordsFailed': 0, 'numberRecordsProcessed': 3459, 'state': 'Completed', 'stateMessage': None, 'systemModstamp': '2019-05-09T18:30:10.000+0000', 'totalProcessingTime': 0}]}
        result =bulk_query("Select name from Account")._get_results(batches)
        expected = {'752r0000000lgyv': {'Type': 1, 'Website': 1}}
        self.assertEqual(result, expected)
