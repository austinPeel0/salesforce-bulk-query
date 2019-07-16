# -*- coding: utf-8 -*-

import requests, json, re, time
from xml.etree import ElementTree
from bs4 import BeautifulSoup

from pandas.io.json import json_normalize
import pandas as pd

from config import  base_url , credentials

class bulk_query():
    
    def __init__(self,sql_query,chunksize=1000):
        self.query = sql_query
        if chunksize > 10000 or chunksize < 1:
            self.chunkSize = 1000
        else:
            self.chunkSize = chunksize
    
    def as_df(self):
        results = self.get()
        data  = pd.DataFrame()
        for batch in results:
            df = json_normalize(results[batch])
            data= data.append(df)
        return data

    def get(self):
        '''
        function to extract data from salesforce query
        '''
        self.get_token()
        self.job_id = self.get_job_id()
        batch_id = self.get_batch(self.job_id)
        batches = self.get_batch_info(self.job_id)
        batches_new = self._wait_for_batch_completion(batches)
        results = self._get_results(batches_new)
        self._check_if_all_results_extracted(results)
        self._close_job()
        return results
    
    def get_token(self):
        r = requests.post(f"{base_url}/oauth2/token", params=credentials)
        access_token = r.json().get("access_token")
        self.access_token = re.sub(r'(!)', r'\\!',access_token)
        return self.access_token
    
    def create_xml(self,xml_type='open'):
        if xml_type == 'open':
            xml = f'''<?xml version="1.0" encoding="UTF-8"?>
                        <jobInfo
                        xmlns="http://www.force.com/2009/06/asyncapi/dataload">
                        <operation>query</operation>
                        <object>{self._get_data_object(self.query)}</object>
                        <concurrencyMode>Parallel</concurrencyMode>
                        <contentType>JSON</contentType>
                        </jobInfo>'''
        elif xml_type == 'close':
            xml = '''<?xml version="1.0" encoding="UTF-8"?>
                        <jobInfo xmlns="http://www.force.com/2009/06/asyncapi/dataload">
                        <state>Closed</state>
                        </jobInfo>'''
        else:
            print("xml_type needs to be open or close")
        return xml

    def create_headers(self,response_type):
        headers = {'X-SFDC-Session': self.access_token}
        if response_type == "job":
            headers.update({
                'Content-type': 'application/xml; charset=UTF-8',
                'Sforce-Enable-PKChunking': f'chunkSize={self.chunkSize}'
            })
        elif response_type == 'batch':
            headers.update({
                    'Content-Type': 'application/json; charset=UTF-8'}) 
        elif response_type == 'close':
            headers.update({
                "Content-Type": "application/xml; charset=UTF-8"})      
        return headers

    def get_job(self):
        headers = self.create_headers(response_type='job')
        xml = self.create_xml()
        r = requests.post(f'{base_url}/async/40.0/job', data=xml, headers=headers)
        return r.text
    
    def _check_if_all_results_extracted(self,results):
        number_of_records =0
        for result in results:
            number_of_records += len(results[result])
        if number_of_records == self.count:
            print(f"extracted {number_of_records} out of {self.count}")
        else:
            import warnings
            warnings.warn(f"extracted {number_of_records} out of {self.count}")
        self.number_of_records = number_of_records

    def get_job_id(self):
        s = self.get_job()
        soup = BeautifulSoup(s,'xml')
        titles = soup.find('id')
        job_id = titles.get_text()  
        return job_id                


    def get_batch(self,job_id):        
        url = f"{base_url}/async/40.0/job/{job_id}/batch"
        headers = self.create_headers(response_type='batch')
        r = requests.post(url,data=self.query, headers=headers)
        batch_id = r.json().get("id")
        return batch_id


    def get_batch_info(self,job_id):
        url = f"{base_url}/async/40.0/job/{job_id}/batch"
        headers = self.create_headers(response_type="base")
        r= requests.get(url, headers=headers)
        batch_info = r.json()
        return batch_info
    
    def get_result_id(self,batch):
        url = f"{base_url}/async/40.0/job/{batch['jobId']}/batch/{batch['id']}/result"  
        r= requests.get(url, headers=self.create_headers(response_type="base"))
        data = r.json()
        result_id = data[0]
        return result_id


    def _get_results(self,batches):
        data_dict = {}
        print("Extracting data")
        self.count = 0
        for batch in batches['batchInfo']:
            if batch['state'] == 'Completed':
                result_id = self.get_result_id(batch)
                url = f"{base_url}/async/40.0/job/{batch['jobId']}/batch/{batch['id']}/result/{result_id}"   
                r= requests.get(url, headers= self.create_headers(response_type="base"))
                data = r.json()
                self.count += batch['numberRecordsProcessed']
                data_dict.update({result_id:data})
            elif batch['state'] == 'NotProcessed':
                continue
            else:
                raise ValueError
            
        return data_dict
    
    
    def _get_data_object(self, query):
        query  = query.lower()
        match = re.search(r'from (\S+)', query)
        data_object = match.group(1)
        return data_object

    def _check_for_all_complete(self,batches):
        
        for i in batches['batchInfo']:
            
            if i['state'] == 'Completed':
                next
            elif i['state'] == 'NotProcessed':
                next
            else:
                return False
        return True
    
    def _not_complete(self):
        counter = 1
        print('Waiting for saleforce api to process all batches')
        while counter < 12:
            time.sleep(30 * counter) 
            batches_new = self.get_batch_info(self.job_id)
            if self._check_for_all_complete(batches_new):
                return batches_new
            else:
                counter += 1
        raise BatchTimeOut("Can not get batches to complete processing")
                
    def _wait_for_batch_completion(self, batches):
        for i in batches['batchInfo']:
            if i['state'] == 'Completed':
                pass
            elif i['state'] == 'Queued': 
                batches= self._not_complete()
            elif i['state'] == 'NotProcessed':  
                batches= self._not_complete()
            elif i['state'] == 'InProgress':   
                batches = self._not_complete()   
            else:
                raise BatchTimeOut("Can not find state for batch info")
        print("Retrieved all batches")
        return batches

    def _close_job(self):
        headers = self.create_headers(response_type='close')
        xml = self.create_xml(xml_type='close')
        r = requests.post(f'{base_url}/async/40.0/job/{self.job_id}', data=xml, headers=headers)        
        print("closing job")
        

    
class BatchTimeOut(Exception):
    pass
    
