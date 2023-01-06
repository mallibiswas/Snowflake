import hashlib
import hmac
import random
import requests
import time
import string
import json
import pandas as pd
import numpy as np
from modules import initialize
import logging

module_logger = logging.getLogger("mainApp.readData")

globalVars = initialize.readConfigFile()

class CloudTrax(object):

    def __init__(self, key, secret):
        self._key = key
        self._secret = secret
        self.api_server = 'https://api-v2.cloudtrax.com'

    def _build_headers(self, endpoint, data=''):

        nonce = ''.join([random.choice(string.ascii_letters) for _ in range(8)])
        now = int(time.time())

        authorization = 'key={},timestamp={},nonce={}'.format(self._key, str(now), nonce)
        signature = hmac.new(key=self._secret, msg=str(authorization + endpoint + data).encode('utf-8'),
                             digestmod=hashlib.sha256).hexdigest()
        
        headers = {
            'Authorization': authorization,
            'Signature': signature,
            'Content-Type': 'application/json',
            'OpenMesh-API-Version': '2'
        }
        return headers

    def get(self, endpoint):
        headers = self._build_headers(endpoint)
        response = requests.get(self.api_server + endpoint, headers=headers)
        return response.json()

    def post(self, endpoint, data):
        body = json.dumps(data)
        headers = self._build_headers(endpoint, body)
        response = requests.post(self.api_server + endpoint, headers=headers, data=body)
        return response.json()

    def put(self, endpoint, data):
        body = json.dumps(data)
        headers = self._build_headers(endpoint, body)
        response = requests.put(self.api_server + endpoint, headers=headers, data=body)
        return response.json()

    def delete(self, endpoint):
        headers = self._build_headers(endpoint)
        response = requests.delete(self.api_server + endpoint, headers=headers)
        return response.json()

def readCloudtraxOpenmeshData():

    columns = ['mac','down','uptime','uptime_seconds','ip','name','network_first_add','outdoor','active_clients','alerts','lan_info','asof_date']

    # configure
    logger = logging.getLogger("mainApp.readData.add")

    # connect and download from Cloudtrax Openmesh Dashboard API	
    try:
        api=CloudTrax(key=str(globalVars.CLOUDTRAX_OPENMESH_KEY), secret=str(globalVars.CLOUDTRAX_OPENMESH_SECRET).encode('ascii'))

        result_=api.get(endpoint="/network/list")
        result=pd.DataFrame(result_['networks'])['network_id']
        df0=pd.DataFrame()
        for network_id in result:
           endpoint='/node/network/'+str(network_id)+'/list'
           response=api.get(endpoint=endpoint)['nodes']
           df=pd.DataFrame.from_dict(response, orient='index')
           df0=df0.append(df)

       # subset to needed columns
        df0['asof_date']=pd.datetime.now()
        df=df0[columns]

    except Exception as e: # any error here is severe
        logger.error ("raised exception reading df from Cloudtrax-Openmesh Dashboard API: {}".format(str(e)))

    return df
