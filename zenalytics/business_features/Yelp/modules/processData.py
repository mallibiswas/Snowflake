# -*- coding: utf-8 -*-

import os
import json
import pandas as pd
import numpy as np
from modules import initialize
import logging
import requests

module_logger = logging.getLogger("mainApp.processData")

globalVars = initialize.readConfigFile()


def get_yelp_search_parameters(name,street,city,state,country,zip,latitude,longitude):

    # configure logger
    logger = logging.getLogger("mainApp.processData.add")

    #See the Yelp API for more details
    params = {}
    params["name"] = str(name)
    params["address1"] = str(street)
    params["city"] = str(city)
    params["state"] = str(state)
    params["country"] = str(country)
    params["zip_code"] = str(zip)
    params["latitude"] = str(latitude)
    params["longitude"] = str(longitude)
    params["limit"] = str(globalVars.LIMIT)

    logger.debug('yelp parameters:{}'.format(params))

    return params


def get_yelp_match_results (business_id, params):

    # configure logger
    logger = logging.getLogger("mainApp.processData.add")

    yelp_id = 'Missing'
    yelp_dump={"yelp":"missing"}
    yelp_match=True
    # get yelp results
    try:

        logger.debug('businesses_id={},match_url={},params={},headers={}'.format(business_id,globalVars.YELP_MATCH_URL,params,globalVars.YELP_HEADERS))

        rM = requests.get(globalVars.YELP_MATCH_URL, params=params, headers=globalVars.YELP_HEADERS)
        yelp_id=json.loads(rM.text)["businesses"][0].get('id')
        yelp_dump=json.loads(rM.text)
        match_results = dict({'yelp_match':yelp_match,'business_id':business_id,'yelp_id':yelp_id, 'yelp_dump':yelp_dump})

    except (KeyError, IndexError) as e:
        match_results = dict({'yelp_match':False,'business_id':business_id,'yelp_id':yelp_id, 'yelp_dump':str(e)})
        return match_results

    logger.debug('yelp match:{0}, yelp id:{1}, yelp output:{2}'.format(yelp_match, yelp_id, match_results))

    return match_results


def get_yelp_business_results (yelp_id):

    # configure logger
    logger = logging.getLogger("mainApp.processData.add")

    business_url=globalVars.YELP_BUSINESS_URL

    try:
        rB = requests.get(business_url.format(yelp_id), headers=globalVars.YELP_HEADERS)
        yelp_businesses = json.loads(rB.text)
    except (KeyError, IndexError) as e:
        yelp_businesses = dict({'status':str(e)})

    logger.debug('yelp businesses:{}'.format(yelp_businesses))

    return yelp_businesses
