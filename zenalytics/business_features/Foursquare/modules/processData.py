import os
import json
import pandas as pd
import numpy as np
from modules import initialize
import logging
import requests

module_logger = logging.getLogger("mainApp.processData")

globalVars = initialize.readConfigFile()

def foursquare_search_parameters(name,street,city,state,country,zipcode,latitude,longitude):
    
    # configure logger
    logger = logging.getLogger("mainApp.processData.add")
    
    #See the foursquare API for more details
    params = {}
    params["query"] = str(name)
    params["ll"] = "{},{}".format(latitude,longitude)
    params["radius"] = str(globalVars.RADIUS)
    params["limit"] = str(globalVars.LIMIT)
    params["intent"] = 'match'
    params["address"] = str(street)
    params["city"] = str(city)
    params["state"] = str(state)
    params["zip"] = str(zipcode)

    logger.debug('foursquare parameters:{}'.format(params))
    
    return params


def foursquare_match (parent_name,business_name,street,city,state,country,zipcode,latitude,longitude):

    # configure logger
    logger = logging.getLogger("mainApp.processData.add")
    
    is_match=True

    try: # try searching on Parent Name

        foursquare_params=foursquare_search_parameters (parent_name,street,city,state,country,zipcode,latitude,longitude)
        foursquare_output=globalVars.FOURSQUARE_CLIENT.venues.search(params=foursquare_params)
        foursquare_id=foursquare_output["venues"][0].get('id') 

    except Exception as e:

        try: # If Parent Name search fails, try searching on child name
            foursquare_params=foursquare_search_parameters (business_name,street,city,state,country,zipcode,latitude,longitude)
            foursquare_output=globalVars.FOURSQUARE_CLIENT.venues.search(params=foursquare_params)
            foursquare_id=foursquare_output["venues"][0].get('id') 

        except Exception as e:
            
            is_match = False
            foursquare_id='No Match'
            foursquare_output = dict({'status':str(e)})

    logger.debug('foursquare match:{0}, foursquare id:{1}, foursquare output:{2}'.format(is_match, foursquare_id, foursquare_output))
    
    return is_match, foursquare_id, foursquare_output


def foursquare_venues (foursquare_id):

    # configure logger
    logger = logging.getLogger("mainApp.processData.add")
    
    foursquare_categories={}
    is_venue = True

    try:
        foursquare_output=globalVars.FOURSQUARE_CLIENT.venues(foursquare_id)
        foursquare_categories=foursquare_output["venue"]["categories"]

    except Exception as e:

        is_venue = False
        foursquare_categories = dict({'status':str(e)})

    logger.debug('foursquare venue match:{0} foursquare categories:{1}'.format(is_venue, foursquare_categories))
    
    return is_venue, foursquare_categories

         
