#/!bin/python3.x

# -*- coding: utf-8 -*-
"""
Created on Mon May 20 14:09:30 2019
@author: carrie.isaacson 
@deployed: mallinath.biswas
"""

#!/anaconda3/bin/python3.5

# Note: Run using Python 3.x
#

import pandas as pd
import time
import snowflake.connector

from modules import initialize
from modules import readData 
from modules import processData 
from modules import writeData 

import os

import logging
import logging.config
from os import path


def setLogger (configFile, propagate):
    
    # setup logger Based on http://docs.python.org/howto/logging.html#configuring-logging    
    config_file_path = path.join(path.dirname(path.abspath(__file__)), configFile)
    if path.isfile(config_file_path) is False:
        raise Exception("Config file {} not found".format(config_file_path))
    else:
        logging.config.fileConfig(config_file_path) 
    logger = logging.getLogger("mainApp");

    logger.propagate = propagate # turn off upper logger including console logging

    return logger


def trackTime ():    
    
    now = time.strftime("%H:%M:%S", time.localtime(time.time()))
    
    return now, time.time()


#########################
# Main program
#########################

if __name__ == '__main__':

    # setup logger, propagate=True for printing to terminal 
    logger = setLogger('logging.conf', propagate=True)    
    
    now, startClock = trackTime()
    
    logger.info ("Start execution at {}".format(now))
    
    # initialize global variables
    globalVars = initialize.readConfigFile() # set global variables

    # read static data 

    query_by_place_id = readData.readStaticData()['query_by_place_id']
    manual_queries = readData.readStaticData()['manual_queries']
    do_not_match = readData.readStaticData()['do_not_match']
    skip_parent = readData.readStaticData()['skip_parent']

    # Connecting to Snowflake using the default authenticator
    cursor_read = globalVars.CONN.cursor()
    # create business query	
    qq = readData.createBusinessQuery()
    # execute business query	
    cursor_read.execute(qq)

#    logger.info(cursor_read)

    business_ids = []
    for (business_id, parent_id, parent_name, name, city, state, street, zipcode, google_place_id) in cursor_read:

        business_profile = {
            'business_id': business_id,
            'parent_id': parent_id,
            'parent_name': parent_name,
            'name': name,
            'city': city, 
            'state': state, 
            'street': street, 
            'zipcode': zipcode, 
            'google_place_id': google_place_id
             }    

        logger.info ('Searching for {} {} in {}, {}, {} (parent {})'.format(business_id, name, city, state, zipcode, parent_name))
        logger.info('Searching for {} {} in {}, {}, {} (parent {}, place_id {})'.format(business_id, name, street, city, state, parent_name, google_place_id))

        business_ids.append(business_id)

        if business_profile.get('business_id') not in do_not_match:

           # Search APIs for business details
            out = processData.get_business_details(business_profile)
            logger.info ('out:{}'.format(out))

    	  # write to Snowflake 
            status = writeData.update_business_details(business_profile, out)
            logger.info("update status:{}".format(status))

        logger.info('#'*20)

    # close database connection
    globalVars.CONN.close()

    now, endClock = trackTime()    
    
    logger.info ("Main Job Completed at {} in {} seconds".format(now, int(round(endClock-startClock))))
