#/!bin/python3.x

# -*- coding: utf-8 -*-
"""
Created on July 20 2020 14:09:30 2019
@author: mallinath.biswas
@deployed: mallinath.biswas
"""

#!/anaconda3/bin/python3.57

#

import pandas as pd
import time
import snowflake.connector
import json

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

#    logger.propagate = propagate # turn off upper logger including console logging

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

    # Fetch snowflake data
    snow_df = pd.read_sql(readData.createReadQuery(), globalVars.CONN)

    logger.debug ('Print snow_df.head:{}'.format(snow_df.head()))

    for index, row in snow_df.iterrows():

        # assign record to variables
        input_dict = row.to_dict()

        business_id=row["BUSINESS_ID"]
        parent_name=row["PARENT_NAME"]
        business_name=row["BUSINESS_NAME"]
        street=row["STREET"]
        city=row["CITY"]
        state=row["STATE"]
        country=row["COUNTRY"]
        zip=row["ZIPCODE"]
        latitude=row["LATITUDE"]
        longitude=row["LONGITUDE"]

        logger.info ('Searching for business id:{}'.format(business_id))

        # get yelp results

        yelp_params=processData.get_yelp_search_parameters (parent_name, street, city, state, country, zip, latitude, longitude)
        rP = processData.get_yelp_match_results (business_id, yelp_params)
        yelp_match = rP['yelp_match']

        if yelp_match:
            logger.info ('Matching Parent:{}'.format(yelp_match))
            yelp_id=rP['yelp_id']
            writeData.write_yelp_record (business_id, yelp_id)

        else: # parent name did not match
            yelp_params=processData.get_yelp_search_parameters (business_name, street, city, state, country, zip, latitude, longitude)
            rB = processData.get_yelp_match_results (business_id, yelp_params)
            yelp_match = rB['yelp_match']
            logger.info ('Matching Business:{}'.format(yelp_match))

            if yelp_match:
                yelp_id=rB['yelp_id']
                writeData.write_yelp_record (business_id, yelp_id)
            else:
                writeData.write_yelp_record (business_id, 'No Match')
                logger.debug ('did not match on business_id:{}'.format(business_id))

        logger.info('---'*20)

        time.sleep(1)


    # close database connection
    globalVars.CONN.close()

    now, endClock = trackTime()

    logger.info ("Main Job Completed at {} in {} seconds".format(now, int(round(endClock-startClock))))
