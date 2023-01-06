#/!bin/python3.x

# -*- coding: utf-8 -*-
"""
Created on July 20 2020 14:09:30 2019
@author: mallinath.biswas
@deployed: mallinath.biswas
"""

#!/anaconda3/bin/python3.5

# Note: Run using Python 3.x
#

import pandas as pd
import time
import snowflake.connector
import json

from modules import initialize
from modules import readData
from modules import processData

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

    # Fetch snowflake data
    snow_df = pd.read_sql(readData.createReadQuery(), globalVars.CONN)

    logger.info ('Print snow_df.head:{}'.format(snow_df.head()))

    for index, row in snow_df.iterrows():

        business_id=row["BUSINESS_ID"]
        parent_name=row["PARENT_NAME"]
        business_name=row["BUSINESS_NAME"]
        street=row["STREET"]
        city=row["CITY"]
        state=row["STATE"]
        country=row["COUNTRY"]
        zipcode=row["ZIPCODE"]
        latitude=row["LATITUDE"]
        longitude=row["LONGITUDE"]

        # get foursquare results

        logger.info ('Searching for business id:{}'.format(business_id))

        is_match, foursquare_id, foursquare_output = processData.foursquare_match (parent_name,business_name,street,city,state,country,zipcode,latitude,longitude)

        logger.info ('is match:{0}, foursquare_id:{1}, foursquare_output:{2}'.format(is_match,foursquare_id,foursquare_output))

        if is_match:
            is_venues, foursquare_categories = processData.foursquare_venues (foursquare_id)
            if is_venues:

                logger.info ('Categories found:{}'.format(foursquare_categories))

                # insert record
                wq=str(globalVars.WRITE_QUERY)
                dml = wq.format(business_id=business_id,
                              foursquare_id=foursquare_id,
                              foursquare_dump=json.dumps(foursquare_categories).replace("\'", "\\'").replace('\"', '\\"'))

                logger.info ('DML:{}'.format(dml))

                globalVars.CONN.cursor().execute(dml);

        else:
            logger.info ('No Match for business id:{}'.format(business_id))
            # insert record
            wq=str(globalVars.WRITE_QUERY)
            dml = wq.format(business_id=business_id,
                          foursquare_id='No Match',
                          foursquare_dump='')

            globalVars.CONN.cursor().execute(dml);

        time.sleep(1)

        logger.info('---'*20)

        time.sleep(1)

    # close database connection
    globalVars.CONN.close()

    now, endClock = trackTime()

    logger.info ("Main Job Completed at {} in {} seconds".format(now, int(round(endClock-startClock))))
