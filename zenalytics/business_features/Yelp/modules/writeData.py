# -*- coding: utf-8 -*-

import snowflake.connector
import json
import os
import pandas as pd
import numpy as np
from modules import initialize, processData
import logging

module_logger = logging.getLogger("mainApp.writeData")

globalVars = initialize.readConfigFile()

def write_yelp_record (business_id, yelp_id):

    # configure logger
    logger = logging.getLogger("mainApp.writeData.add")

    # connect to DB
    try:
        snowflake_cursor=globalVars.CONN.cursor()
    except Exception as e:
        logger.error ("Error in Snowflake connection {}".format(str(e)))
        raise

    try:
        yelp_businesses = processData.get_yelp_business_results (yelp_id)
    except Exception as e: # any error here is severe
        status=dict({'status':str(e)})
        yelp_dump = status
        logger.error ("Missing Yelp Record for Business Id: {}, status:{}".format(business_Id,str(e)))

    # insert record
    if snowflake_cursor:
       snowflake_cursor.execute("USE DATABASE {}".format(globalVars.SNOWFLAKE_DATABASE))
       snowflake_cursor.execute("USE SCHEMA {}".format(globalVars.SNOWFLAKE_SCHEMA))
       snowflake_cursor.execute("USE WAREHOUSE {}".format(globalVars.SNOWFLAKE_WAREHOUSE))

       dml = globalVars.WRITE_QUERY.format(business_id=business_id,
                                  yelp_id=yelp_id,
                                  yelp_dump=json.dumps(yelp_businesses).replace("\'", "\\'").replace('\"', '\\"'))

       snowflake_cursor.execute(dml);
       snowflake_cursor.close();
       status=dict({'status':'ok'})


    return status
