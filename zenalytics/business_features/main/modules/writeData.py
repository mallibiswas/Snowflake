import snowflake.connector
import json
import os
import pandas as pd
import numpy as np
from modules import initialize
import logging

module_logger = logging.getLogger("mainApp.writeData")

globalVars = initialize.readConfigFile()

def update_business_details(business_profile, out):

    # configure logger
    logger = logging.getLogger("mainApp.writeData.add")

    # connect to DB
    try:
        snowflake_cursor=globalVars.CONN.cursor()
    except Exception as e:
        logger.error ("Error in Snowflake connection {}".format(str(e)))
        raise

    try:
        if out['manual_review'] or out['match_quality'] == 'Zero Results':
            processed = False
        else:
            processed = True

        if snowflake_cursor:
           snowflake_cursor.execute("USE DATABASE {}".format(globalVars.SNOWFLAKE_DATABASE))
           snowflake_cursor.execute("USE SCHEMA {}".format(globalVars.SNOWFLAKE_SCHEMA))
           snowflake_cursor.execute("USE WAREHOUSE {}".format(globalVars.SNOWFLAKE_WAREHOUSE))

           update_query = globalVars.UPDATE_QUERY.format(dbname=globalVars.SNOWFLAKE_DATABASE, \
							schemaname = globalVars.SNOWFLAKE_SCHEMA, \
							tablename = globalVars.OUTPUT_TABLE, \
							business_id = business_profile.get('business_id'), \
           						google = json.dumps(out['google']).replace("\'", "\\'"), \
           						foursquare = json.dumps(out['foursquare']).replace("\'", "\\'"), \
           						match_quality = json.dumps(out['match_quality']), \
           						processed = processed, \
							manual_review = out['manual_review'])

           # TO-DO: Fix case when text fields contains ' or ", which is breaking parse_json
           update_query = update_query.replace("'NULL'", "NULL")

#           logger.info ("update query to execute ...{}".format(update_query))

           snowflake_cursor.execute(update_query);

           logger.info ("Completed ...")
           status='OK'

    except Exception as e: # any error here is severe
        logger.error ("raised exception writing data to Snowflake: {}".format(str(e)))
        status='ERROR'

        
    return status 
