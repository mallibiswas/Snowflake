import snowflake.connector
import json
import os
import pandas as pd
import numpy as np
from modules import initialize
import logging

module_logger = logging.getLogger("mainApp.writeData")

globalVars = initialize.readConfigFile()

def writeCloudtraxOpenmeshData(df):
    
    # configure logger
    logger = logging.getLogger("mainApp.writeData.add")

    # write to file.json
    outFile=os.path.join(globalVars.DATA_DIRECTORY, globalVars.OUTPUT_FILE)
    logger.info ("Writing data to {}".format(str(outFile)))

    #df.to_json(outFile,orient='records')
    df.to_csv(outFile, sep='|', index = None, header=True)

    # connect to DB
    try:
        conn = snowflake.connector.connect(account=globalVars.SNOWFLAKE_ACCOUNT,user=globalVars.SNOWFLAKE_USER,password=globalVars.SNOWFLAKE_PASSWORD)            
    except Exception as e:
        logger.error ("Error in Snowflake connection {}".format(str(e)))
        raise

    try:
        logger.info ("data directory = {}".format(globalVars.DATA_DIRECTORY))
        snowflake_cursor = conn.cursor()
        logger.info ("Connected to Snowflake...")

        if snowflake_cursor:
           snowflake_cursor.execute("USE DATABASE {}".format(globalVars.SNOWFLAKE_DATABASE))
           snowflake_cursor.execute("USE SCHEMA {}".format(globalVars.SNOWFLAKE_SCHEMA))
           snowflake_cursor.execute("USE WAREHOUSE {}".format(globalVars.SNOWFLAKE_WAREHOUSE))
           logger.info ("Writing data to table stage ...")
           snowflake_cursor.execute("PUT file://{} @%{}".format(outFile, globalVars.OUTPUT_TABLE))
           logger.info ("Loading into Snowflake {}  ...".format(globalVars.OUTPUT_TABLE))
           snowflake_cursor.execute("COPY INTO {} FILE_FORMAT=(type = csv field_delimiter = '|' skip_header = 1)".format(globalVars.OUTPUT_TABLE));
           logger.info ("Cleaning up table stage ...")
           snowflake_cursor.execute("REMOVE @%{}".format(globalVars.OUTPUT_TABLE));
           logger.info ("Completed ...")

    except Exception as e: # any error here is severe
        logger.error ("raised exception writing df to Snowflake: {}".format(str(e)))
    finally:
        conn.close()

        
    return df
