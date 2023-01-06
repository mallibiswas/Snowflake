import psycopg2 
import snowflake.connector
import os
import json
import pandas as pd
import numpy as np
from modules import initialize
import logging
import requests


module_logger = logging.getLogger("mainApp.readData")

globalVars = initialize.readConfigFile()

def readStaticData():

    # configure logger
    logger = logging.getLogger("mainApp.readData.add")

    # Read pre-process data
    preProcessFileName = os.path.join(globalVars.DATA_DIRECTORY, globalVars.PRE_PROCESS_FILE)

    pre_process_data = json.load(open(preProcessFileName))

    # Read place ids
    query_by_place_id = pre_process_data["place_ids"]

    # Read manual processing ids 
    manual_queries = pre_process_data["manual_queries"]

    # exclude business ids from matching
    do_not_match = pre_process_data["do_not_match"]

    # Read parent ids to exclude
    skip_parent = pre_process_data["skip_parent"]

    return {"query_by_place_id":query_by_place_id, "manual_queries":manual_queries, "do_not_match":do_not_match, "skip_parent":skip_parent}


def createBusinessQuery():

    business_query = str(globalVars.BUSINESS_QUERY).format(	dbname=globalVars.SNOWFLAKE_DATABASE, 
								schemaname=globalVars.SNOWFLAKE_SCHEMA, 
								tablename=globalVars.INPUT_TABLE,
								limit=globalVars.GOOGLE_API_LIMIT, 
								offset=globalVars.GOOGLE_API_OFFSET)

    return business_query  

