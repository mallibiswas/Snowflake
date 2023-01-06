import psycopg2 
from meraki.meraki import Meraki
import snowflake.connector
import json
import pandas as pd
import numpy as np
from modules import initialize
import logging

module_logger = logging.getLogger("mainApp.readData")

globalVars = initialize.readConfigFile()

def readCiscoMerakiData():
    
    # configure logger
    logger = logging.getLogger("mainApp.readData.add")

    # connect and download from Cisco Meraki Dashboard API	
    try:
        client = Meraki(globalVars.CISCO_MERAKI_API_KEY)
        organizations_controller = client.organizations
        result = organizations_controller.get_organization_device_statuses(globalVars.CISCO_MERAKI_ORG_ID)
        df = pd.DataFrame(result)
        df['asof_date']=pd.datetime.now()

    except Exception as e: # any error here is severe
        logger.error ("raised exception reading df from Cisco-Meraki Dashboard API: {}".format(str(e)))

        
    return df
