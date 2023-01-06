# -*- coding: utf-8 -*-
"""
Created on Fri Mar  2 12:30:46 2018
@author: mallinath.biswas
"""

import json
import datetime
import sys
import configparser
import logging
import base64
from cryptography.fernet import Fernet
import snowflake.connector


module_logger = logging.getLogger("mainApp.initialize")

class readConfigFile():
    
    """
        This class reads inputs from the command line and processes the configuration files to setup global variables for the rest of the code
    """    
    
    def __init__(self):

        # configure logger
        logger = logging.getLogger("mainApp.initialize.add")
        
        if len(sys.argv) < 3:
            logger.error ("Invalid Arguments")
            logger.error ("usage: python main.py config.ini 12345")
            sys.exit()
            
        self.configFile = sys.argv[1] 
	
        self.CURRENT_DATETIME = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')

        f = Fernet(sys.argv[2].encode())
                        
        self.configs = configparser.ConfigParser()
        self.configs.read(self.configFile)

        account_ = self.configs['Connections']['Account']
        user_ = self.configs['Connections']['User']
        database_ = self.configs['Connections']['Database']
        warehouse_ = self.configs['Connections']['Warehouse']
        schema_ = self.configs['Connections']['Schema']

        # decrypt variables and convert binary to ASCII	
        password_ = f.decrypt(self.configs['Connections']['Password'].encode()).decode().strip()

        self.CONN = snowflake.connector.connect( user=user_, password=password_, account=account_, warehouse=warehouse_, database=database_, schema=schema_)

        self.SNOWFLAKE_ACCOUNT = account_ 
        self.SNOWFLAKE_USER = user_ 
        self.SNOWFLAKE_PASSWORD = password_ 
        self.SNOWFLAKE_DATABASE = database_ 
        self.SNOWFLAKE_WAREHOUSE = warehouse_ 
        self.SNOWFLAKE_SCHEMA = schema_ 

        foursquare_client_id_ = f.decrypt(self.configs['Keys']['foursquare_client_id'].encode()).decode().strip()
        foursquare_client_secret_ = f.decrypt(self.configs['Keys']['foursquare_client_secret'].encode()).decode().strip()
        foursquare_version_ = f.decrypt(self.configs['Keys']['foursquare_version'].encode()).decode().strip()

        self.FOURSQUARE_API = {'foursquare_client_id': foursquare_client_id_, 'foursquare_client_secret': foursquare_client_secret_, 'foursquare_version': foursquare_version_}

        self.GOOGLE_API_KEY = f.decrypt(self.configs['Keys']['google_api_key'].encode()).decode().strip()
        self.GOOGLE_API_LIMIT = self.configs['Constants']['google_api_limit']
        self.GOOGLE_API_OFFSET = self.configs['Constants']['google_api_offset']

        self.GOOGLE_MAPS_PLACES_API_URL = self.configs['Urls']['google_maps_places_api_url']
        self.GOOGLE_MAPS_TIMEZONE_API_URL = self.configs['Urls']['google_maps_timezone_api_url']
        self.FOURSQUARE_VENUES_SEARCH_API_URL = self.configs['Urls']['foursquare_venues_search_api_url']

        self.GOOGLE_MAPS_PLACES_API_FIELDS = self.configs['Fields']['google_maps_places_api_fields']

        self.BUSINESS_QUERY = self.configs['Queries']['business_query']
        self.UPDATE_QUERY = self.configs['Queries']['update_query']

        self.DATA_DIRECTORY = self.configs['Directories']['data_directory']

        self.INPUT_TABLE = self.configs['Tables']['input_table']
        self.OUTPUT_TABLE = self.configs['Tables']['output_table']
        self.OUTPUT_FILE = self.configs['Files']['output_file']
        self.PRE_PROCESS_FILE = self.configs['Files']['pre_process_file']

        return logger.info ("Initialized variables and modules")
