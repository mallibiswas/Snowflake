# -*- coding: utf-8 -*-

import json
import datetime
import sys
import configparser
import logging
import base64
from cryptography.fernet import Fernet
import snowflake.connector
import foursquare

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
                        
        self.configs = configparser.ConfigParser(allow_no_value=True)
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
        
        self.FOURSQUARE_CLIENT = foursquare.Foursquare(client_id=foursquare_client_id_, client_secret=foursquare_client_secret_, version=foursquare_version_)

        self.READ_QUERY = self.configs['Queries']['select_records']
        self.WRITE_QUERY = self.configs['Queries']['insert_category_query']

        self.DATA_DIRECTORY = self.configs['Directories']['data_directory']

        self.INPUT_TABLE = self.configs['Tables']['input_table']
        self.OUTPUT_TABLE = self.configs['Tables']['output_table']

        self.RADIUS = self.configs['Constants']['RADIUS']
        self.LIMIT = self.configs['Constants']['LIMIT']
        
        return logger.info ("Initialized variables and modules")
