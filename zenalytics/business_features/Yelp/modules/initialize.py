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

        yelp_api_key_ = f.decrypt(self.configs['Keys']['yelp_api_key'].encode()).decode().strip()
        self.YELP_HEADERS = {'Authorization': 'Bearer %s' % yelp_api_key_}

        self.READ_QUERY = self.configs['Queries']['select_records']
        self.WRITE_QUERY = self.configs['Queries']['insert_category_query']

        self.DATA_DIRECTORY = self.configs['Directories']['data_directory']

        self.INPUT_TABLE = self.configs['Tables']['input_table']
        self.OUTPUT_TABLE = self.configs['Tables']['output_table']

        self.RADIUS = self.configs['Constants']['RADIUS']
        self.LIMIT = self.configs['Constants']['LIMIT']

        self.YELP_MATCH_URL = self.configs['Urls']['yelp_match_url']
        self.YELP_BUSINESS_URL = self.configs['Urls']['yelp_business_url']

        return logger.info ("Initialized variables and modules")
