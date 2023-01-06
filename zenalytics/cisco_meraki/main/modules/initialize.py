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
from Crypto.Cipher import AES
import base64

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

        cipher = AES.new(sys.argv[2],AES.MODE_ECB) # never use ECB in strong systems obviously
                        
        self.configs = configparser.ConfigParser()
        self.configs.read(self.configFile)

        account_ = self.configs['Connections']['Account']
        user_ = self.configs['Connections']['User']
        database_ = self.configs['Connections']['Database']
        warehouse_ = self.configs['Connections']['Warehouse']
        schema_ = self.configs['Connections']['Schema']

        # decrypt variables and convert binary to ASCII	
        password_ = cipher.decrypt(base64.b64decode(self.configs['Connections']['Password'])).strip().decode('ascii')

        self.DBCONNECTSTR = "account={Account}, user={User}, password={Password} , warehouse={Warehouse}, database={Database}, schema={Schema}". \
				format(Account=account_,User=user_,Password=password_,Database=database_,Warehouse=warehouse_,Schema=schema_)

        self.SNOWFLAKE_ACCOUNT = account_ 
        self.SNOWFLAKE_USER = user_ 
        self.SNOWFLAKE_PASSWORD = password_ 
        self.SNOWFLAKE_DATABASE = database_ 
        self.SNOWFLAKE_WAREHOUSE = warehouse_ 
        self.SNOWFLAKE_SCHEMA = schema_ 

        self.CISCO_MERAKI_API_KEY = cipher.decrypt(base64.b64decode(self.configs['Connections']['Cisco_meraki_api_key'])).strip().decode('ascii')
        self.CISCO_MERAKI_ORG_ID = cipher.decrypt(base64.b64decode(self.configs['Connections']['Cisco_meraki_org_id'])).strip().decode('ascii')

        self.DATA_DIRECTORY = self.configs['Directories']['Data_directory']
        self.OUTPUT_TABLE = self.configs['Tables']['Output_table']
        self.OUTPUT_FILE = self.configs['Files']['Output_file']

        return logger.info ("Initialized variables and modules")
