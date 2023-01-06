#/!bin/python3.x

# -*- coding: utf-8 -*-
"""
Created on Mon May 20 14:09:30 2019
@author: mallinath.biswas
"""

#!/anaconda3/bin/python3.5

# Note: Run using Python 3.x
#

import pandas as pd
import time

from modules import initialize
from modules import readData 
from modules import writeData 

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
    
    return now, time.clock()


#########################
# Main program
#########################

if __name__ == '__main__':

    # setup logger, propagate=True for printing to terminal 
    logger = setLogger('logging.conf', propagate=False)    
    
    now, startClock = trackTime()
    
    logger.info ("Start execution at {}".format(now))
    
    # initialize global variables
    globalVars = initialize.readConfigFile() # set global variables

    # read from Cisco Meraki Dashboard API 
    df = readData.readCiscoMerakiData()

#    logger.info (df.info())

    # write to Snowflake 
    writeData.writeCiscoMerakiData(df)
    
    now, endClock = trackTime()    
    
    logger.info ("Main Job Completed at {} in {} seconds".format(now, int(round(endClock-startClock))))
