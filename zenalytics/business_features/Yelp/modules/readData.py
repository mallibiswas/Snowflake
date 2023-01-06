# -*- coding: utf-8 -*-

from modules import initialize
import logging
import requests


module_logger = logging.getLogger("mainApp.readData")

globalVars = initialize.readConfigFile()


def createReadQuery():

    read_query = str(globalVars.READ_QUERY)

    return read_query
