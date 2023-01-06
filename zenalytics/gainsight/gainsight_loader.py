#!/usr/bin/env python
#########################################################################################
# Aptrinsic data loader
# Loads user or customer data using Aptrinsic's REST api
# v1.0
# 11/2018
#########################################################################################

from __future__ import print_function
import csv
import sys
import optparse
import os
import os.path
import datetime
import json
import requests
import dateutil.parser as dparser
from datetime import datetime
from distutils.util import strtobool
import ast
import os

# If you get an error on the above statement, install the requests library with this command: 
#   pip install requests

ENDPOINT_DEV = "http://api-dev.aptrinsic.com/v1" 
ENDPOINT_PROD = "http://api.aptrinsic.com/v1" 

USER_TYPE = "USER"
ACCOUNT_TYPE = "ACCOUNT"
CUSTOM_EVENT = "CUSTOM_EVENT"
VALID_TYPES = [USER_TYPE, ACCOUNT_TYPE,CUSTOM_EVENT]
DATATYPE_DATE = "DATE_TIME"
DATATYPE_NUMBER = "NUMBER"
DATATYPE_BOOLEAN = "BOOLEAN"
headers = {
        'content-type': 'application/json', 
        'X-APTRINSIC-API-KEY': None
    }
INFO_BY_TYPE = {
    USER_TYPE: {
        'keyField': 'identifyId',
        'endpoint': "users",
        'metadataEndPoint' : "user",
        'fieldNames': [
            "aptrinsicId",
            "identifyId",
            "type",
            "gender",
            "email",
            "firstName",
            "lastName",
            "lastSeenDate",
            "signUpDate",
            "firstVisitDate",
            "title",
            "phone",
            "score",
            "role",
            "subscriptionId",
            "accountId",
            "numberOfVisits",
            "location.city",
            "location.stateCode",
            "location.countryCode",
            "location.timeZone",
            "location.coordinates.latitude",
            "location.coordinates.longitude",
            "createDate",
            "lastModifiedDate",
            "customAttributes",
            "globalUnsubscribe",
            "sfdcContactId"
        ],
        "apiNameMapping" :{
            "id" : "identifyId"
        }
    },
    ACCOUNT_TYPE: {
        'keyField': 'id',
        'endpoint': "accounts",
        'metadataEndPoint' : "account",
        'fieldNames': [
            "id",
            "name",
            "trackedSubscriptionId",
            "sfdcId",
            "lastSeenDate",
            "dunsNumber",
            "industry",
            "numberOfEmployees",
            "sicCode",
            "website",
            "naicsCode",
            "plan",
            "location",
            "createDate",
            "lastModifiedDate",
            "customAttributes"
        ],
        "apiNameMapping" :{}
    },
    CUSTOM_EVENT : {
        'keyField': 'identifyId',
        'endpoint' : "events/custom",
        'fieldNames' : [
            "identifyId",
            "eventName",
            "date",
            "attributes",
            "accountId",
            "url",
            "referrer",
            "remoteHost"
        ],
        "apiNameMapping" :{}
    }
}

def getBaseURL(env):
    baseurl = None
    if env.upper() == "PROD":
        baseurl = ENDPOINT_PROD
    else:
        baseurl = ENDPOINT_DEV
    return baseurl

def loadFieldTypeMapping(datatype, env, headers):
    datatypeMapping = {}
    if dataType.upper() == CUSTOM_EVENT:
        # No metadata endpoint for custom events
        return datatypeMapping
    baseurl = getBaseURL(env)
    metadataendpoint = baseurl + "/admin/model/" + INFO_BY_TYPE[datatype].get("metadataEndPoint") + "/attributes"
    response = requests.get(metadataendpoint,headers=headers)
    if response.status_code == 200:
        fields = json.loads(response.text)
        datatypeMapping = dict((fld.get("apiName"),fld.get("type")) for fld in fields)
    else : 
        print("\n Error %d while fetching meta data : '%s'" % (response.status_code, response.text), file=sys.stderr)
    return datatypeMapping
    

def loadConfig(configFile):
    print("Loading config from '%s'" % (configFile.name))
    config = json.load(configFile)
    return config
def writeToErrorFile(errorFile,csvRow,errorResponse):
    errorResponseJSON = json.loads(errorResponse)
    csvRow["error_text_gs"] = errorResponseJSON["externalapierror"]["subErrors"][0]["message"]
    ef=open(errorFile, "a+")
    ef.write('"' + ('","'.join(csvRow.values())) + '"\n')
    ef.close()

def validateConfig(config, dataType):
    # Validate required fields
    for fieldName in ["apiKey","productKey","fieldMapping"]:
        if not config.get(fieldName):
            raise Exception("Missing required field '%s' in '%s'" % (fieldName, configFile.name))

    fieldMapping = config.get('fieldMapping')
    if not isinstance(fieldMapping, dict) or not fieldMapping.get(dataType) or not isinstance(fieldMapping.get(dataType), dict):
        raise Exception("Invalid fieldMapping value '%s' in '%s'. Must be an object with an entry for %s" % (config.get('fieldMapping'), configFile.name, dataType))

    dataTypeFieldMapping = fieldMapping.get(dataType)
    if len(dataTypeFieldMapping.items()) < 2:
        raise Exception("Invalid fieldMapping value '%s' in '%s'. Must be an object with at least two entries" % (dataTypeFieldMapping, configFile.name))

    keyField = INFO_BY_TYPE[dataType].get('keyField')
    if keyField not in dataTypeFieldMapping.keys():
        raise Exception("Invalid fieldMapping value '%s' in '%s'. Missing required key field '%s'" % (dataTypeFieldMapping, configFile.name, keyField))

    # Validate non-key fields
    for fieldName in dataTypeFieldMapping.keys():
        if fieldName not in INFO_BY_TYPE[dataType].get('fieldNames') and not (fieldName.startswith("customAttributes.") or fieldName.startswith("attributes.")) :
            raise Exception("Unknown field '%s' in fieldMapping in '%s'.  Valid Values: '%s'" % 
                    (fieldName, configFile.name, INFO_BY_TYPE[dataType].get('fieldNames')))

def loadEventsData(endpoint,headers,event):
    event["userType"] = "USER"
    event['propertyKey'] = config['productKey']
    eventJson = json.dumps(event)
    return requests.post(endpoint, headers=headers, data=eventJson)
            
def loadData(config, inputFile, startRow, lastRow, dataType, insertMissing, verbose, dryRun):
    print("Loading %s data from '%s'" % (dataType, inputFile.name))
    errorfile = os.path.splitext(os.path.basename(inputFile.name))[0] + "_error.txt"
    errorCounter = skippedCounter = updatedCounter = insertedCounter = 0
    csvReader = csv.DictReader(inputFile)
    keyField = INFO_BY_TYPE[dataType].get('keyField')
    env = config.get("env")
    if env is None:
        env = "PROD"
    endpoint = getBaseURL(env)
    endpointSuffix = INFO_BY_TYPE[dataType].get('endpoint')
    endpoint = endpoint + "/" + endpointSuffix
    if (startRow > 0):
        print(("Skipping to row %d" % startRow), file=sys.stderr)
    headers['X-APTRINSIC-API-KEY'] = config.get('apiKey')
    datatypeMapping = loadFieldTypeMapping(dataType, env, headers)
    #print("Data Type Mapping: " + str(datatypeMapping))
    for (rowIndex,csvRow) in enumerate(csvReader):
        if rowIndex < startRow:
            continue
        if lastRow != 0 and rowIndex > lastRow:
            print("Stopping after %d" % (lastRow))
            break
        updateData = mapRecord(csvRow, config.get('fieldMapping').get(dataType),dataType,datatypeMapping)
        print(json.dumps(updateData))
        if not updateData.get(keyField):
            print("\nRow: %d Error Missing %s value: '%s'" % (rowIndex, keyField, updateData), file=sys.stderr)
            break
                
        uniqueId = updateData[keyField]
        updateEndpoint = "%s/%s" % (endpoint, uniqueId)

        if dryRun:
            print("DRYRUN: Request: '%s' Data: '%s'" % (endpoint, updateJson))
        else:
            if dataType.upper() == CUSTOM_EVENT:
                updateJson = json.dumps(updateData)
                response = loadEventsData(endpoint,headers,updateData)
                if response.status_code != 201:
                    print(("Error %d : '%s' on %s" % (response.status_code, response.text, updateData)), file=sys.stderr)
                    print(("Error CSV Row : %s" % (response.status_code, response.text, csvRow)), file=sys.stderr)
                    errorCounter += 1
                    writeToErrorFile(errorfile,csvRow,response.text)
                    sys.stderr.flush()
                else :
                    print("Inserted custom event %s" % (updateJson))
                    insertedCounter += 1
            else:
                updateData['propertyKeys'] = config['productKey'] if isinstance(config['productKey'],list) else [config['productKey']]
                updateJson = json.dumps(updateData)
                response = requests.put(updateEndpoint, headers=headers, data=updateJson)
                if (response.status_code != 204):
                    if (response.status_code == 404):
                        if insertMissing:
                            # Do insert since update failed
                            insertData = updateData
                            insertJson = json.dumps(insertData)
                            insertResponse = requests.post(endpoint, headers=headers, data=insertJson)
                            if (insertResponse.status_code != 201):
                                print("\nRow: %d Error %d on insert : '%s' on %s" % (rowIndex,insertResponse.status_code, insertResponse.text, insertJson), file=sys.stderr)
                                errorCounter += 1
                                writeToErrorFile(errorfile,csvRow,response.text)
                            else:
                                insertedCounter += 1
                                if verbose:
                                    print("Inserted record for '%s'" % (uniqueId))
                        else:
                            print("\nRow: %d Skipping update, no match found for %s==%s" % (rowIndex, keyField, uniqueId ), file=sys.stderr)
                            skippedCounter += 1
                    else:
                        print("\nRow: %d Error %d : '%s' on %s" % (rowIndex,response.status_code, response.text, updateJson), file=sys.stderr)
                        errorCounter += 1
                        writeToErrorFile(errorfile,csvRow,response.text)
                    sys.stderr.flush()
                else:
                    updatedCounter += 1
                    if verbose:
                        print("Updated record for '%s'" % (uniqueId))

        sys.stdout.flush()

    print("\nDONE%s: %d updated, %d inserted, %d skipped, %d errors" % ((" (DRYRUN) " if dryRun else ""),updatedCounter, insertedCounter, skippedCounter, errorCounter))
    return

def mapRecord(csvRow, fieldMapping,dataType,datatypeMapping):
    # Return an object built using data from csvRow with Aptrinsic field names from mapping data
    record = {}
    stdDateValue = datetime(1970, 1, 1, 0, 0, 0)
    for mapping in fieldMapping.items():
        aptrinsicFieldname = mapping[0]
        sourceFieldname = mapping[1]
        if sourceFieldname in csvRow:
            fieldValue = csvRow[sourceFieldname]
            apAPINamearr = aptrinsicFieldname.split(".")
            apAPIName = apAPINamearr[len(apAPINamearr)-1]
            if apAPIName in INFO_BY_TYPE[dataType]["apiNameMapping"]:
                apAPIName = INFO_BY_TYPE[dataType]["apiNameMapping"][apAPIName]
            if fieldValue is not None and len(fieldValue) == 0 and aptrinsicFieldname in datatypeMapping :
                continue
            if apAPIName in datatypeMapping:
                if datatypeMapping.get(apAPIName).upper() == DATATYPE_DATE:
                    try:
                        dateValue = dparser.parse(fieldValue,fuzzy=False)
                        fieldValue = int((dateValue - stdDateValue).total_seconds())*1000
                    except:
                        pass
                elif datatypeMapping.get(apAPIName).upper() == DATATYPE_NUMBER:
                    try:
                        fieldValue = ast.literal_eval(fieldValue)
                    except:
                        pass
                elif datatypeMapping.get(apAPIName).upper() == DATATYPE_BOOLEAN:
                    try:
                        if strtobool(str(fieldValue)):
                            fieldValue = "true"
                        else:
                            fieldValue = "false"
                    except:
                        pass
            if "." in aptrinsicFieldname:
                # Field is nested in object
                fieldNames = aptrinsicFieldname.split('.')
                nestedObject = record.get(fieldNames[0],{}) 
                record[fieldNames[0]] = nestedObject 
                for index,fieldName in enumerate(fieldNames[1:]):
                    if index == len(fieldNames)-2:
                        nestedObject[fieldName] = fieldValue
                    else:
                        nestedObject[fieldName] = nestedObject.get(fieldName,{})
                        nestedObject = nestedObject[fieldName]
            else:
                record[aptrinsicFieldname] = fieldValue 

    return record


if __name__ == "__main__":
    usage = """usage: %prog [options] config_file [USER|ACCOUNT|CUSTOM_EVENT] input_file
Example:
    %prog config.json USER input.csv 2> errors.log | tee output.log

Config File Example:
    {
      "apiKey" : "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX",
      "productKey" : "AP-XXXXXXXXXXXX-2",
      "fieldMapping" : {
        "USER" : {
          "identifyId" : "id",
          "title" : "title",
          "phone" : "telephone",
          "location.city" : "city",
          "location.stateCode" : "state",
          "customAttributes.strtmp1" : "strtmp1"
        },
        "ACCOUNT" : {
          "id" : "id",
          "name" : "account_name"
        },
        "CUSTOM_EVENT" :{
          "identifyId" : "User_Id",
          "eventName" : "Event_Name",
          "date" : "Time",
          "url":"current_url",
          "referrer":"referrer",
          "remoteHost":"referring_domain",
          "attributes.Organisation" : "Organisation",
          "attributes.FromTo" : "FromTo"
        }
      }
    } 
"""

    parser = optparse.OptionParser(usage=usage)
    parser.add_option("-n", dest="startRow", type=int, default=0, help="Start at line number, Default: %default")
    parser.add_option("-l", dest="lastRow", type=int, default=0, help="Stop at line number, Default: %default")
    parser.add_option("-i", "--insertMissing", dest="insertMissing", action="store_true", 
            default=False, help="If set, will insert records that do not match to existing records. Default: %default")
    parser.add_option("-d", "--dryRun", dest="dryRun", action="store_true", default=False, help="If set, will not insert/update data Default: %default")
    parser.add_option("-v", "--verbose", dest="verbose", action="store_true", default=False, help="If set, enables verbose logging. Default: %default")
    (opts, args) = parser.parse_args()

    if len(args) != 3:
        print("Incorrect number of arguments: %d" % len(args))
        parser.print_help()
        sys.exit(1)

    configFilename = args[0]
    dataType = args[1]
    inputFilename = args[2]
    startRow = opts.startRow
    lastRow = opts.lastRow
    insertMissing = opts.insertMissing
    verbose = opts.verbose
    dryRun = opts.dryRun

    if not os.path.isfile(configFilename):
        print("ERROR: Unable to find configFile at '%s'" % (configFilename), file=sys.stderr)
        parser.print_help()
        sys.exit(1)

    if not os.path.isfile(inputFilename):
        print("ERROR: Unable to find inputFile at '%s'" % (inputFilename), file=sys.stderr)
        parser.print_help()
        sys.exit(1)

    if dataType not in VALID_TYPES:
        print("ERROR: Invalid data type: '%s', must be one of '%s'" % (dataType, VALID_TYPES), file=sys.stderr)
        parser.print_help()
        sys.exit(1)

    configFile = open(configFilename,'r')
    inputFile = open(inputFilename,'r')

    config = loadConfig(configFile)
    try:
        validateConfig(config, dataType)
    except Exception as e:
        print("ERROR: %s" % (e), file=sys.stderr)
        sys.exit(1)
    loadData(config, inputFile, startRow, lastRow, dataType, insertMissing, verbose, dryRun)

