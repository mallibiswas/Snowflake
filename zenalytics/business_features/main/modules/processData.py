import psycopg2 
import os
import json
import pandas as pd
import numpy as np
from modules import initialize
import logging
import requests
from fuzzywuzzy import fuzz

module_logger = logging.getLogger("mainApp.processData")

globalVars = initialize.readConfigFile()


def create_google_places_query(business_profile, use_parent=True):

    # configure logger
    logger = logging.getLogger("mainApp.processData.add")

    # Note: Contrary to expectation including the street address in the Google query
    # actually generated a lot more spurious matches.
    query_string = '{0} {1} {2} {3}'.format(business_profile.get('name', ''),
                                                business_profile.get('city', ''),
                                                business_profile.get('state', ''),
                                                business_profile.get('zipcode',''))

    if use_parent and business_profile.get('parent_name','') is not None:
        query_string = '{0} {1}'.format(query_string, business_profile.get('parent_name',''))
    
    query_string = query_string.replace("#", '%23')
    query_string = query_string.replace(' ', '%20')

    logger.debug('create_google_places_query:{}'.format(query_string))

    return query_string



def choose_google_closest_match(candidates, zname, zaddress):

    # configure logger
    logger = logging.getLogger("mainApp.processData.add")

    max_partial_ratio_name = fuzz.partial_ratio(candidates[0].get('name', '').lower(), zname)
    max_partial_ratio_address = fuzz.partial_ratio(candidates[0].get('formatted_address', '').lower(), zaddress)
    max_index = 0

    for i in range(1,len(candidates)):
        partial_ratio_name = fuzz.partial_ratio(candidates[i].get('name', '').lower(), zname)
        partial_ratio_address = fuzz.partial_ratio(candidates[i].get('formatted_address', '').lower(), zaddress)
        if partial_ratio_name > max_partial_ratio_name and partial_ratio_address > max_partial_ratio_address:
            max_index = i

    logger.debug('choose_google_closest_match match_Index = {}'.format(candidates[max_index]))

    return candidates[max_index]

    
def get_google_places_data(business_profile, use_parent=True):

    # configure logger
    logger = logging.getLogger("mainApp.processData.add")

    business_query = create_google_places_query(business_profile, use_parent)

    logger.debug('get_google_places_data business_profile = {}'.format(business_profile))
    logger.debug('get_google_places_data business_query = {}'.format(business_query))

    z_address = "{}, {}, {}, {}".format(business_profile.get('street'), 
                           business_profile.get('city'),
                           business_profile.get('state'),
                           business_profile.get('zipcode'))
    
    # Query Google Places API
    if business_profile.get('google_place_id') is not None:

        places_req = ("{0}/details/json?placeid={1}&key={2}&{3}").format(globalVars.GOOGLE_MAPS_PLACES_API_URL, 
                                                                         business_profile['google_place_id'],
                                                                         globalVars.GOOGLE_API_KEY,
                                                                         globalVars.GOOGLE_MAPS_PLACES_API_FIELDS) 
    else:
        places_req = ("{0}/findplacefromtext/json?input={1}&inputtype=textquery&key={2}&{3}").format(globalVars.GOOGLE_MAPS_PLACES_API_URL,
                                                                                                     business_query,
                                                                                                     globalVars.GOOGLE_API_KEY,
                                                                                                     globalVars.GOOGLE_MAPS_PLACES_API_FIELDS)  
    
    logger.debug('get_google_places_data places_req = {}'.format(places_req))

    places_res = json.loads(requests.get(places_req).text)

    logger.debug('get_google_places_data places_response = {}'.format(places_res))

    if len(places_res.get('candidates', [])) == 1:
        place = places_res['candidates'][0]
    elif len(places_res.get('candidates', [])) > 1:
        place = choose_google_closest_match(places_res['candidates'], business_profile.get('name'), z_address)
    elif len(places_res.get('result', [])) > 0:
        place = places_res.get('result')
    else:
        return {}, True, places_res['status']
        

    name_match_quality = fuzz.partial_ratio(place.get('name').lower(), business_profile.get('name'))
    
    address_match_quality = fuzz.partial_ratio(place.get('formatted_address').lower(), z_address)

    match_quality = {'address': address_match_quality, 'name': name_match_quality}
    
    # When google places returns a municipality, entry requires manual review
    if place.get('types') == ['locality', 'political']\
    or address_match_quality < 70:
        manual_review = True
    else:
        manual_review = False
        
    place['maps_url'] = ('https://www.google.com/maps/search/?api=1&query={0}&query_place_id={1}').format(business_query, place['place_id'])
    
    logger.debug('get_google_places_data place = {}'.format(place))
    logger.debug('get_google_places_data manual_review = {}'.format(manual_review))
    logger.debug('get_google_places_data match_quality = {}'.format(match_quality))

    return place, manual_review, match_quality


def get_google_timezone(google_place):

    # configure logger
    logger = logging.getLogger("mainApp.processData.add")

    # note: timezone request fixed to specific date for daylight savings purposes, review if necessary
    lat = google_place.get('geometry',{}).get('location',{}).get('lat', 'NULL')
    lng = google_place.get('geometry',{}).get('location',{}).get('lng', 'NULL')

    timezone_res = ("https://maps.googleapis.com/maps/api/timezone/json?location="
                   "{0},{1}&timestamp=1458000000"
                   "&key={2}").format(lat, lng, globalVars.GOOGLE_API_KEY)    
    timezone_res = json.loads(requests.get(timezone_res).text)
    timezone_id = timezone_res.get("timeZoneId", 'NULL')

    logger.debug('get_google_timezone timezone_id = {}'.format(timezone_id))

    return timezone_id


def create_foursquare_venue_request(google_place, business_name=None):

    # configure logger
    logger = logging.getLogger("mainApp.processData.add")

    '''
    Optionally provide a business_profile. If provided, company name from the business profile will be used
    rather than the google places company name.
    '''
    if business_name is None:
        business_name = google_place.get('name')
    
    foursquare_req = '{0}/search?ll={1},{2}&query={3}&client_id={4}&client_secret={5}&v={6}&radius=100&intent=browse'.format(
            globalVars.FOURSQUARE_VENUES_SEARCH_API_URL, 
            str(google_place.get('geometry').get('location').get('lat')),
            str(google_place.get('geometry').get('location').get('lng')),
            business_name,
            globalVars.FOURSQUARE_API['foursquare_client_id'],
            globalVars.FOURSQUARE_API['foursquare_client_secret'],
            globalVars.FOURSQUARE_API['foursquare_version']
        )  

    logger.debug('create_foursquare_venue_request foursquare_req = {}'.format(foursquare_req))

    return foursquare_req


def create_foursquare_hours_request(foursquare_venue_id):

    # configure logger
    logger = logging.getLogger("mainApp.processData.add")

    '''
    Optionally provide a business_profile. If provided, company name from the business profile will be used
    rather than the google places company name.
    '''

    foursquare_req = '{0}/{1}/hours?client_id={2}&client_secret={3}&v={4}'.format(
            globalVars.FOURSQUARE_VENUES_SEARCH_API_URL, 
            foursquare_venue_id,
            globalVars.FOURSQUARE_API['foursquare_client_id'],
            globalVars.FOURSQUARE_API['foursquare_client_secret'],
            globalVars.FOURSQUARE_API['foursquare_version']
        )  

    logger.debug('create_foursquare_hours_request foursquare_req = {}'.format(foursquare_req))

    return foursquare_req    

def get_foursquare_data(google_place, business_profile):

    # configure logger
    logger = logging.getLogger("mainApp.processData.add")

    foursquare_req = create_foursquare_venue_request(google_place)
    foursquare_resp_search = json.loads(requests.get(foursquare_req).text)
    
    response_200 = foursquare_resp_search.get('meta',{}).get('code') == 200
    count_venues_returned = len(foursquare_resp_search.get('response',{}).get('venues',[]))

    # If Foursquare does not return any venues using the name in Z database
    if response_200 and count_venues_returned == 0:
        foursquare_req = create_foursquare_venue_request(google_place, business_profile['name'])
        foursquare_resp_search = json.loads(requests.get(foursquare_req).text)
        
    venues = foursquare_resp_search.get('response',{}).get('venues',[{}])

    if venues == []: venues = [{}]
    venue = venues[0]
    foursquare_venue_id = venue.get('id', None)
    foursquare_name = venue.get('name', None)
    foursquare_categories = venue.get('categories',[{'name': None, 'primary': True}])

    if len(foursquare_categories) == 0: foursquare_categories = [{'name': None, 'primary': True}]
    foursquare_primary_category = [c for c in foursquare_categories if c.get('primary', False)]
    foursquare_primary_category = foursquare_primary_category[0].get('name') 
    # drop extraneous category details, just keep list of names
    foursquare_categories = [c.get('name') for c in foursquare_categories]
    
    if foursquare_venue_id is not None:
        foursquare_req = create_foursquare_hours_request(foursquare_venue_id)
        foursquare_resp_hours = json.loads(requests.get(foursquare_req).text)
        foursquare_hours = foursquare_resp_hours.get('response',{}).get('hours', {})
    else:
        foursquare_resp_hours = None
        foursquare_hours = None
        
    logger.debug('get_foursquare_data primary_category = {}'.format(foursquare_primary_category))

    return {'venue_id': foursquare_venue_id,
            'name': foursquare_name,
            'primary_category': foursquare_primary_category,
            'categories': foursquare_categories,
            'hours': foursquare_hours,
            'resp_search': foursquare_resp_search,
            'resp_hours': foursquare_resp_hours}

def get_business_details(business_profile):    

    # configure logger
    logger = logging.getLogger("mainApp.processData.add")

    google_place, manual_review, match_quality = get_google_places_data(business_profile, use_parent=True)

    # sometimes google returns better data when the parent information is included in the query.
    # sometimes it breaks the google search badly.
    # if it returns nothing or the name match is attrocious try without the parent

    if google_place == {}:
        google_place, manual_review, match_quality = get_google_places_data(business_profile, use_parent=False)        
    
    # If google_place is empty, don't bother doing the foursquare search.
    if google_place.get('name') is not None:
        google_place['tz'] = get_google_timezone(google_place)
        foursquare_place = get_foursquare_data(google_place, business_profile)
    else:
        foursquare_place = {'name': None, 'primary_category': None, 'foursquare_venue_id': None}
        
    out = {'google': google_place,
           'foursquare': foursquare_place,
           'match_quality': match_quality,
           'manual_review': manual_review}
    
    return out
         
