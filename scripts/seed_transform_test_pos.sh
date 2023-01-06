#!/bin/bash

# Setting the env for testing
export ENV=TEST

dbt run --models \
    seed_analytics_customer \
    seed_businessprofile_hierarchy \
    seed_customer \
    seed_d_business_geocode \
    seed_purchase \
    seed_finished_sightings \
    seed_merchant \
    seed_portal_accessdevice \
    seed_portal_accessdeviceownership \
    seed_portal_businessprofile \
    seed_portal_userprofile \
    mart_merged_presence_pos__contact_details \
    mart_merged_presence_pos__pos_locations \
    mart_merged_presence_pos__pos_sightings \
    mart_merged_presence_pos__enriched_sightings \
    mart_merged_presence_pos__transactions \
    mart_merged_presence_pos__temporospatial_contact_profiles \
    mart_merged_presence_pos__local_crm_contacts \
    mart_merged_presence_pos__pos_customers \
    mart_merged_presence_pos__crm_contact_profiles \
    mart_merged_presence_pos__wifi_pos_presence \
    --full-refresh

# Test
dbt test --models \
    mart_merged_presence_pos__contact_details \
    mart_merged_presence_pos__pos_locations \
    mart_merged_presence_pos__pos_sightings \
    mart_merged_presence_pos__enriched_sightings \
    mart_merged_presence_pos__transactions \
    mart_merged_presence_pos__temporospatial_contact_profiles \
    mart_merged_presence_pos__local_crm_contacts \
    mart_merged_presence_pos__pos_customers \
    mart_merged_presence_pos__crm_contact_profiles \
    mart_merged_presence_pos__wifi_pos_presence


