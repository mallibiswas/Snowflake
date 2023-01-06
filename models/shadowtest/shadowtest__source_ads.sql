
{% set db = "ZENPROD"-%}
{% set schema = "ADS"-%}


{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="AD_ACCOUNTS"),
    b_relation=ref('src_ads__accounts'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="ADS"),
    b_relation=ref('src_ads__ads'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="ADSBIZ"),
    b_relation=ref('src_ads__adsbiz'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="AD_INSIGHTS"),
    b_relation=ref('src_ads__ad_insights'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="AD_INSIGHTS_CUSTOM_CONVERSIONS"),
    b_relation=ref('src_ads__ad_insight_custom_conversions'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="AMS_CAMPAIGN"),
    b_relation=ref('src_ads__ams_campaigns'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="AMS_CAMPAIGN_CREDITS"),
    b_relation=ref('src_ads__ams_campaign_credits'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="CAMPAIGNS"),
    b_relation=ref('src_ads__campaigns'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="AD_CREATIVES"),
    b_relation=ref('src_ads__creatives'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="CUSTOM_CONVERSIONS"),
    b_relation=ref('src_ads__custom_conversions'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="INSIGHTS"),
    b_relation=ref('src_ads__insights'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="INSIGHTS_CUSTOM_CONVERSIONS"),
    b_relation=ref('src_ads__insights_custom_conversions'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="LR_ADVERTISERS"),
    b_relation=ref('src_ads__liveramp_advertisers'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="LR_AD_GROUPS"),
    b_relation=ref('src_ads__liveramp_ad_groups'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="LR_CAMPAIGNS"),
    b_relation=ref('src_ads__liveramp_campaigns'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="LR_CREATIVES"),
    b_relation=ref('src_ads__liveramp_creatives'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="LR_IMPRESSIONS_WALKTHROUGHS_BY_DAY"),
    b_relation=ref('src_ads__liveramp_daily_walkthrough_impressions'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="LR_INSIGHTS_CREATIVES"),
    b_relation=ref('src_ads__liveramp_insight_creatives'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="LR_PROCESSED_EXPORTS"),
    b_relation=ref('src_ads__liveramp_processed_exports'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="OFFLINE_EVENT_SETS"),
    b_relation=ref('src_ads__offline_event_sets'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="OFFLINE_EVENT_SET_CONVERSIONS"),
    b_relation=ref('src_ads__offline_event_set_conversions'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="PAGES"),
    b_relation=ref('src_ads__pages'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="PAGES_HOURS"),
    b_relation=ref('src_ads__pages_hours'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="PAGES_INSTAGRAM_ACCOUNTS"),
    b_relation=ref('src_ads__pages_instagram_accounts'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="PAGES_VERTICALS"),
    b_relation=ref('src_ads__pages_verticals'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="SAMPLE_RATES"),
    b_relation=ref('src_ads__sample_rates'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="AD_SETS"),
    b_relation=ref('src_ads__sets'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="AD_SET_INSIGHTS"),
    b_relation=ref('src_ads__set_insights'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="AD_SET_INSIGHTS_CUSTOM_CONVERSIONS"),
    b_relation=ref('src_ads__set_insights_custom_conversions'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="UPLOADED_SIGHTINGS"),
    b_relation=ref('src_ads__uploaded_sightings'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="UPLOADED_SIGHTINGS_CUSTOM_DATA"),
    b_relation=ref('src_ads__uploaded_sightings_custom_data'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="UPLOADED_SIGHTINGS_LIVERAMP"),
    b_relation=ref('src_ads__uploaded_sightings_liveramp'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="ZENREACH_AD_ACCOUNTS"),
    b_relation=ref('src_ads__zenreach_ad_accounts'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="ZENREACH_AD_ACCOUNT_CAMPAIGNS"),
    b_relation=ref('src_ads__zenreach_ad_account_campaigns'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="ZENREACH_AD_ACCOUNT_FB_CONFIG"),
    b_relation=ref('src_ads__zenreach_ad_account_FB_configurations'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="ZENREACH_AD_ACCOUNT_LOCATIONS"),
    b_relation=ref('src_ads__zenreach_ad_account_locations'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="ZENREACH_AD_SET_GOALS"),
    b_relation=ref('src_ads__zenreach_ad_set_goals'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="ZENREACH_CAMPAIGNS"),
    b_relation=ref('src_ads__zenreach_campaigns'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="ZENREACH_CAMPAIGN_RECORDS"),
    b_relation=ref('src_ads__zenreach_campaign_records'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="ZENREACH_CAMPAIGN_RECORDS_LOCATIONS"),
    b_relation=ref('src_ads__zenreach_campaign_record_locations'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="ZENREACH_CAMPAIGN_RECORDS_MARGINS"),
    b_relation=ref('src_ads__zenreach_campaign_record_margins'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="ZENREACH_CAMPAIGN_VISIBILITY"),
    b_relation=ref('src_ads__zenreach_campaign_visibilities'),
    exclude_columns=["ASOF_DATE"]
) }}
