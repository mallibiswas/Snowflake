SELECT $1                  AS INSIGHTS_ADS_CUSTOM_ID,
       $2                  AS CUSTOM_CONV_ID,
       $3                  AS AD_ID,
       $4::INTEGER         AS WALKTHROUGHS,
       $5                  AS INSIGHT_TYPE,
       $6                  AS BREAKDOWN_TYPE,
       $7                  AS BREAKDOWN_VALUE,
       $8::TIMESTAMP       AS LAST_SYNCED,
       $9::INTEGER         AS WALKTHROUGHS1_DAY,
       $10::INTEGER        AS WALKTHROUGHS7_DAY,
       $11::INTEGER        AS WALKTHROUGHS28_DAY,
       current_timestamp() AS ASOF_DATE
FROM {{ most_recent_s3_file_name ( 'ADS' , 'ARCHIVER_ADS_S3_STAGE' , '.*/nwfbcampaignsync/.*/insights_ads_custom_v3.csv' ) }}

