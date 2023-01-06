SELECT  $1                  AS campaign_id
       ,$2                  AS platform_campaign_id
       ,$3::date            AS date
       ,$4::number          AS ads_spend_cents
       ,$5::number ads_spend_cents_at_bill_time
       ,$6::float           AS margin
       ,$7::timestamp       AS created
       ,$8::timestamp       AS updated
       ,current_timestamp() AS asof_date
FROM {{ most_recent_s3_file_name('AMS_ACCOUNTS', 'AMS_ACCOUNTS_S3_STAGE', '.*/ads_campaign_daily_spend.csv') }}
