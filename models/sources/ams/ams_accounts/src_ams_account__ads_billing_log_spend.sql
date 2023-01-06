SELECT  $1 ads_billing_log_id
       ,$2::number          AS spend_cents
       ,$3::timestamp       AS date
       ,$4::float           AS margin
       ,$5                  AS paltform_campaign_id
       ,current_timestamp() AS asof_date
FROM {{ most_recent_s3_file_name('AMS_ACCOUNTS', 'AMS_ACCOUNTS_S3_STAGE', '.*/ads_billing_log_spend.csv') }}
