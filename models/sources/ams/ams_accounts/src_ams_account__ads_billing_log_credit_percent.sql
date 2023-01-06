SELECT  $1                  AS ads_billing_log_id
       ,$2::text          AS credit_id
       ,$3::number          AS value
       ,$4::number          AS cents
       ,current_timestamp() AS asof_date
FROM {{ most_recent_s3_file_name('AMS_ACCOUNTS', 'AMS_ACCOUNTS_S3_STAGE', '.*/ads_billing_log_credit_percent.csv') }}
