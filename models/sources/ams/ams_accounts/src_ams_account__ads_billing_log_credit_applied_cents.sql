SELECT  $1                  AS ads_billing_log_id
       ,$2                  AS credit_id
       ,$3::number          AS cents
       ,$4::number          AS total_credit_cents
       ,current_timestamp() AS asof_date
FROM {{ most_recent_s3_file_name('AMS_ACCOUNTS', 'AMS_ACCOUNTS_S3_STAGE', '.*/ads_billing_log_credit_applied_cents.csv') }}
