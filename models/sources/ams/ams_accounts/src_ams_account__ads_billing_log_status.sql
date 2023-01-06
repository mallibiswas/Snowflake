SELECT  $1                  AS ads_billing_log_id
       ,$2                  AS status
       ,$3::timestamp       AS updated
       ,$4                  AS note
       ,$5                  AS username
       ,current_timestamp() AS asof_date
FROM {{ most_recent_s3_file_name('AMS_ACCOUNTS', 'AMS_ACCOUNTS_S3_STAGE', '.*/ads_billing_log_status.csv') }}
