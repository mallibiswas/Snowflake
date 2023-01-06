SELECT  $1                  AS id
       ,$2::timestamp       AS created
       ,$3::timestamp       AS updated
       ,current_timestamp() AS asof_date
FROM {{ most_recent_s3_file_name('AMS_ACCOUNTS', 'AMS_ACCOUNTS_S3_STAGE', '.*/ads_billing_api_requests.csv') }}
