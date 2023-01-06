SELECT  $1                  AS id
       ,$2                  AS provider_type
       ,$3                  AS recurly_provider_id
       ,$4::timestamp       AS created
       ,$5::timestamp       AS updated
       ,$6                  AS type
       ,current_timestamp() AS asof_date
FROM {{ most_recent_s3_file_name('AMS_ACCOUNTS', 'AMS_ACCOUNTS_S3_STAGE', '.*/payment_info.csv') }}
