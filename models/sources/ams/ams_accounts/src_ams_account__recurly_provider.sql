SELECT  $1                  AS id
       ,$2                  AS recurly_id
       ,$3                  AS name
       ,$4                  AS email
       ,$5                  AS url
       ,$6::timestamp       AS created
       ,$7::timestamp       AS updated
       ,current_timestamp() AS asof_date
FROM {{ most_recent_s3_file_name('AMS_ACCOUNTS', 'AMS_ACCOUNTS_S3_STAGE', '.*/recurly_provider.csv') }}
