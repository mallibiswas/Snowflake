SELECT  $1                  AS id
       ,$2                  AS package_id
       ,$3                  AS quotea_id
       ,$4::timestamp       AS created
       ,current_timestamp() AS asof_date
FROM {{ most_recent_s3_file_name('AMS_PRODUCTLICENSER', 'AMS_PRODUCTLICENSER_S3_STAGE', '.*package_quota_link.csv') }}