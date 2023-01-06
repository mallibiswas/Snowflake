SELECT  $1                  AS id
       ,$2                  AS code
       ,$3                  AS product_id
       ,$4::timestamp       AS created
       ,current_timestamp() AS asof_date
FROM {{ most_recent_s3_file_name('AMS_PRODUCTLICENSER', 'AMS_PRODUCTLICENSER_S3_STAGE', '.*quota_type.csv') }}