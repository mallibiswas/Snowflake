SELECT  $1                  AS id
       ,$2                  AS account_id
       ,$3                  AS package_id
       ,$4::number          AS total_units
       ,$5::number          AS assigned_units
       ,$6::timestamp       AS created
       ,$7::timestamp       AS updated
       ,current_timestamp() AS asof_date
FROM {{ most_recent_s3_file_name('AMS_PRODUCTLICENSER', 'AMS_PRODUCTLICENSER_S3_STAGE', '.*license.csv') }}