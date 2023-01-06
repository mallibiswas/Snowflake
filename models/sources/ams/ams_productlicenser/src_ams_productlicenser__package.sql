SELECT  $1                  AS id
       ,$2                  AS product_id
       ,$3                  AS name
       ,$4                  AS code
       ,$5::timestamp       AS created
       ,$6::timestamp       AS updated
       ,$7::number          AS monthly_list_price
       ,current_timestamp() AS asof_date
FROM {{ most_recent_s3_file_name('AMS_PRODUCTLICENSER', 'AMS_PRODUCTLICENSER_S3_STAGE', '.*package.csv') }}