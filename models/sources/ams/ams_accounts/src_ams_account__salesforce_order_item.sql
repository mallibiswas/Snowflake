SELECT  $1                  AS id
       ,$2                  AS replacement_order_item_id
       ,$3::number          AS quantity
       ,$4::timestamp       AS created
       ,$5::timestamp       AS updated
       ,current_timestamp() AS asof_date
FROM {{ most_recent_s3_file_name('AMS_ACCOUNTS', 'AMS_ACCOUNTS_S3_STAGE', '.*/salesforce_order_item.csv') }}
