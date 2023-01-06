SELECT  $1                  AS id
       ,$2                  AS order_id
       ,$3                  AS asset_id
       ,$4                  AS salesforce_order_item_id
       ,$5                  AS salesforce_quote_line_item_uuid
       ,$6::boolean         AS salesforce_asset_synced
       ,$7::boolean         AS dirty
       ,$8::timestamp       AS created
       ,$9::timestamp       AS updated
       ,current_timestamp() AS asof_date
FROM {{ most_recent_s3_file_name('AMS_ACCOUNTS', 'AMS_ACCOUNTS_S3_STAGE', '.*/order_item.csv') }}
