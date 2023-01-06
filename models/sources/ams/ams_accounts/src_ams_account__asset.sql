SELECT  $1                  AS id
       ,$2                  AS account_id
       ,$3                  AS salesforce_asset_id
       ,$4                  AS subscription_id
       ,$5                  AS charge_id
       ,$6                  AS item_type
       ,$7::timestamp       AS created
       ,$8::timestamp       AS updated
       ,$9                  AS payment_info_id
       ,$10                 AS campaign_id
       ,current_timestamp() AS asof_date
FROM {{ most_recent_s3_file_name('AMS_ACCOUNTS', 'AMS_ACCOUNTS_S3_STAGE', '.*/asset.csv') }}
