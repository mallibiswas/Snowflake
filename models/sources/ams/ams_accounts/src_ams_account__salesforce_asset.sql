SELECT  $1                  AS id
       ,$2                  AS parent_id
       ,$3::number          AS quantity
       ,$4::number          AS unit_price_cents
       ,$5::timestamp       AS installed_date
       ,$6::timestamp       AS purchase_date
       ,$7::timestamp       AS termination_date
       ,$8::timestamp       AS created
       ,$9::timestamp       AS updated
       ,current_timestamp() AS asof_date
FROM {{ most_recent_s3_file_name('AMS_ACCOUNTS', 'AMS_ACCOUNTS_S3_STAGE', '.*/salesforce_asset.csv') }}
