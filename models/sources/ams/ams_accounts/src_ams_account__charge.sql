SELECT  $1                  AS id
       ,$2                  AS account_id
       ,$3                  AS charge_id
       ,$4                  AS name
       ,$5::number          AS quantity
       ,$6::number          AS unit_price_cents
       ,$7::timestamp       AS created
       ,$8::timestamp       AS updated
       ,current_timestamp() AS asof_date
FROM {{ most_recent_s3_file_name('AMS_ACCOUNTS', 'AMS_ACCOUNTS_S3_STAGE', '.*/charge.csv') }}
