SELECT  $1                  AS id
       ,$2                  AS account_id
       ,$3                  AS salesforce_quote_line_item
       ,$4                  AS charge_id
       ,$5::number          AS unit_amount_in_cents
       ,$6::number          AS quantitity
       ,$7                  AS description
       ,$8                  AS error
       ,$9::timestamp       AS created
       ,$10::timestamp      AS updated
       ,current_timestamp() AS asof_date
FROM {{ most_recent_s3_file_name('AMS_ACCOUNTS', 'AMS_ACCOUNTS_S3_STAGE', '.*/charge_log.csv')}}
