SELECT  $1                  AS id
       ,$2                  AS account_id
       ,$3                  AS salesforce_quote_line_item_uuid
       ,$4                  AS subscription_id
       ,$5::boolean         AS active
       ,$6                  AS product
       ,$7                  AS package
       ,$8::boolean         AS manual_invoice
       ,$9::number          AS unit_price_cents
       ,$10::number         AS quantity
       ,$11::timestamp      AS start_date
       ,$12::number         AS billing_frequency_months
       ,$13                 AS notes
       ,$14                 AS operation
       ,$15                 AS error
       ,$16::timestamp      AS created
       ,$17::timestamp      AS updated
       ,current_timestamp() AS asof_date
FROM {{ most_recent_s3_file_name('AMS_ACCOUNTS', 'AMS_ACCOUNTS_S3_STAGE', '.*/subscription_log.csv') }}
