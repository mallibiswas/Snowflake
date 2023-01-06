SELECT  $1                  AS id
       ,$2                  AS account_id
       ,$3::date            AS start_date
       ,$4::date            AS end_date
       ,$5::number          AS total_price_cents
       ,$6::number          AS margin_percent
       ,$7::boolean         AS manual_invoice
       ,$8::boolean         AS overspend
       ,$9::boolean         AS dirty
       ,$10::timestamp      AS created
       ,$11::timestamp      AS updated
       ,$12                 AS internal_description
       ,current_timestamp() AS asof_date
FROM {{ most_recent_s3_file_name('AMS_ACCOUNTS', 'AMS_ACCOUNTS_S3_STAGE', '.*/campaign.csv') }}
