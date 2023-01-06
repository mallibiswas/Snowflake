SELECT  $1                  AS id
       ,$2                  AS recurly_subscription_id
       ,$3::timestamp       AS start_date
       ,$4::number          AS unit_price_cents
       ,$5::boolean         AS active
       ,$6::number          AS quantity
       ,$7                  AS collection_method
       ,$8                  AS notes
       ,$9                  AS url
       ,$10                 AS plan_code
       ,$11::number         AS billing_frequency_months
       ,$12::timestamp      AS created
       ,$13::timestamp      AS updated
       ,current_timestamp() AS asof_date
FROM {{ most_recent_s3_file_name('AMS_ACCOUNTS', 'AMS_ACCOUNTS_S3_STAGE', '.*/recurly_subscription.csv') }}
