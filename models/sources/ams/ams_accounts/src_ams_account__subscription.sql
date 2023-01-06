SELECT  $1                  AS id
       ,$2                  AS account_id
       ,$3                  AS recurly_subscription_id
       ,$4                  AS provider_type
       ,$5                  AS product
       ,$6                  AS package
       ,$7::boolean         AS manual_invoice
       ,$8::timestamp       AS created
       ,$9::timestamp       AS updated
       ,current_timestamp() AS asof_date
FROM {{ most_recent_s3_file_name('AMS_ACCOUNTS', 'AMS_ACCOUNTS_S3_STAGE', '.*/subscription.csv') }}
