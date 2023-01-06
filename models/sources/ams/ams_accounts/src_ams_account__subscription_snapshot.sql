SELECT  $1                  AS id
       ,$2                  AS subscription_id
       ,$3                  AS recurly_subscription_snapshot_id
       ,$4                  AS account_id
       ,$5                  AS recurly_subscription_id
       ,$6                  AS provider_type
       ,$7                  AS product
       ,$8                  AS package
       ,$9::boolean         AS manual_invoice
       ,$10::timestamp      AS created
       ,$11::timestamp      AS updated
       ,current_timestamp() AS asof_date
FROM {{ most_recent_s3_file_name('AMS_ACCOUNTS', 'AMS_ACCOUNTS_S3_STAGE', '.*/subscription_snapshot.csv') }}
