SELECT  $1                  AS id
       ,$2                  AS account_id
       ,$3                  AS salesforce_quote_uuid
       ,$4                  AS salesforce_order_id
       ,$5                  AS signer_name
       ,$6::timestamp       AS signed_date
       ,$7                  AS hardcopy_url
       ,$8::timestamp       AS created
       ,$9::timestamp       AS updated
       ,$10::timestamp      AS cancelled
       ,current_timestamp() AS asof_date
FROM {{ most_recent_s3_file_name('AMS_ACCOUNTS', 'AMS_ACCOUNTS_S3_STAGE', '.*/order.csv') }}
