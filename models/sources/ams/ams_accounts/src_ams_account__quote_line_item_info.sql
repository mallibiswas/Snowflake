SELECT  $1                  AS id
       ,$2                  AS payment_info_id
       ,$3                  AS salesforce_quote_line_item_id
       ,$4::timestamp       AS created
       ,current_timestamp() AS asof_date
FROM {{ most_recent_s3_file_name('AMS_ACCOUNTS', 'AMS_ACCOUNTS_S3_STAGE', '.*/quote_line_item_info.csv') }}
