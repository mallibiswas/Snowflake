SELECT  $1                  AS id 
       ,$2                  AS payment_info_id 
       ,$3                  AS account_id 
       ,$4 :: TIMESTAMP     AS created 
       ,CURRENT_TIMESTAMP() AS asof_date
FROM {{ most_recent_s3_file_name ('AMS_ACCOUNTS','AMS_ACCOUNTS_S3_STAGE','.*/account_payment_infos.csv') }}
