SELECT $1                  AS ID
     , $2                  AS PAYMENT_INFO_ID
     , $3                  AS SALESFORCE_ACCOUNT
     , $4                  AS ACCOUNT_TYPE
     , $5::BOOLEAN         AS ACTIVE
     , $6::TIMESTAMP       AS CREATED
     , $7::TIMESTAMP       AS UPDATED
     , $8::VARCHAR         AS ACCOUNT_OWNER
     , current_timestamp() AS ASOF_DATE
FROM {{ most_recent_s3_file_name ( 'AMS_ACCOUNTS' , 'AMS_ACCOUNTS_S3_STAGE' , '.*/account.csv' ) }}
