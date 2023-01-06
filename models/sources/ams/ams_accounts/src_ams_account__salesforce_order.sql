SELECT $1                  AS ID,
       $2                  AS STATUS,
       $3::TIMESTAMP       AS EFFECTIVE_DATE,
       $4::TIMESTAMP       AS CREATED,
       $5::TIMESTAMP       AS UPDATED,
       $6::TIMESTAMP       AS SIGNED_DATE,
       current_timestamp() AS ASOF_DATE
FROM {{ most_recent_s3_file_name ( 'AMS_ACCOUNTS' , 'AMS_ACCOUNTS_S3_STAGE' , '.*/salesforce_order.csv' ) }}